package ddbstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"golang.org/x/sync/errgroup"
)

type Stream struct {
	api       dynamodbiface.DynamoDBAPI
	streamAPI dynamodbstreamsiface.DynamoDBStreamsAPI
	options   Options
	tableName string
}

type idSet struct {
	mutex sync.Mutex
	data  map[string]time.Time
}

func (m *idSet) AddAll(ss ...*dynamodbstreams.Shard) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for _, s := range ss {
		if m.data == nil {
			m.data = map[string]time.Time{}
		}

		id := aws.StringValue(s.ShardId)
		m.data[id] = time.Now()
	}
}

func (m *idSet) Contains(id string) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, ok := m.data[id]
	return ok
}

func (m *idSet) Expire(age time.Duration) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	n := 0
	cutoff := time.Now().Add(-age)
	for k, v := range m.data {
		if v.Before(cutoff) {
			delete(m.data, k)
			n++
		}
	}

	return n
}

func (m *idSet) Remove(id string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	delete(m.data, id)
}

func (m *idSet) Size() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return len(m.data)
}

func (m *idSet) Slice() (ss []string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	for v := range m.data {
		ss = append(ss, v)
	}
	return ss
}

type Subscriber struct {
	stream  *Stream
	cancel  context.CancelFunc
	done    chan struct{}
	err     error
	invoker invokeFunc
	options Options
}

func New(api dynamodbiface.DynamoDBAPI, streamAPI dynamodbstreamsiface.DynamoDBStreamsAPI, tableName string, opts ...Option) *Stream {
	options := buildOptions(opts...)
	return &Stream{
		api:       api,
		options:   options,
		streamAPI: streamAPI,
		tableName: tableName,
	}
}

func (s *Stream) Subscribe(ctx context.Context, v interface{}) (*Subscriber, error) {
	ctx, cancel := context.WithCancel(ctx)

	describeInput := dynamodb.DescribeTableInput{
		TableName: aws.String(s.tableName),
	}
	describeOutput, err := s.api.DescribeTableWithContext(ctx, &describeInput)
	if err != nil {
		return nil, fmt.Errorf("unable to subscribe to table, %v: %w", s.tableName, err)
	}
	if table := describeOutput.Table; table == nil || table.StreamSpecification == nil {
		return nil, fmt.Errorf("unable to subscribe to table, %v: no stream specification found", s.tableName)
	}
	streamARN := aws.StringValue(describeOutput.Table.LatestStreamArn)

	subscriber := &Subscriber{
		stream:  s,
		cancel:  cancel,
		done:    make(chan struct{}),
		invoker: newInvoker(streamARN, v),
		options: s.options,
	}

	go func() {
		defer close(subscriber.done)
		defer cancel()
		subscriber.err = subscriber.mainLoop(ctx, streamARN)
	}()

	return subscriber, nil
}

func (s *Subscriber) getShards(ctx context.Context, streamARN string) ([]*dynamodbstreams.Shard, error) {
	var (
		shards       []*dynamodbstreams.Shard
		startShardId *string
	)

	for {
		input := dynamodbstreams.DescribeStreamInput{
			ExclusiveStartShardId: startShardId,
			Limit:                 aws.Int64(100),
			StreamArn:             aws.String(streamARN),
		}
		output, err := s.stream.streamAPI.DescribeStreamWithContext(ctx, &input)
		if err != nil {
			return nil, fmt.Errorf("unable to describe dynamodb stream, %v: %w", streamARN, err)
		}

		s.options.debug("found %v shards", len(output.StreamDescription.Shards))
		shards = append(shards, output.StreamDescription.Shards...)

		startShardId = output.StreamDescription.LastEvaluatedShardId
		if startShardId == nil {
			break
		}
	}

	return shards, nil
}

func (s *Subscriber) isLatest() bool {
	return s.options.shardIteratorType == dynamodbstreams.ShardIteratorTypeLatest
}

func (s *Subscriber) mainLoop(ctx context.Context, streamARN string) (err error) {
	var (
		ch        = make(chan *dynamodbstreams.Record)
		next      = make(chan struct{}, 1)
		wip       = &idSet{}
		completed = &idSet{}
	)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		expire := time.NewTicker(12 * time.Hour)
		defer expire.Stop()

		for {
			s.options.debug("scanning for shards")

			shards, err := s.getShards(ctx, streamARN)
			if err != nil {
				return err
			}

			excludeCompleted := func(shard *dynamodbstreams.Shard) bool {
				return !containsString(completed.Slice(), aws.StringValue(shard.ShardId))
			}
			shards = filter(shards, excludeCompleted) // ignore any shards we've completed

			d := dag{}
			d.addShards(shards...)

			for _, item := range d.Roots() {
				shard := item
				shardID := aws.StringValue(shard.ShardId)
				if wip.Contains(shardID) {
					continue
				}

				wip.AddAll(shard)
				go func() {
					defer wip.Remove(shardID)
					defer completed.AddAll(shard)

					select {
					case next <- struct{}{}:
					default:
					}

					s.options.debug("shard started, %v\n", shardID)
					defer s.options.debug("shard completed, %v\n", shardID)

					err := s.iterateShardWithRetry(ctx, streamARN, shard, ch)
					if err != nil {
						s.options.debug("iterate shard failed, %v", err)
					}
				}()
			}

			select {
			case <-ctx.Done():
				return nil
			case <-next:
				continue

			case <-expire.C:
				n := completed.Expire(36 * time.Hour)
				s.options.debug("expired %v elements", n)

			case <-ticker.C:
				continue
			}
		}
	})
	group.Go(func() error {
		ticker := time.NewTicker(s.options.pollInterval)
		defer ticker.Stop()

		var records []*dynamodbstreams.Record
		invoke := func() error {
			if len(records) == 0 {
				return nil
			}

			for {
				if err := s.invoker(ctx, records); err != nil {
					delay := 3 * time.Second
					fmt.Printf("callback failed - %v; will retry in %v\n", err, delay)

					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(delay):
						continue
					}
				}
				break
			}

			records = nil
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return nil

			case v := <-ch:
				records = append(records, v)
				if len(records) >= s.stream.options.batchSize {
					if err := invoke(); err != nil {
						return err
					}
				}

			case <-ticker.C:
				if err := invoke(); err != nil {
					return err
				}
			}
		}
	})
	return group.Wait()
}

func filter(shards []*dynamodbstreams.Shard, conditions ...func(shard *dynamodbstreams.Shard) bool) (ss []*dynamodbstreams.Shard) {
loop:
	for _, shard := range shards {
		for _, fn := range conditions {
			if !fn(shard) {
				continue loop
			}
		}
		ss = append(ss, shard)
	}
	return ss
}

func (s *Subscriber) iterateShardWithRetry(ctx context.Context, streamARN string, shard *dynamodbstreams.Shard, ch chan *dynamodbstreams.Record) error {
	var sequenceNumber *string
	var multiplier time.Duration = 1
	for {
		if multiplier > 16 {
			multiplier = 16
		}

		if err := s.iterateShard(ctx, streamARN, shard, ch, sequenceNumber); err != nil {
			var ae awserr.Error
			if ok := errors.As(err, &ae); ok && ae.Code() == dynamodbstreams.ErrCodeLimitExceededException {
				multiplier *= 2
				delay := time.Second * multiplier
				s.options.debug("rate limit exceeded, pausing %v", delay)

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
					continue
				}
			}
			return err
		}
		return nil
	}
}

func (s *Subscriber) iterateShard(ctx context.Context, streamARN string, shard *dynamodbstreams.Shard, ch chan *dynamodbstreams.Record, sequenceNumber *string) error {
	iteratorType := s.options.shardIteratorType
	if sequenceNumber != nil {
		iteratorType = dynamodbstreams.ShardIteratorTypeAfterSequenceNumber
	}

	for {
		iterInput := dynamodbstreams.GetShardIteratorInput{
			SequenceNumber:    sequenceNumber,
			ShardId:           shard.ShardId,
			ShardIteratorType: aws.String(iteratorType),
			StreamArn:         aws.String(streamARN),
		}
		iterOutput, err := s.stream.streamAPI.GetShardIteratorWithContext(ctx, &iterInput)
		if err != nil {
			return fmt.Errorf("failed to retrieve iterator for shard, %v: %w", aws.StringValue(shard.ShardId), err)
		}

		iterator := iterOutput.ShardIterator
		for {
			input := dynamodbstreams.GetRecordsInput{
				ShardIterator: iterator,
				Limit:         aws.Int64(1000),
			}
			output, err := s.stream.streamAPI.GetRecordsWithContext(ctx, &input)
			if err != nil {
				return fmt.Errorf("failed to get records from shard, %v: %w", aws.StringValue(iterOutput.ShardIterator), err)
			}

			for _, record := range output.Records {
				select {
				case <-ctx.Done():
					return nil
				case ch <- record:
					// ok
				}
				sequenceNumber = record.Dynamodb.SequenceNumber
			}
			//s.options.debug("%v: read %3d records [%v]", aws.StringValue(shard.ShardId), len(output.Records), aws.StringValue(sequenceNumber))

			iterator = output.NextShardIterator
			if iterator == nil {
				return nil
			}

			if len(output.Records) == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(3 * time.Second):
					// ok
				}
			}
		}
	}
}

func (s *Subscriber) Close() error {
	s.cancel()
	<-s.done
	return s.err
}
