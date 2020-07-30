package ddbstream

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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
	input := dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamARN),
	}
	output, err := s.stream.streamAPI.DescribeStreamWithContext(ctx, &input)
	if err != nil {
		return nil, fmt.Errorf("unable to describe dynamodb stream, %v: %w", streamARN, err)
	}

	s.options.debug("found %v shards", len(output.StreamDescription.Shards))

	var open []*dynamodbstreams.Shard
	for _, shard := range output.StreamDescription.Shards {
		if shard.SequenceNumberRange.EndingSequenceNumber == nil {
			open = append(open, shard)
		}
	}
	return open, nil
}

func (s *Subscriber) mainLoop(ctx context.Context, streamARN string) (err error) {
	var ch = make(chan *dynamodbstreams.Record)

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		m := map[string]struct{}{}
		for {
			shards, err := s.getShards(ctx, streamARN)
			if err != nil {
				return err
			}

			for _, item := range shards {
				shard := item
				shardID := aws.StringValue(shard.ShardId)
				if _, ok := m[shardID]; ok {
					continue
				}

				m[shardID] = struct{}{}
				group.Go(func() error {
					return s.iterateShard(ctx, streamARN, shard, ch)
				})
			}

		loop:
			for k := range m {
				for _, item := range shards {
					if aws.StringValue(item.ShardId) == k {
						continue loop
					}
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				continue
			}
		}
	})
	group.Go(func() error {
		ticker := time.NewTicker(5 * time.Second)
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

func (s *Subscriber) iterateShard(ctx context.Context, streamARN string, shard *dynamodbstreams.Shard, ch chan *dynamodbstreams.Record) error {
	iterInput := dynamodbstreams.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeLatest),
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
		}
		output, err := s.stream.streamAPI.GetRecordsWithContext(ctx, &input)
		if err != nil {
			return fmt.Errorf("failed to get records from shard, %v: %w", aws.StringValue(iterOutput.ShardIterator), err)
		}
		s.options.debug("%v: read %v records", aws.StringValue(shard.ShardId), len(output.Records))

		for _, record := range output.Records {
			select {
			case <-ctx.Done():
				return nil
			case ch <- record:
				// ok
			}
		}

		iterator = output.NextShardIterator
		if iterator == nil {
			break
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

	return nil
}

func (s *Subscriber) Close() error {
	s.cancel()
	<-s.done
	return s.err
}
