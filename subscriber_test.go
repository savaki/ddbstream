package ddbstream

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func TestLive(t *testing.T) {
	var (
		accessKeyID     = os.Getenv("AWS_ACCESS_KEY_ID")
		secretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		sessionToken    = os.Getenv("AWS_SESSION_TOKEN")
		tableName       = os.Getenv("TABLE_NAME")
	)

	if accessKeyID == "" || secretAccessKey == "" || tableName == "" {
		t.SkipNow()
	}

	s := session.Must(session.NewSession(aws.NewConfig().
		WithCredentials(credentials.NewStaticCredentials(accessKeyID, secretAccessKey, sessionToken)).
		WithRegion("us-west-2")))
	stream := New(dynamodb.New(s), dynamodbstreams.New(s), tableName,
		WithIteratorType(dynamodbstreams.ShardIteratorTypeTrimHorizon),
		WithDebug(debug),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var counter int64
	fn := func(ctx context.Context, records []*dynamodbstreams.Record) error {
		atomic.AddInt64(&counter, int64(len(records)))
		return nil
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				v := atomic.LoadInt64(&counter)
				fmt.Println("replayed", v)
			}
		}
	}()

	sub, err := stream.Subscribe(ctx, fn)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	defer sub.Close()

	time.Sleep(time.Hour)
}

func debug(format string, args ...interface{}) {
	format = strings.TrimRight(format, "\n") + "\n"
	fmt.Printf(format, args...)
}
