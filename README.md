ddbstream
------------------------

Simplifies the development of Go lambda functions that consume directly
from dynamodb streams.

### Use Case

`ddbstream` allows once to subscribe to a dynamodb stream by passing a 
function that should be invoked each time a new event (or set of events) is
received.

### Example

```go
s := session.Must(session.NewSession(aws.NewConfig()))
tableName := "blah"
stream := New(dynamodb.New(s), dynamodbstreams.New(s), tableName)

ctx := context.Background()
fn := func(ctx context.Context, data json.RawMessage) error {
    // do work here
    return nil
}
sub, _ := stream.Subscribe(ctx, fn)
defer sub.Close()
```

### Notes

* Not suitable for production use
* Only supports reading from LATEST

