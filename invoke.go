package ddbstream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type lambdaHandler interface {
	Invoke(context.Context, []byte) ([]byte, error)
}

type handlerFunc func(ctx context.Context, data []byte) error

func (fn handlerFunc) Handle(ctx context.Context, data []byte) error {
	return fn(ctx, data)
}

type invokeFunc func(ctx context.Context, records []*dynamodbstreams.Record) error

func newInvoker(streamARN string, raw interface{}) invokeFunc {
	var handler handlerFunc
	switch fn := raw.(type) {
	case lambdaHandler:
		handler = func(ctx context.Context, data []byte) error {
			_, err := fn.Invoke(ctx, data)
			return err
		}

	case func(ctx context.Context, data json.RawMessage) error:
		handler = func(ctx context.Context, data []byte) error {
			return fn(ctx, data)
		}

	case func(ctx context.Context, records []*dynamodbstreams.Record) error:
		return fn

	default:
		handler = newHandler(fn)
	}

	return func(ctx context.Context, records []*dynamodbstreams.Record) error {
		event := map[string]interface{}{
			"Records": wrap(streamARN, records),
		}
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("unable to marshal event: %w", err)
		}

		return handler.Handle(ctx, data)
	}
}

func wrap(streamARN string, rr []*dynamodbstreams.Record) []map[string]interface{} {
	var records []map[string]interface{}
	for _, r := range rr {
		records = append(records, map[string]interface{}{
			"awsRegion":      aws.StringValue(r.AwsRegion),
			"dynamodb":       r.Dynamodb,
			"eventID":        aws.StringValue(r.EventID),
			"eventName":      aws.StringValue(r.EventName),
			"eventSource":    aws.StringValue(r.EventSource),
			"eventSourceARN": streamARN,
			"eventVersion":   aws.StringValue(r.EventVersion),
		})
	}
	return records
}

func newHandler(v interface{}) handlerFunc {
	typ := reflect.TypeOf(v)
	val := reflect.ValueOf(v)
	if typ.Kind() != reflect.Func {
		return func(ctx context.Context, data []byte) error {
			return fmt.Errorf("invalid handler type, %T: expected func", v)
		}
	}
	if v := typ.NumIn(); v > 2 {
		return func(ctx context.Context, data []byte) error {
			return fmt.Errorf("handler func accepts at most 2 arguments: got %v", v)
		}
	}
	switch {
	case typ.NumIn() == 1:
		if arg1 := typ.In(0); !isStruct(arg1) {
			return func(ctx context.Context, data []byte) error {
				return fmt.Errorf("argument must be a struct type; got %T", v)
			}
		}

	case typ.NumIn() == 2:
		if arg1, arg2 := typ.In(0), typ.In(1); !isContext(arg1) || !isStruct(arg2) {
			return func(ctx context.Context, data []byte) error {
				return fmt.Errorf("want func(context.Context, {struct}); got func(%v, %v)", arg1, arg2)
			}
		}
	}

	if typ.NumOut() != 1 {
		return func(ctx context.Context, data []byte) error {
			return fmt.Errorf("handler func expects exactly 1 return value, error; got %v", v)
		}
	}

	param := typ.In(typ.NumIn() - 1)
	return func(ctx context.Context, data []byte) error {
		v := reflect.New(param).Interface()
		if err := json.Unmarshal(data, v); err != nil {
			return fmt.Errorf("unable to unmarshal param, %v: %w", param.String(), err)
		}

		paramValue := reflect.ValueOf(v)
		if param.Kind() != reflect.Ptr {
			paramValue = paramValue.Elem()
		}

		var args []reflect.Value
		if typ.NumIn() == 2 {
			args = []reflect.Value{
				reflect.ValueOf(ctx),
				paramValue,
			}
		} else {
			args = []reflect.Value{
				paramValue,
			}
		}

		got := val.Call(args)
		if got[0].IsNil() {
			return nil
		}
		return got[0].Interface().(error)
	}
}

var contextType = reflect.TypeOf(new(context.Context)).Elem()

func isContext(arg reflect.Type) bool {
	return arg.Implements(contextType)
}

func isStruct(arg reflect.Type) bool {
	return arg.Kind() == reflect.Struct || (arg.Kind() == reflect.Ptr && arg.Elem().Kind() == reflect.Struct)
}
