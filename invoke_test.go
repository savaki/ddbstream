package ddbstream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func Test_newHandler(t *testing.T) {
	type Event struct {
		Records []json.RawMessage
	}

	t.Run("via reflection", func(t *testing.T) {
		var event Event
		fn := func(e Event) error {
			event = e
			return nil
		}

		handler := newHandler(fn)
		err := handler.Handle(context.Background(), []byte(`{"Records":[{}]}`))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := len(event.Records), 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("via reflection - with context", func(t *testing.T) {
		var event Event
		fn := func(ctx context.Context, e Event) error {
			event = e
			return nil
		}

		handler := newHandler(fn)
		err := handler.Handle(context.Background(), []byte(`{"Records":[{}]}`))
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := len(event.Records), 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("via reflection - returns error", func(t *testing.T) {
		want := io.EOF
		fn := func(ctx context.Context, _ Event) error {
			return want
		}

		handler := newHandler(fn)
		got := handler.Handle(context.Background(), []byte(`{"Records":[{}]}`))
		if !errors.Is(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("failure modes", func(t *testing.T) {
		var testCases = map[string]struct {
			Input interface{}
			Want  string
		}{
			"invalid handler type": {
				Input: "invalid handler type",
				Want:  "invalid handler type",
			},
			"> 2 args": {
				Input: func(context.Context, string, string) error { return nil },
				Want:  "handler func accepts at most 2",
			},
			"last arg not struct - 1 arg": {
				Input: func(string) error { return nil },
				Want:  "argument must be a struct type",
			},
			"last arg not struct - 2 args": {
				Input: func(context.Context, string) error { return nil },
				Want:  "want func(context.Context, {struct})",
			},
			"multiple return types": {
				Input: func(context.Context, struct{}) (int, error) { return 0, nil },
				Want:  "handler func expects exactly 1 return value",
			},
		}
		ctx := context.Background()
		for label, tc := range testCases {
			t.Run(label, func(t *testing.T) {
				handler := newHandler(tc.Input)
				err := handler.Handle(ctx, []byte(`{}`))
				if err == nil {
					t.Fatalf("got nil; want not nil")
				}
				if got := err.Error(); !strings.Contains(got, tc.Want) {
					t.Fatalf("got %v; want contains %v", got, tc.Want)
				}
			})
		}
	})
}

type TestHandler struct {
	invocations int
}

func (t *TestHandler) Invoke(context.Context, []byte) ([]byte, error) {
	t.invocations++
	return nil, nil
}

func Test_newInvoker(t *testing.T) {
	t.Run("lambda.Handler", func(t *testing.T) {
		handler := &TestHandler{}
		fn := newInvoker("blah", handler)
		err := fn(context.Background(), nil)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := handler.invocations, 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("via reflection", func(t *testing.T) {
		invocations := 0
		raw := func(ctx context.Context, event struct{ Records []json.RawMessage }) error {
			invocations++
			return nil
		}
		fn := newInvoker("blah", raw)
		err := fn(context.Background(), nil)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := invocations, 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
