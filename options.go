package ddbstream

import "time"

const (
	defaultBatchSize = 100
	defaultInterval  = 5 * time.Second
)

type Options struct {
	batchSize    int
	debug        func(format string, args ...interface{})
	pollInterval time.Duration
}

type Option func(*Options)

func WithBatchSize(n int) Option {
	return func(o *Options) {
		o.batchSize = n
	}
}

func WithDebug(fn func(format string, args ...interface{})) Option {
	return func(o *Options) {
		o.debug = fn
	}
}

func WithPollInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.pollInterval = interval
	}
}

func buildOptions(opts ...Option) Options {
	options := Options{}
	for _, opt := range opts {
		opt(&options)
	}

	if options.batchSize <= 0 || options.batchSize > 1000 {
		options.batchSize = defaultBatchSize
	}
	if options.debug == nil {
		options.debug = func(format string, args ...interface{}) {}
	}
	if options.pollInterval <= 0 {
		options.pollInterval = defaultInterval
	}

	return options
}
