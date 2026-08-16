package main

import "time"

const (
	defaultRetryInitialDelay = 500 * time.Millisecond
	defaultRetryMaxDelay     = 30 * time.Second
)

// RetryOptions controls RetryWithBackoff. MaxAttempts counts calls to op; zero
// means retry forever, which is the startup mode that turns a transient upstream
// outage into self-healing rather than a process crash loop.
type RetryOptions struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	MaxAttempts  int
	OnError      func(err error, attempt int, delay time.Duration)
	Sleep        func(time.Duration)
}

// RetryWithBackoff runs op until it succeeds or MaxAttempts is reached. Delay
// growth is capped without multiplying past time.Duration's range.
func RetryWithBackoff[T any](op func() (T, error), opts RetryOptions) (T, error) {
	initialDelay := opts.InitialDelay
	if initialDelay == 0 {
		initialDelay = defaultRetryInitialDelay
	}
	maxDelay := opts.MaxDelay
	if maxDelay == 0 {
		maxDelay = defaultRetryMaxDelay
	}
	delay := min(initialDelay, maxDelay)
	sleep := opts.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}

	attempt := 0
	for {
		value, err := op()
		if err == nil {
			return value, nil
		}

		attempt++
		if opts.OnError != nil {
			opts.OnError(err, attempt, delay)
		}
		if opts.MaxAttempts > 0 && attempt >= opts.MaxAttempts {
			var zero T
			return zero, err
		}

		sleep(delay)
		if delay >= maxDelay-delay {
			delay = maxDelay
		} else {
			delay *= 2
		}
	}
}
