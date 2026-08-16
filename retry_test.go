package main

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestRetryWithBackoffCappedGrowthAndInjectedSleep(t *testing.T) {
	transient := errors.New("upstream unavailable")
	calls := 0
	var sleeps []time.Duration
	var observedAttempts []int
	var observedDelays []time.Duration

	got, err := RetryWithBackoff(func() (string, error) {
		calls++
		if calls <= 4 {
			return "", transient
		}
		return "ready", nil
	}, RetryOptions{
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     12 * time.Millisecond,
		Sleep: func(delay time.Duration) {
			sleeps = append(sleeps, delay)
		},
		OnError: func(err error, attempt int, delay time.Duration) {
			if !errors.Is(err, transient) {
				t.Errorf("OnError received %v", err)
			}
			observedAttempts = append(observedAttempts, attempt)
			observedDelays = append(observedDelays, delay)
		},
	})
	if err != nil || got != "ready" {
		t.Fatalf("RetryWithBackoff = %q, %v", got, err)
	}
	if calls != 5 {
		t.Fatalf("op calls = %d, want 5", calls)
	}
	wantDelays := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 12 * time.Millisecond, 12 * time.Millisecond}
	if !reflect.DeepEqual(sleeps, wantDelays) {
		t.Fatalf("sleeps = %v, want %v", sleeps, wantDelays)
	}
	if !reflect.DeepEqual(observedDelays, wantDelays) {
		t.Fatalf("OnError delays = %v, want %v", observedDelays, wantDelays)
	}
	if want := []int{1, 2, 3, 4}; !reflect.DeepEqual(observedAttempts, want) {
		t.Fatalf("OnError attempts = %v, want %v", observedAttempts, want)
	}
}

func TestRetryWithBackoffDefaultsToRetryForever(t *testing.T) {
	calls := 0
	var sleeps int
	got, err := RetryWithBackoff(func() (int, error) {
		calls++
		if calls < 7 {
			return 0, errors.New("not yet")
		}
		return 42, nil
	}, RetryOptions{
		InitialDelay: time.Nanosecond,
		MaxDelay:     4 * time.Nanosecond,
		Sleep:        func(time.Duration) { sleeps++ },
	})
	if err != nil || got != 42 {
		t.Fatalf("RetryWithBackoff = %d, %v", got, err)
	}
	if calls != 7 || sleeps != 6 {
		t.Fatalf("calls=%d sleeps=%d, want 7/6", calls, sleeps)
	}
}

func TestRetryWithBackoffAttemptCapSkipsFinalSleep(t *testing.T) {
	terminal := errors.New("still down")
	calls := 0
	var sleeps []time.Duration
	got, err := RetryWithBackoff(func() (int, error) {
		calls++
		return 99, terminal
	}, RetryOptions{
		InitialDelay: 8 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
		MaxAttempts:  3,
		Sleep:        func(delay time.Duration) { sleeps = append(sleeps, delay) },
	})
	if got != 0 || !errors.Is(err, terminal) {
		t.Fatalf("RetryWithBackoff = %d, %v", got, err)
	}
	if calls != 3 {
		t.Fatalf("op calls = %d, want 3", calls)
	}
	want := []time.Duration{8 * time.Millisecond, 10 * time.Millisecond}
	if !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("sleeps = %v, want %v", sleeps, want)
	}
}

func TestRetryWithBackoffCapsInitialDelay(t *testing.T) {
	calls := 0
	var sleeps []time.Duration
	_, err := RetryWithBackoff(func() (struct{}, error) {
		calls++
		if calls == 1 {
			return struct{}{}, errors.New("once")
		}
		return struct{}{}, nil
	}, RetryOptions{
		InitialDelay: 50 * time.Millisecond,
		MaxDelay:     7 * time.Millisecond,
		Sleep:        func(delay time.Duration) { sleeps = append(sleeps, delay) },
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{7 * time.Millisecond}
	if !reflect.DeepEqual(sleeps, want) {
		t.Fatalf("sleeps = %v, want cap %v", sleeps, want)
	}
}
