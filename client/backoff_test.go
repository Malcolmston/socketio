package client

import (
	"testing"
	"time"
)

func TestBackoffDefaultsExponential(t *testing.T) {
	b := NewBackoff(BackoffOptions{}) // defaults: 100ms, x2, max 10s
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
	}
	for i, w := range want {
		if got := b.Duration(); got != w {
			t.Errorf("attempt %d: Duration = %v, want %v", i, got, w)
		}
	}
	if b.Attempts() != len(want) {
		t.Errorf("Attempts = %d, want %d", b.Attempts(), len(want))
	}
}

func TestBackoffClampsToMax(t *testing.T) {
	b := NewBackoff(BackoffOptions{Min: time.Second, Max: 3 * time.Second, Factor: 2})
	got := []time.Duration{b.Duration(), b.Duration(), b.Duration(), b.Duration()}
	want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attempt %d = %v, want %v", i, got[i], want[i])
		}
	}
}

func TestBackoffReset(t *testing.T) {
	b := NewBackoff(BackoffOptions{Min: 50 * time.Millisecond, Factor: 2})
	b.Duration()
	b.Duration()
	if b.Attempts() != 2 {
		t.Fatalf("Attempts = %d", b.Attempts())
	}
	b.Reset()
	if b.Attempts() != 0 {
		t.Fatalf("after Reset Attempts = %d", b.Attempts())
	}
	if got := b.Duration(); got != 50*time.Millisecond {
		t.Fatalf("after Reset Duration = %v, want 50ms", got)
	}
}

func TestBackoffJitterDeterministic(t *testing.T) {
	// A fixed Rand makes jitter reproducible. r=0.5, jitter=0.5:
	// base=100ms, deviation = 0.5*0.5*100ms = 25ms.
	// int(0.5*10)=5, 5&1==1 -> add: 125ms.
	b := NewBackoff(BackoffOptions{
		Min:    100 * time.Millisecond,
		Factor: 2,
		Jitter: 0.5,
		Rand:   func() float64 { return 0.5 },
	})
	if got := b.Duration(); got != 125*time.Millisecond {
		t.Fatalf("jittered Duration = %v, want 125ms", got)
	}
}

func TestBackoffNoJitterWithoutRand(t *testing.T) {
	// Jitter set but no Rand -> deterministic, no jitter applied.
	b := NewBackoff(BackoffOptions{Min: 100 * time.Millisecond, Jitter: 0.9})
	if got := b.Duration(); got != 100*time.Millisecond {
		t.Fatalf("Duration = %v, want 100ms (jitter suppressed without Rand)", got)
	}
}

// TestBackoffJitterFloorsDeviation pins backo2's flooring: the deviation is
// floor(rand*jitter*ms), so a fractional deviation is truncated BEFORE it is
// applied. r=0.05 is an even decile, so it is subtracted: 100 - floor(2.5) = 98,
// not 97.5.
func TestBackoffJitterFloorsDeviation(t *testing.T) {
	b := NewBackoff(BackoffOptions{
		Min:    100 * time.Millisecond,
		Jitter: 0.5,
		Rand:   func() float64 { return 0.05 },
	})
	if got := b.Duration(); got != 98*time.Millisecond {
		t.Fatalf("Duration = %v, want 98ms", got)
	}
}

// TestBackoffJitterAppliedBeforeMax pins the order of operations: the ceiling is
// enforced after the jitter, and the deviation is computed from the UNCLAMPED
// delay. Attempt 3 would be 8000ms unclamped; jittering that down by
// floor(0.05*0.5*8000)=200 gives 7800, which is then clamped to 2000. Clamping
// first would instead have produced 1900.
func TestBackoffJitterAppliedBeforeMax(t *testing.T) {
	b := NewBackoff(BackoffOptions{
		Min:    time.Second,
		Max:    2 * time.Second,
		Jitter: 0.5,
		Rand:   func() float64 { return 0.05 },
	})
	var got []time.Duration
	for i := 0; i < 4; i++ {
		got = append(got, b.Duration())
	}
	want := []time.Duration{
		975 * time.Millisecond,  // 1000 - floor(25)
		1950 * time.Millisecond, // 2000 - floor(50)
		2 * time.Second,         // 4000 - 100 = 3900, clamped
		2 * time.Second,         // 8000 - 200 = 7800, clamped
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("attempt %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// TestBackoffJitterOutOfRangeDisabled pins that a jitter outside (0,1] disables
// randomisation rather than being clamped, as backo2 does.
func TestBackoffJitterOutOfRangeDisabled(t *testing.T) {
	for _, jitter := range []float64{-0.5, 1.5, 100} {
		b := NewBackoff(BackoffOptions{
			Min:    100 * time.Millisecond,
			Jitter: jitter,
			Rand:   func() float64 { return 0.9 },
		})
		if got := b.Duration(); got != 100*time.Millisecond {
			t.Errorf("jitter %v: Duration = %v, want 100ms (randomisation disabled)", jitter, got)
		}
	}
}

// TestBackoffWholeMilliseconds pins backo2's trailing `| 0`: a delay is always a
// whole number of milliseconds, even when the factor is fractional.
func TestBackoffWholeMilliseconds(t *testing.T) {
	b := NewBackoff(BackoffOptions{Min: 1000 * time.Millisecond, Factor: 1.5, Max: time.Minute})
	want := []time.Duration{1000, 1500, 2250, 3375, 5062}
	for i, w := range want {
		got := b.Duration()
		if got != w*time.Millisecond {
			t.Errorf("attempt %d = %v, want %v", i, got, w*time.Millisecond)
		}
		if got%time.Millisecond != 0 {
			t.Errorf("attempt %d = %v is not a whole number of milliseconds", i, got)
		}
	}
}

func BenchmarkBackoffDuration(b *testing.B) {
	bo := NewBackoff(BackoffOptions{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bo.Duration()
		if i%10 == 0 {
			bo.Reset()
		}
	}
}
