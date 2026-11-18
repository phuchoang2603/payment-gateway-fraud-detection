package application

import (
	"sync"
	"time"
)

const (
	dedupeTTL     = 15 * time.Minute
	dedupeSweepAt = 1024
)

type dedupeEntry struct {
	done   chan struct{}
	result PayResult
	err    error
	at     time.Time
}

type attemptCache struct {
	mu      sync.Mutex
	entries map[string]*dedupeEntry
	now     func() time.Time
}

func newAttemptCache(now func() time.Time) *attemptCache {
	return &attemptCache{entries: make(map[string]*dedupeEntry), now: now}
}

func (c *attemptCache) do(key string, run func() (PayResult, error)) (PayResult, error) {
	c.mu.Lock()
	if entry, found := c.entries[key]; found {
		c.mu.Unlock()
		<-entry.done
		if entry.err == nil {
			return entry.result, nil
		}
		return c.do(key, run)
	}
	if len(c.entries) >= dedupeSweepAt {
		c.sweepLocked()
	}
	entry := &dedupeEntry{done: make(chan struct{}), at: c.now()}
	c.entries[key] = entry
	c.mu.Unlock()

	entry.result, entry.err = run()
	c.mu.Lock()
	if entry.err != nil {
		delete(c.entries, key)
	}
	c.mu.Unlock()
	close(entry.done)
	return entry.result, entry.err
}

func (c *attemptCache) sweepLocked() {
	cutoff := c.now().Add(-dedupeTTL)
	for key, entry := range c.entries {
		select {
		case <-entry.done:
			if entry.at.Before(cutoff) {
				delete(c.entries, key)
			}
		default:
		}
	}
}
