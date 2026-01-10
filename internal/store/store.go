package store

import (
	"sync"
	"time"
)

type Item struct {
	Value     string
	ExpiresAt time.Time // Zero value means no expiration
}

type Store struct {
	mu   sync.RWMutex
	data map[string]Item
}

func New() *Store {
	return &Store{
		data: make(map[string]Item),
	}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = Item{Value: value}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock() // Using Lock because we might modify the map (lazy deletion)
	defer s.mu.Unlock()

	item, ok := s.data[key]
	if !ok {
		return "", false
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return "", false
	}

	return item.Value, true
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if it exists and remove it regardless of expiration
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
	}
	return exists
}

// Expire sets a timeout on key. After the timeout has expired, the key will automatically be deleted.
// Returns true if the timeout was set, false if key does not exist.
func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.data[key]
	if !ok {
		return false
	}

	// Check if already expired
	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return false
	}

	item.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = item
	return true
}

// TTL returns the remaining time to live of a key that has a timeout.
// Returns -2 if the key does not exist.
// Returns -1 if the key exists but has no associated expire.
func (s *Store) TTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.data[key]
	if !ok {
		return -2
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return -2
	}

	if item.ExpiresAt.IsZero() {
		return -1
	}

	return int(time.Until(item.ExpiresAt).Seconds())
}
