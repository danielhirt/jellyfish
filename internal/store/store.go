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

// Lock manually locks the store for writing. Used for transactions.
func (s *Store) Lock() {
	s.mu.Lock()
}

// Unlock manually unlocks the store.
func (s *Store) Unlock() {
	s.mu.Unlock()
}

// SetWithoutLock writes to the store without locking. Caller must hold the lock.
func (s *Store) SetWithoutLock(key, value string) {
	s.data[key] = Item{Value: value}
}

// GetWithoutLock reads from the store without locking. Caller must hold the lock.
func (s *Store) GetWithoutLock(key string) (string, bool) {
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

// DelWithoutLock deletes without locking. Caller must hold the lock.
func (s *Store) DelWithoutLock(key string) bool {
	_, exists := s.data[key]
	if exists {
		delete(s.data, key)
	}
	return exists
}

// ExpireWithoutLock sets expiration without locking. Caller must hold the lock.
func (s *Store) ExpireWithoutLock(key string, seconds int) bool {
	item, ok := s.data[key]
	if !ok {
		return false
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return false
	}

	item.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second)
	s.data[key] = item
	return true
}

// TTLWithoutLock returns the TTL without locking. Caller must hold the lock.
func (s *Store) TTLWithoutLock(key string) int {
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

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SetWithoutLock(key, value)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.GetWithoutLock(key)
}

func (s *Store) Del(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DelWithoutLock(key)
}

func (s *Store) Expire(key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ExpireWithoutLock(key, seconds)
}

func (s *Store) TTL(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.TTLWithoutLock(key)
}
