package store

import (
	"sync"
	"time"
)

const (
	TypeString = 0
	TypeVector = 1
	TypeHash   = 2
)

type Item struct {
	Type      uint8
	StrVal    string
	VecVal    []float32
	HashVal   map[string]string
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
	s.data[key] = Item{
		Type:   TypeString,
		StrVal: value,
	}
}

// SetVectorWithoutLock writes a vector to the store.
func (s *Store) SetVectorWithoutLock(key string, vec []float32) {
	s.data[key] = Item{
		Type:   TypeVector,
		VecVal: vec,
	}
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

	if item.Type != TypeString {
		// Redis protocol usually returns error for wrong type, but here we return nil/false or handle it upper layer.
		// For simplicity, we return empty string and true, but let's stick to "not found" behavior for wrong type
		// or let the handler check type?
		// Better: Return the raw item or check type.
		// To match existing API signature `(string, bool)`, we return false if it's not a string.
		return "", false
	}

	return item.StrVal, true
}

// GetVectorWithoutLock reads a vector.
func (s *Store) GetVectorWithoutLock(key string) ([]float32, bool) {
	item, ok := s.data[key]
	if !ok {
		return nil, false
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return nil, false
	}

	if item.Type != TypeVector {
		return nil, false
	}

	return item.VecVal, true
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

// --- Public Thread-Safe API ---

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SetWithoutLock(key, value)
}

func (s *Store) SetVector(key string, vec []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SetVectorWithoutLock(key, vec)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.GetWithoutLock(key)
}

func (s *Store) GetVector(key string) ([]float32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.GetVectorWithoutLock(key)
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

// HSetWithoutLock sets fields on a hash. Returns the number of new fields added, or -1 on WRONGTYPE.
func (s *Store) HSetWithoutLock(key string, fields map[string]string) int {
	item, ok := s.data[key]
	if ok {
		if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
			delete(s.data, key)
			ok = false
		}
	}

	if ok && item.Type != TypeHash {
		return -1
	}

	if !ok {
		item = Item{Type: TypeHash, HashVal: make(map[string]string)}
	}

	added := 0
	for f, v := range fields {
		if _, exists := item.HashVal[f]; !exists {
			added++
		}
		item.HashVal[f] = v
	}

	s.data[key] = item
	return added
}

// HGetWithoutLock returns the value of a hash field. Returns (value, found, typeOk).
func (s *Store) HGetWithoutLock(key, field string) (string, bool, bool) {
	item, ok := s.data[key]
	if !ok {
		return "", false, true
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return "", false, true
	}

	if item.Type != TypeHash {
		return "", false, false
	}

	val, exists := item.HashVal[field]
	return val, exists, true
}

// HDelWithoutLock deletes fields from a hash. Returns the number of fields removed, or -1 on WRONGTYPE.
func (s *Store) HDelWithoutLock(key string, fields []string) int {
	item, ok := s.data[key]
	if !ok {
		return 0
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return 0
	}

	if item.Type != TypeHash {
		return -1
	}

	removed := 0
	for _, f := range fields {
		if _, exists := item.HashVal[f]; exists {
			delete(item.HashVal, f)
			removed++
		}
	}

	s.data[key] = item
	return removed
}

// HGetAllWithoutLock returns all fields and values of a hash. Returns (map, typeOk).
// nil map + true = key not found. non-nil map + true = success. nil + false = WRONGTYPE.
func (s *Store) HGetAllWithoutLock(key string) (map[string]string, bool) {
	item, ok := s.data[key]
	if !ok {
		return nil, true
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return nil, true
	}

	if item.Type != TypeHash {
		return nil, false
	}

	result := make(map[string]string, len(item.HashVal))
	for k, v := range item.HashVal {
		result[k] = v
	}
	return result, true
}

// HExistsWithoutLock checks if a field exists in a hash. Returns (exists, typeOk).
func (s *Store) HExistsWithoutLock(key, field string) (bool, bool) {
	item, ok := s.data[key]
	if !ok {
		return false, true
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return false, true
	}

	if item.Type != TypeHash {
		return false, false
	}

	_, exists := item.HashVal[field]
	return exists, true
}

// HLenWithoutLock returns the number of fields in a hash, or -1 on WRONGTYPE.
func (s *Store) HLenWithoutLock(key string) int {
	item, ok := s.data[key]
	if !ok {
		return 0
	}

	if !item.ExpiresAt.IsZero() && time.Now().After(item.ExpiresAt) {
		delete(s.data, key)
		return 0
	}

	if item.Type != TypeHash {
		return -1
	}

	return len(item.HashVal)
}

// --- Public Hash API ---

func (s *Store) HSet(key string, fields map[string]string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HSetWithoutLock(key, fields)
}

func (s *Store) HGet(key, field string) (string, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HGetWithoutLock(key, field)
}

func (s *Store) HDel(key string, fields []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HDelWithoutLock(key, fields)
}

func (s *Store) HGetAll(key string) (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HGetAllWithoutLock(key)
}

func (s *Store) HExists(key, field string) (bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HExistsWithoutLock(key, field)
}

func (s *Store) HLen(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.HLenWithoutLock(key)
}

// GetAllVectors returns a map of all valid vectors (for search).
func (s *Store) GetAllVectors() map[string][]float32 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create a copy to avoid race conditions during iteration by caller if they were to use the map directly
	// Actually, we should return a snapshot.
	vectors := make(map[string][]float32)
	now := time.Now()

	for k, v := range s.data {
		if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt) {
			continue // Don't return expired items (cleanup happens on Get/Del usually)
		}
		if v.Type == TypeVector {
			vectors[k] = v.VecVal
		}
	}
	return vectors
}
