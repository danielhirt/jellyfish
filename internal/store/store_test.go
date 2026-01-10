package store

import (
	"testing"
	"time"
)

func TestStore_SetGet(t *testing.T) {
	s := New()
	key := "foo"
	val := "bar"

	s.Set(key, val)

	got, found := s.Get(key)
	if !found {
		t.Errorf("Get(%q) should be found", key)
	}
	if got != val {
		t.Errorf("Get(%q) = %q, want %q", key, got, val)
	}
}

func TestStore_Del(t *testing.T) {
	s := New()
	key := "foo"
	val := "bar"

	s.Set(key, val)

	// Test successful delete
	deleted := s.Del(key)
	if !deleted {
		t.Errorf("Del(%q) should return true", key)
	}

	// Verify key is gone
	_, found := s.Get(key)
	if found {
		t.Errorf("Get(%q) should not be found after Del", key)
	}

	// Test delete non-existent
	deleted = s.Del(key)
	if deleted {
		t.Errorf("Del(%q) of non-existent key should return false", key)
	}
}

func TestStore_Expire(t *testing.T) {
	s := New()
	key := "expire_key"
	val := "expire_val"

	s.Set(key, val)

	// Test Expire on existing key
	ok := s.Expire(key, 2) // 2 seconds
	if !ok {
		t.Errorf("Expire(%q) should return true", key)
	}

	// Test TTL
	ttl := s.TTL(key)
	if ttl < 0 {
		t.Errorf("TTL(%q) should be >= 0, got %d", key, ttl)
	}

	// Wait for expiration
	time.Sleep(2100 * time.Millisecond)

	// Test Lazy Expiration on Get
	_, found := s.Get(key)
	if found {
		t.Errorf("Get(%q) should not be found after expiration", key)
	}

	// Test TTL on missing key
	ttl = s.TTL(key)
	if ttl != -2 {
		t.Errorf("TTL(%q) should be -2 (missing), got %d", key, ttl)
	}
}

func TestStore_TTL(t *testing.T) {
	s := New()
	key := "ttl_key"
	s.Set(key, "val")

	// No expiry
	if ttl := s.TTL(key); ttl != -1 {
		t.Errorf("Expected TTL -1 for persistent key, got %d", ttl)
	}

	// Non-existent
	if ttl := s.TTL("missing"); ttl != -2 {
		t.Errorf("Expected TTL -2 for missing key, got %d", ttl)
	}
}
