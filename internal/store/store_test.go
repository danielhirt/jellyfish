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

func TestStore_HSetHGet(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(s *Store)
		hsetKey    string
		hsetField  map[string]string
		wantAdded  int
		getField   string
		wantVal    string
		wantFound  bool
		wantTypeOk bool
	}{
		{
			name:       "set and get single field",
			hsetKey:    "myhash",
			hsetField:  map[string]string{"f1": "v1"},
			wantAdded:  1,
			getField:   "f1",
			wantVal:    "v1",
			wantFound:  true,
			wantTypeOk: true,
		},
		{
			name:       "set multiple fields",
			hsetKey:    "myhash",
			hsetField:  map[string]string{"f1": "v1", "f2": "v2"},
			wantAdded:  2,
			getField:   "f2",
			wantVal:    "v2",
			wantFound:  true,
			wantTypeOk: true,
		},
		{
			name: "overwrite existing field",
			setup: func(s *Store) {
				s.HSet("myhash", map[string]string{"f1": "old"})
			},
			hsetKey:    "myhash",
			hsetField:  map[string]string{"f1": "new"},
			wantAdded:  0,
			getField:   "f1",
			wantVal:    "new",
			wantFound:  true,
			wantTypeOk: true,
		},
		{
			name:       "get missing field",
			hsetKey:    "myhash",
			hsetField:  map[string]string{"f1": "v1"},
			wantAdded:  1,
			getField:   "f_missing",
			wantVal:    "",
			wantFound:  false,
			wantTypeOk: true,
		},
		{
			name:       "get from missing key",
			hsetKey:    "myhash",
			hsetField:  map[string]string{"f1": "v1"},
			wantAdded:  1,
			getField:   "f1",
			wantVal:    "",
			wantFound:  false,
			wantTypeOk: true,
		},
		{
			name: "WRONGTYPE on string key for HSET",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			hsetKey:   "strkey",
			hsetField: map[string]string{"f1": "v1"},
			wantAdded: -1,
		},
		{
			name: "WRONGTYPE on string key for HGET",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			hsetKey:    "unused",
			hsetField:  map[string]string{},
			wantAdded:  0,
			getField:   "f1",
			wantVal:    "",
			wantFound:  false,
			wantTypeOk: false,
		},
		{
			name: "WRONGTYPE on vector key for HSET",
			setup: func(s *Store) {
				s.SetVector("veckey", []float32{1.0, 2.0})
			},
			hsetKey:   "veckey",
			hsetField: map[string]string{"f1": "v1"},
			wantAdded: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.setup != nil {
				tt.setup(s)
			}

			added := s.HSet(tt.hsetKey, tt.hsetField)
			if added != tt.wantAdded {
				t.Errorf("HSet() added = %d, want %d", added, tt.wantAdded)
			}

			// Skip HGet check for WRONGTYPE HSET cases
			if tt.wantAdded == -1 {
				return
			}

			// For the "get from missing key" test, query a different key
			getKey := tt.hsetKey
			if tt.name == "get from missing key" {
				getKey = "otherkey"
			}
			// For the "WRONGTYPE on string key for HGET" test, query the string key
			if tt.name == "WRONGTYPE on string key for HGET" {
				getKey = "strkey"
			}

			val, found, typeOk := s.HGet(getKey, tt.getField)
			if typeOk != tt.wantTypeOk {
				t.Errorf("HGet() typeOk = %v, want %v", typeOk, tt.wantTypeOk)
			}
			if found != tt.wantFound {
				t.Errorf("HGet() found = %v, want %v", found, tt.wantFound)
			}
			if val != tt.wantVal {
				t.Errorf("HGet() val = %q, want %q", val, tt.wantVal)
			}
		})
	}
}

func TestStore_HDel(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(s *Store)
		key         string
		fields      []string
		wantRemoved int
	}{
		{
			name: "delete existing field",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1", "f2": "v2"})
			},
			key:         "h",
			fields:      []string{"f1"},
			wantRemoved: 1,
		},
		{
			name: "delete missing field",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1"})
			},
			key:         "h",
			fields:      []string{"f_missing"},
			wantRemoved: 0,
		},
		{
			name:        "delete from missing key",
			key:         "nokey",
			fields:      []string{"f1"},
			wantRemoved: 0,
		},
		{
			name: "WRONGTYPE",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			key:         "strkey",
			fields:      []string{"f1"},
			wantRemoved: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.setup != nil {
				tt.setup(s)
			}
			removed := s.HDel(tt.key, tt.fields)
			if removed != tt.wantRemoved {
				t.Errorf("HDel() = %d, want %d", removed, tt.wantRemoved)
			}
		})
	}
}

func TestStore_HGetAll(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(s *Store)
		key        string
		wantMap    map[string]string
		wantTypeOk bool
	}{
		{
			name: "returns all pairs",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1", "f2": "v2"})
			},
			key:        "h",
			wantMap:    map[string]string{"f1": "v1", "f2": "v2"},
			wantTypeOk: true,
		},
		{
			name:       "missing key returns nil map",
			key:        "nokey",
			wantMap:    nil,
			wantTypeOk: true,
		},
		{
			name: "WRONGTYPE",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			key:        "strkey",
			wantMap:    nil,
			wantTypeOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.setup != nil {
				tt.setup(s)
			}
			m, typeOk := s.HGetAll(tt.key)
			if typeOk != tt.wantTypeOk {
				t.Errorf("HGetAll() typeOk = %v, want %v", typeOk, tt.wantTypeOk)
			}
			if tt.wantMap == nil {
				if m != nil {
					t.Errorf("HGetAll() map = %v, want nil", m)
				}
			} else {
				if len(m) != len(tt.wantMap) {
					t.Errorf("HGetAll() map len = %d, want %d", len(m), len(tt.wantMap))
				}
				for k, v := range tt.wantMap {
					if m[k] != v {
						t.Errorf("HGetAll() map[%q] = %q, want %q", k, m[k], v)
					}
				}
			}
		})
	}
}

func TestStore_HExists(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(s *Store)
		key        string
		field      string
		wantExists bool
		wantTypeOk bool
	}{
		{
			name: "existing field",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1"})
			},
			key:        "h",
			field:      "f1",
			wantExists: true,
			wantTypeOk: true,
		},
		{
			name: "missing field",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1"})
			},
			key:        "h",
			field:      "f_missing",
			wantExists: false,
			wantTypeOk: true,
		},
		{
			name:       "missing key",
			key:        "nokey",
			field:      "f1",
			wantExists: false,
			wantTypeOk: true,
		},
		{
			name: "WRONGTYPE",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			key:        "strkey",
			field:      "f1",
			wantExists: false,
			wantTypeOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.setup != nil {
				tt.setup(s)
			}
			exists, typeOk := s.HExists(tt.key, tt.field)
			if typeOk != tt.wantTypeOk {
				t.Errorf("HExists() typeOk = %v, want %v", typeOk, tt.wantTypeOk)
			}
			if exists != tt.wantExists {
				t.Errorf("HExists() exists = %v, want %v", exists, tt.wantExists)
			}
		})
	}
}

func TestStore_HLen(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(s *Store)
		key     string
		wantLen int
	}{
		{
			name: "correct count",
			setup: func(s *Store) {
				s.HSet("h", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})
			},
			key:     "h",
			wantLen: 3,
		},
		{
			name:    "missing key returns 0",
			key:     "nokey",
			wantLen: 0,
		},
		{
			name: "WRONGTYPE",
			setup: func(s *Store) {
				s.Set("strkey", "val")
			},
			key:     "strkey",
			wantLen: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if tt.setup != nil {
				tt.setup(s)
			}
			got := s.HLen(tt.key)
			if got != tt.wantLen {
				t.Errorf("HLen() = %d, want %d", got, tt.wantLen)
			}
		})
	}
}

func TestStore_HashExpiry(t *testing.T) {
	s := New()
	s.HSet("h", map[string]string{"f1": "v1"})
	s.Expire("h", 1)

	// Verify field is accessible before expiry
	val, found, typeOk := s.HGet("h", "f1")
	if !found || !typeOk || val != "v1" {
		t.Fatalf("HGet before expiry: val=%q found=%v typeOk=%v", val, found, typeOk)
	}

	time.Sleep(1100 * time.Millisecond)

	// After expiry, should behave as key not found
	val, found, typeOk = s.HGet("h", "f1")
	if found {
		t.Errorf("HGet after expiry should not find field, got val=%q", val)
	}
	if !typeOk {
		t.Errorf("HGet after expiry should have typeOk=true (key gone, not wrong type)")
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
