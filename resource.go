// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"fmt"
	"sync"
)

// ResourceBag carries process-prewarmed dependencies without serialization or
// reflection on access. It is safe for concurrent jobs in in-process mode.
type ResourceBag struct {
	mu     sync.RWMutex
	values map[string]any
}

func NewResourceBag() *ResourceBag { return &ResourceBag{values: make(map[string]any)} }

func (b *ResourceBag) Put(key string, value any) error {
	if key == "" {
		return fmt.Errorf("resource key must not be empty")
	}
	b.mu.Lock()
	b.values[key] = value
	b.mu.Unlock()
	return nil
}
func (b *ResourceBag) Delete(key string) { b.mu.Lock(); delete(b.values, key); b.mu.Unlock() }
func (b *ResourceBag) get(key string) (any, bool) {
	b.mu.RLock()
	value, ok := b.values[key]
	b.mu.RUnlock()
	return value, ok
}

func PutResource[T any](bag *ResourceBag, key string, value T) error {
	if bag == nil {
		return fmt.Errorf("resource bag is nil")
	}
	return bag.Put(key, value)
}

func Resource[T any](bag *ResourceBag, key string) (T, bool) {
	var zero T
	if bag == nil {
		return zero, false
	}
	value, ok := bag.get(key)
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	return typed, ok
}
