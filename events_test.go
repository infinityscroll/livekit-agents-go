// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"reflect"
	"sync"
	"testing"
)

func TestEventEmitterOrderedUnsubscribeAndPanicIsolation(t *testing.T) {
	t.Parallel()
	var emitter EventEmitter[int]
	var mu sync.Mutex
	var got []int
	emitter.Subscribe(func(value int) {
		mu.Lock()
		got = append(got, value)
		mu.Unlock()
	})
	emitter.Subscribe(func(int) { panic("subscriber bug") })
	unsubscribe := emitter.Subscribe(func(value int) {
		mu.Lock()
		got = append(got, value*10)
		mu.Unlock()
	})
	emitter.Emit(2)
	unsubscribe()
	unsubscribe()
	emitter.Emit(3)
	mu.Lock()
	defer mu.Unlock()
	if want := []int{2, 20, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("callbacks = %v, want %v", got, want)
	}
}
