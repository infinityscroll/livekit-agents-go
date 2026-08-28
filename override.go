// SPDX-License-Identifier: Apache-2.0

package agents

type overrideState uint8

const (
	overrideInherited overrideState = iota
	overrideUse
	overrideDisabled
)

// Override models the Python/TypeScript inherit/use/disable tri-state. Its zero
// value inherits.
type Override[T any] struct {
	state overrideState
	value T
}

func Use[T any](value T) Override[T]    { return Override[T]{state: overrideUse, value: value} }
func Disable[T any]() Override[T]       { return Override[T]{state: overrideDisabled} }
func (o Override[T]) IsInherited() bool { return o.state == overrideInherited }
func (o Override[T]) IsDisabled() bool  { return o.state == overrideDisabled }
func (o Override[T]) Value() (T, bool)  { return o.value, o.state == overrideUse }

// Resolve returns the selected value and whether the component is enabled.
func (o Override[T]) Resolve(inherited T, inheritedEnabled bool) (T, bool) {
	switch o.state {
	case overrideUse:
		return o.value, true
	case overrideDisabled:
		var zero T
		return zero, false
	default:
		return inherited, inheritedEnabled
	}
}
