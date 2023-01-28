package mr

import (
	"reflect"
	"testing"
)

func TestSynchronousSlice_GetElementsSnap(t *testing.T) {
	sl := NewSynchronousSlice[string]()
	sl.Append("a")
	sl.Append("b")
	sl.Append("c")

	snap := sl.GetElementsSnap()
	sl.Set(0, "d")
	reflect.DeepEqual(snap, []string{"a", "b"})
}
