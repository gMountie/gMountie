package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateContext_NilCallerDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("createContext panicked on nil caller: %v", r)
		}
	}()
	c := createContext(context.Background(), nil)
	assert.NotNil(t, c)
	assert.Equal(t, uint32(0), c.Caller.Owner.Uid)
	assert.Equal(t, uint32(0), c.Caller.Owner.Gid)
	assert.Equal(t, uint32(0), c.Caller.Pid)
}
