package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArbiter(t *testing.T) {
	_, err := NewArbiter("you_would_never_define_an_arbiter_in_this_name")
	assert.NotNil(t, err)
}
