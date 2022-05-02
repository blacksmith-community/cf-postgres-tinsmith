package main

import (
	"os"
	"testing"
)

func TestCfg(t *testing.T) {
	value := cfg("default", "foo")
	if value != "default" {
		t.Fatalf(`expected cfg("default", "foo") = %q, got: %v`, "default", value)
	}

	expectedValue := "bar"
	os.Setenv("foo", expectedValue)
	value = cfg("default", "foo")
	if value != expectedValue {
		t.Fatalf(`expected cfg("default", "foo") = %q, got: %v`, expectedValue, value)
	}
	os.Unsetenv("foo")
}
