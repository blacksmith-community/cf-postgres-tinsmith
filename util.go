package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
)

func cfg(deflt, env string) string {
	if s := os.Getenv(env); s != "" {
		return s
	}
	return deflt
}

func info(m string, args ...interface{}) {
	fmt.Printf(m, args...)
}

func oops(m string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, m, args...)
}

var chars = "0123456789abcdefghijklmnopqrstuvwxyz"

func random(n int) string {
	var buffer bytes.Buffer

	for i := 0; i < n; i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			return ""
		}
		indexInt := index.Int64()
		buffer.WriteString(string(chars[indexInt]))
	}

	return buffer.String()
}
