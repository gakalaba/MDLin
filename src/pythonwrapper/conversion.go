package main

import (
	"crypto/md5"
	"encoding/binary"
	"strings"
)

func stringToInt64Hash(input string) int64 {
	// Normalize the input string
	normalized := strings.ToLower(strings.TrimSpace(input))

	// Generate MD5 hash
	hash := md5.Sum([]byte(normalized))

	// Convert first 8 bytes of MD5 hash to int64
	return int64(binary.BigEndian.Uint64(hash[:8]))
}