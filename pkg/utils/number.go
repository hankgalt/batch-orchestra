package utils

import (
	"errors"
	"math"
	"math/big"
	"strconv"
	"strings"
	"unicode"
)

const (
	ERR_EMPTY_STRING  = "empty string"
	ERR_INVALID_INT   = "invalid integer format"
	ERR_INVALID_FLOAT = "invalid float format"
	ERR_OUT_OF_RANGE  = "value out of int64 range"
)

var (
	ErrEmptyString  = errors.New(ERR_EMPTY_STRING)
	ErrInvalidInt   = errors.New(ERR_INVALID_INT)
	ErrInvalidFloat = errors.New(ERR_INVALID_FLOAT)
	ErrOutOfRange   = errors.New(ERR_OUT_OF_RANGE)
)

// ParseBigInt converts a "large number" string into *big.Int.
// Supports optional separators (',' or '_') and 0x/0b/0o prefixes.
// n1, _ := ParseBigInt("123456789012345678901234567890")
// n2, _ := ParseBigInt("0xFFFFFFFFFFFFFFFFFFFFFFFF")
// n3, _ := ParseBigInt("-1_000_000_000_000")
func ParseBigInt(s string) (*big.Int, error) {
	if s == "" {
		return nil, ErrEmptyString
	}
	// Trim spaces
	s = strings.TrimSpace(s)

	// Keep leading sign for later
	sign := ""
	if len(s) > 0 && (s[0] == '+' || s[0] == '-') {
		sign = s[:1]
		s = s[1:]
	}

	// Detect base from prefixes
	base := 10
	switch {
	case strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X"):
		base, s = 16, s[2:]
	case strings.HasPrefix(s, "0b") || strings.HasPrefix(s, "0B"):
		base, s = 2, s[2:]
	case strings.HasPrefix(s, "0o") || strings.HasPrefix(s, "0O"):
		base, s = 8, s[2:]
	}

	// Remove common separators
	clean := make([]rune, 0, len(s))
	for _, r := range s {
		if r == ',' || r == '_' || unicode.IsSpace(r) {
			continue
		}
		clean = append(clean, r)
	}
	s = sign + string(clean)

	z := new(big.Int)
	if _, ok := z.SetString(s, base); !ok {
		return nil, ErrInvalidInt
	}
	return z, nil
}

// ParseBigFloat parses a decimal string into *big.Float with the given precision (in bits).
// Example precision: 256 or 512 for high accuracy.
// f1, _ := ParseBigFloat("3.1415926535897932384626433832795028841971", 256)
// f2, _ := ParseBigFloat("1_234_567_890.000_000_001", 256)
func ParseBigFloat(s string, prec uint) (*big.Float, error) {
	if s == "" {
		return nil, ErrEmptyString
	}
	s = strings.TrimSpace(s)

	// Strip separators
	b := make([]rune, 0, len(s))
	for _, r := range s {
		if r == ',' || r == '_' || unicode.IsSpace(r) {
			continue
		}
		b = append(b, r)
	}
	s = string(b)

	f, ok := new(big.Float).SetPrec(prec).SetString(s)
	if !ok {
		return nil, ErrInvalidFloat
	}
	// (Optional) choose rounding mode, e.g., ToNearestEven
	f.SetMode(big.ToNearestEven)
	return f, nil
}

// ParseInt64 converts a numeric string into int64.
// Returns error if the value doesn't fit in int64.
// n1, _ := ParseInt64("1234567890")         // fits in int64
// n2, err := ParseInt64("999999999999999999999999999")
// err: value out of int64 range
func ParseInt64(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, ErrEmptyString
	}

	// First try native 64-bit parse
	v, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return v, nil
	}

	// If it fails, maybe it's just too big
	bigInt, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return 0, ErrInvalidInt
	}

	if bigInt.Cmp(big.NewInt(math.MaxInt64)) > 0 ||
		bigInt.Cmp(big.NewInt(math.MinInt64)) < 0 {
		return 0, ErrOutOfRange
	}

	return bigInt.Int64(), nil
}

// Int64ToString converts an int64 into its decimal string representation.
func Int64ToString(v int64) string {
	return strconv.FormatInt(v, 10)
}
