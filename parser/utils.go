package parser

import (
	"strconv"
	"strings"
)

// parseValues parses comma-separated values
func parseValues(str string) []interface{} {
	var values []interface{}
	parts := strings.Split(str, ",")
	for _, part := range parts {
		values = append(values, parseValue(strings.TrimSpace(part)))
	}
	return values
}

// parseValue parses a single value
func parseValue(str string) interface{} {
	str = strings.TrimSpace(str)

	// String (quoted)
	if (strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'")) ||
		(strings.HasPrefix(str, "\"") && strings.HasSuffix(str, "\"")) {
		return str[1 : len(str)-1]
	}

	// Bool
	if strings.ToLower(str) == "true" {
		return true
	}
	if strings.ToLower(str) == "false" {
		return false
	}

	// Int/Float
	if val, err := strconv.ParseFloat(str, 64); err == nil {
		return val
	}

	// Default: string
	return str
}
