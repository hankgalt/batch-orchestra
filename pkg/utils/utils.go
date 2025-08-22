package utils

import (
	"bytes"
	"encoding/csv"
	"slices"
	"strings"
	"unicode"
)

// CleanAlphaNumericsArr applies CleanAlphaNumerics to each string in the slice.
// It returns a new slice with cleaned strings.
// The exclude parameter allows specifying characters that should not be removed.
func CleanAlphaNumericsArr(s []string, exclude []rune) []string {
	for i, str := range s {
		s[i] = CleanAlphaNumerics(str, true, exclude)
	}
	return s
}

// CleanHeaders cleans the headers by removing leading/trailing spaces
func CleanHeaders(headers []string) []string {
	for i, header := range headers {
		headers[i] = CleanAlphaNumerics(header, false, []rune{'-', '_'})
	}
	return headers
}

// CleanRecord removes unwanted characters from a record string.
// It replaces asterisks with an empty string and cleans quotes from the record.
// It returns the cleaned record string.
func CleanRecord(recStr string) string {
	cleanedStr := strings.ReplaceAll(recStr, "*", "")
	cleanedStr = CleanCSVLineQuotes(cleanedStr, "|")
	return cleanedStr
}

// ReadSingleRecord reads a single record from a string and returns it as a slice of strings.
// It uses a CSV reader with '|' as the delimiter and allows variable number of fields per record.
// If an error occurs during reading, it returns an error.
func ReadSingleRecord(recStr string) ([]string, error) {
	bReader := bytes.NewReader([]byte(recStr))
	csvReader := csv.NewReader(bReader)
	csvReader.Comma = '|'
	csvReader.FieldsPerRecord = -1

	record, err := csvReader.Read()
	if err != nil {
		return nil, err
	}
	return record, nil
}

// CleanAlphaNumerics removes non-alphanumeric characters from the string.
// If spaceSeparator is true, it replaces non-alphanumeric characters with a space; otherwise, it replaces them with an underscore.
// It also trims leading and trailing non-alphanumeric characters.
// The function returns an empty string if no alphanumeric characters are found.
func CleanAlphaNumerics(s string, spaceSeparator bool, exclude []rune) string {
	runes := []rune(s)

	start := AlNumStart(s, exclude)
	end := AlNumEnd(s, exclude)

	if start > end || start < 0 || end < 0 {
		return ""
	}

	result := []rune{}
	prevUnderscore := false
	for i := start; i <= end; i++ {
		if unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || slices.Contains(exclude, runes[i]) {
			result = append(result, runes[i])
			prevUnderscore = false
		} else {
			if !prevUnderscore && i+1 <= end && (unicode.IsLetter(runes[i+1]) || unicode.IsDigit(runes[i+1]) || slices.Contains(exclude, runes[i+1])) {
				if spaceSeparator {
					result = append(result, ' ')
				} else {
					result = append(result, '_')
				}
				prevUnderscore = true
			}
		}
	}

	return string(result)
}

// AlNumStart returns the index of the first alphanumeric character in the string.
// If no alphanumeric character is found, it returns 0.
func AlNumStart(s string, exclude []rune) int {
	start := 0
	runes := []rune(s)
	n := len(runes)
	if n == 0 {
		return -1
	}
	for start < n && !unicode.IsLetter(runes[start]) && !unicode.IsDigit(runes[start]) && !slices.Contains(exclude, runes[start]) {
		start++
	}
	return start
}

// AlNumEnd returns the index of the last alphanumeric character in the string.
// If string is empty or no alphanumeric character is found, it returns -1.
func AlNumEnd(s string, exclude []rune) int {
	runes := []rune(s)
	if len(runes) == 0 {
		return -1
	}
	// Find the last alphanumeric character
	end := len(s) - 1
	for end >= 0 && !unicode.IsLetter(runes[end]) && !unicode.IsDigit(runes[end]) && !slices.Contains(exclude, runes[end]) {
		end--
	}
	return end
}

// CleanCSVLineQuotes removes quotes from the beginning and end of each field in a CSV line.
func CleanCSVLineQuotes(line, separator string) string {
	if separator == "" {
		separator = ","
	}
	split := strings.Split(line, separator)
	for i, f := range split {
		fIdx, lIdx := strings.Index(f, `"`), strings.LastIndex(f, `"`)
		if fIdx >= 0 && lIdx > 0 && fIdx < lIdx && len(f) >= 2 {
			split[i] = f[fIdx+1 : lIdx]
		}
	}
	return strings.Join(split, separator)
}
