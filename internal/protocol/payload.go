package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rafaeelricco/postie/internal/stream"
)

// ConvertValue validates and converts a Debezium value to the JSON value sent
// in a Postie envelope. Numeric literals are preserved without float64.
func ConvertValue(typ stream.PGType, raw json.RawMessage, field string) (json.RawMessage, error) {
	if isNull(raw) {
		return json.RawMessage("null"), nil
	}
	valid := func(name string, parse func() error) (json.RawMessage, error) {
		if err := parse(); err != nil {
			return nil, fmt.Errorf("field %q is not a valid %s", field, name)
		}
		return json.RawMessage(raw), nil
	}
	switch typ {
	case stream.PGInt2:
		return valid("int2", func() error { _, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 16); return err })
	case stream.PGInt4:
		return valid("int4", func() error { _, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 32); return err })
	case stream.PGInt8:
		return valid("int8", func() error { _, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); return err })
	case stream.PGFloat4, stream.PGFloat8:
		bits := 32
		name := "float4"
		if typ == stream.PGFloat8 {
			bits, name = 64, "float8"
		}
		if _, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), bits); err != nil {
			return nil, fmt.Errorf("field %q is not a valid %s", field, name)
		}
		expanded, err := expandDecimal(raw)
		if err != nil {
			return nil, fmt.Errorf("field %q is not a valid %s", field, name)
		}
		return json.RawMessage(expanded), nil
	case stream.PGBool:
		return valid("bool", func() error {
			if v := strings.TrimSpace(string(raw)); v != "true" && v != "false" {
				return fmt.Errorf("invalid")
			}
			return nil
		})
	case stream.PGJSON:
		value, err := decodeString(raw, field)
		if err != nil || !json.Valid([]byte(value)) {
			return nil, fmt.Errorf("field %q is not valid JSON text", field)
		}
	case stream.PGBytea:
		value, err := decodeString(raw, field)
		if err != nil {
			return nil, err
		}
		if _, err := base64.StdEncoding.DecodeString(value); err != nil {
			if _, rawErr := base64.RawStdEncoding.DecodeString(value); rawErr != nil {
				return nil, fmt.Errorf("field %q is not valid base64", field)
			}
		}
	case stream.PGText:
		if _, err := decodeString(raw, field); err != nil {
			return nil, err
		}
	case stream.PGTimestamp:
		micros, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("field %q is not valid timestamp microseconds", field)
		}
		return json.RawMessage(strconv.Quote(formatTimestamp(time.UnixMicro(micros).UTC(), false))), nil
	case stream.PGTimestamptz:
		value, err := decodeString(raw, field)
		if err != nil {
			return nil, err
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return nil, fmt.Errorf("field %q is not valid RFC3339", field)
		}
		return json.RawMessage(strconv.Quote(formatTimestamp(parsed.UTC(), true))), nil
	default:
		return nil, fmt.Errorf("field %q has unsupported type", field)
	}
	return json.RawMessage(raw), nil
}

func formatTimestamp(value time.Time, zone bool) string {
	out := value.Format("2006-01-02 15:04:05")
	if fraction := strings.TrimRight(fmt.Sprintf("%09d", value.Nanosecond()), "0"); fraction != "" {
		out += "." + fraction
	}
	if zone {
		out += "+00"
	}
	return out
}

func expandDecimal(raw []byte) (string, error) {
	s := strings.TrimSpace(string(raw))
	if s == "" || !json.Valid([]byte(s)) {
		return "", fmt.Errorf("invalid decimal")
	}
	decoder := json.NewDecoder(strings.NewReader(s))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", fmt.Errorf("invalid decimal")
	}
	n, ok := value.(json.Number)
	if !ok {
		return "", fmt.Errorf("not a decimal")
	}
	number := n.String()
	sign := ""
	if strings.HasPrefix(number, "-") {
		sign, number = "-", number[1:]
	}
	exponent := 0
	if index := strings.IndexAny(number, "eE"); index >= 0 {
		parsed, err := strconv.Atoi(number[index+1:])
		if err != nil || parsed > maxExpandedDigits || parsed < -maxExpandedDigits {
			return "", fmt.Errorf("decimal exponent out of range")
		}
		exponent, number = parsed, number[:index]
	}
	dot := strings.IndexByte(number, '.')
	decimalPosition := len(number) + exponent
	if dot >= 0 {
		number = number[:dot] + number[dot+1:]
		decimalPosition = dot + exponent
	}
	if number == "" || decimalPosition < -maxExpandedDigits || decimalPosition > maxExpandedDigits {
		return "", fmt.Errorf("decimal expansion out of range")
	}
	var out string
	switch {
	case decimalPosition <= 0:
		out = "0." + strings.Repeat("0", -decimalPosition) + number
	case decimalPosition >= len(number):
		out = number + strings.Repeat("0", decimalPosition-len(number))
	default:
		out = number[:decimalPosition] + "." + number[decimalPosition:]
	}
	if dot := strings.IndexByte(out, '.'); dot >= 0 {
		integer, fraction := out[:dot], out[dot+1:]
		integer = strings.TrimLeft(integer, "0")
		if integer == "" {
			integer = "0"
		}
		out = integer + "." + fraction
	} else {
		out = strings.TrimLeft(out, "0")
		if out == "" {
			out = "0"
		}
	}
	return sign + out, nil
}

func decodeString(raw []byte, field string) (string, error) {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("field %q is not a string", field)
	}
	return value, nil
}

func isNull(raw []byte) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

const maxExpandedDigits = 1 << 20
