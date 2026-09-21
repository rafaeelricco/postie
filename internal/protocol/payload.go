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

// ConvertValue validates one Debezium value and returns the JSON value sent in
// a Postie envelope. It is pure, and errors name the field but never echo the
// value, which may hold customer data.
//
// Numbers keep their literal digits and never pass through float64:
//
//	ConvertValue(stream.PGInt8, []byte("9007199254740993"), "id")      // 9007199254740993
//	ConvertValue(stream.PGFloat8, []byte("1.5e3"), "price")            // 1500
//	ConvertValue(stream.PGTimestamp, []byte("1700000000000000"), "at") // "2023-11-14 22:13:20"
//	ConvertValue(stream.PGInt2, []byte("70000"), "n")                  // error: field "n" is not a valid int2
func ConvertValue(typ stream.PGType, raw json.RawMessage, field string) (json.RawMessage, error) {
	if isNull(raw) {
		return json.RawMessage("null"), nil
	}
	switch typ {
	case stream.PGInt2:
		return integerLiteral(raw, field, "int2", 16)
	case stream.PGInt4:
		return integerLiteral(raw, field, "int4", 32)
	case stream.PGInt8:
		return integerLiteral(raw, field, "int8", 64)
	case stream.PGFloat4:
		return floatLiteral(raw, field, "float4", 32)
	case stream.PGFloat8:
		return floatLiteral(raw, field, "float8", 64)
	case stream.PGBool:
		return boolLiteral(raw, field)
	case stream.PGJSON:
		return jsonText(raw, field)
	case stream.PGBytea:
		return base64Text(raw, field)
	case stream.PGText:
		return plainText(raw, field)
	case stream.PGTimestamp:
		return timestampFromMicros(raw, field)
	case stream.PGTimestamptz:
		return timestampFromRFC3339(raw, field)
	default:
		return nil, fmt.Errorf("field %q has unsupported type", field)
	}
}

// integerLiteral accepts raw unchanged when it fits a signed integer of bits.
func integerLiteral(raw json.RawMessage, field, name string, bits int) (json.RawMessage, error) {
	if _, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, bits); err != nil {
		return nil, fmt.Errorf("field %q is not a valid %s", field, name)
	}
	return raw, nil
}

// floatLiteral accepts raw as a literal only after checking it parses as a
// float of bits: expandDecimal alone would accept digit strings no float of
// that width can represent, such as an exponent far outside the type's range.
func floatLiteral(raw json.RawMessage, field, name string, bits int) (json.RawMessage, error) {
	if _, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), bits); err != nil {
		return nil, fmt.Errorf("field %q is not a valid %s", field, name)
	}
	expanded, err := expandDecimal(raw)
	if err != nil {
		return nil, fmt.Errorf("field %q is not a valid %s", field, name)
	}
	return json.RawMessage(expanded), nil
}

// boolLiteral accepts raw unchanged when it is exactly "true" or "false".
func boolLiteral(raw json.RawMessage, field string) (json.RawMessage, error) {
	if v := strings.TrimSpace(string(raw)); v != "true" && v != "false" {
		return nil, fmt.Errorf("field %q is not a valid %s", field, "bool")
	}
	return raw, nil
}

// jsonText accepts raw unchanged when it is a JSON string that itself decodes
// to valid JSON: the column holds JSON text, quoted once for the wire.
func jsonText(raw json.RawMessage, field string) (json.RawMessage, error) {
	value, err := decodeString(raw, field)
	if err != nil || !json.Valid([]byte(value)) {
		return nil, fmt.Errorf("field %q is not valid JSON text", field)
	}
	return raw, nil
}

// base64Text accepts raw unchanged when it is a JSON string holding standard
// base64, padded or not.
func base64Text(raw json.RawMessage, field string) (json.RawMessage, error) {
	value, err := decodeString(raw, field)
	if err != nil {
		return nil, err
	}
	if _, err := base64.StdEncoding.DecodeString(value); err != nil {
		if _, rawErr := base64.RawStdEncoding.DecodeString(value); rawErr != nil {
			return nil, fmt.Errorf("field %q is not valid base64", field)
		}
	}
	return raw, nil
}

// plainText accepts raw unchanged when it is a JSON string.
func plainText(raw json.RawMessage, field string) (json.RawMessage, error) {
	if _, err := decodeString(raw, field); err != nil {
		return nil, err
	}
	return raw, nil
}

// timestampFromMicros reads raw as microseconds since the Unix epoch and
// renders it without a zone offset, matching a plain PostgreSQL timestamp.
func timestampFromMicros(raw json.RawMessage, field string) (json.RawMessage, error) {
	micros, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("field %q is not valid timestamp microseconds", field)
	}
	return json.RawMessage(strconv.Quote(formatTimestamp(time.UnixMicro(micros).UTC(), false))), nil
}

// timestampFromRFC3339 reads raw as a JSON string holding RFC3339 and renders
// it in UTC with a "+00" zone suffix, matching a PostgreSQL timestamptz.
func timestampFromRFC3339(raw json.RawMessage, field string) (json.RawMessage, error) {
	value, err := decodeString(raw, field)
	if err != nil {
		return nil, err
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, fmt.Errorf("field %q is not valid RFC3339", field)
	}
	return json.RawMessage(strconv.Quote(formatTimestamp(parsed.UTC(), true))), nil
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

// decimal is a JSON number taken apart: its sign, its significant digits, and
// where the point sits among them. A decimal only comes from parseDecimal, so
// digits is never empty and point is always within ±maxExpandedDigits. That is
// why String has no error to return.
//
//	"1.5e3"   -> decimal{digits: "15", point: 4}                  -> "1500"
//	"-2.5E-3" -> decimal{negative: true, digits: "25", point: -2} -> "-0.0025"
type decimal struct {
	negative bool
	digits   string
	point    int // how many digits sit before the point; may be <= 0 or > len(digits)
}

// numberLiteral decodes raw as a JSON number and returns its literal text
// exactly as written, so later steps never round it through float64.
//
//	numberLiteral([]byte(" 1.50e3 ")) // "1.50e3", nil
func numberLiteral(raw []byte) (string, error) {
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
	return n.String(), nil
}

// splitSign separates a leading minus sign from the digits that follow.
//
//	splitSign("-2.5") // true, "2.5"
func splitSign(number string) (negative bool, unsigned string) {
	if strings.HasPrefix(number, "-") {
		return true, number[1:]
	}
	return false, number
}

// splitExponent pulls a trailing "e"/"E" exponent off number, so later steps
// work on the mantissa alone. A number with no exponent passes through
// unchanged.
//
//	splitExponent("1.5e3") // "1.5", 3, nil
//	splitExponent("1.5")   // "1.5", 0, nil
func splitExponent(number string) (mantissa string, exponent int, err error) {
	index := strings.IndexAny(number, "eE")
	if index < 0 {
		return number, 0, nil
	}
	parsed, err := strconv.Atoi(number[index+1:])
	if err != nil || parsed > maxExpandedDigits || parsed < -maxExpandedDigits {
		return "", 0, fmt.Errorf("decimal exponent out of range")
	}
	return number[:index], parsed, nil
}

// dropPoint removes the decimal point from mantissa and folds exponent into
// the point's position among the remaining digits.
//
//	dropPoint("1.5", 3) // "15", 4
func dropPoint(mantissa string, exponent int) (digits string, point int) {
	dot := strings.IndexByte(mantissa, '.')
	if dot < 0 {
		return mantissa, len(mantissa) + exponent
	}
	return mantissa[:dot] + mantissa[dot+1:], dot + exponent
}

// parseDecimal reads a JSON number literal into its sign, digits, and point
// position, rejecting anything malformed or whose expansion would place the
// point more than maxExpandedDigits away from the digits.
func parseDecimal(raw []byte) (decimal, error) {
	literal, err := numberLiteral(raw)
	if err != nil {
		return decimal{}, err
	}
	negative, unsigned := splitSign(literal)
	mantissa, exponent, err := splitExponent(unsigned)
	if err != nil {
		return decimal{}, err
	}
	digits, point := dropPoint(mantissa, exponent)
	if digits == "" || point < -maxExpandedDigits || point > maxExpandedDigits {
		return decimal{}, fmt.Errorf("decimal expansion out of range")
	}
	return decimal{negative: negative, digits: digits, point: point}, nil
}

// placePoint writes digits with a decimal point at point, padding with zeros
// on whichever side point falls outside the digits.
//
//	placePoint("15", 4)  // "1500"
//	placePoint("25", -2) // "0.0025"
func placePoint(digits string, point int) string {
	switch {
	case point <= 0:
		return "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		return digits + strings.Repeat("0", point-len(digits))
	default:
		return digits[:point] + "." + digits[point:]
	}
}

// trimLeadingZeros strips leading zeros from number's integer part, leaving a
// single "0" rather than an empty string, and leaves any fraction untouched.
//
//	trimLeadingZeros("0.0025") // "0.0025"
//	trimLeadingZeros("00")     // "0"
func trimLeadingZeros(number string) string {
	dot := strings.IndexByte(number, '.')
	if dot < 0 {
		trimmed := strings.TrimLeft(number, "0")
		if trimmed == "" {
			return "0"
		}
		return trimmed
	}
	integer, fraction := number[:dot], number[dot+1:]
	integer = strings.TrimLeft(integer, "0")
	if integer == "" {
		integer = "0"
	}
	return integer + "." + fraction
}

// String places the point among digits, trims the leading zero it
// introduces, and reattaches the sign. It never fails: parseDecimal already
// bounded point, so the zero padding stays within maxExpandedDigits.
func (d decimal) String() string {
	out := trimLeadingZeros(placePoint(d.digits, d.point))
	sign := ""
	if d.negative {
		sign = "-"
	}
	return sign + out
}

// expandDecimal rewrites a JSON number without an exponent, digit for digit,
// so a float never loses precision by passing through float64.
func expandDecimal(raw []byte) (string, error) {
	d, err := parseDecimal(raw)
	if err != nil {
		return "", err
	}
	return d.String(), nil
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
