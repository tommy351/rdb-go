package convert

import (
	"fmt"
	"reflect"
	"strconv"
	"unsafe"
)

type Error struct {
	Value interface{}
	Type  string
}

func (c Error) Error() string {
	return fmt.Sprintf("unable to convert value %v to %s", c.Value, c.Type)
}

func Float64(value interface{}) (float64, error) {
	v := reflect.ValueOf(value)

	// nolint: exhaustive
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	case reflect.String:
		return strconv.ParseFloat(v.String(), 64)
	}

	return 0, Error{Value: value, Type: "float64"}
}

func String(value interface{}) (string, error) {
	v := reflect.ValueOf(value)

	// nolint: exhaustive
	switch v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'f', -1, 64), nil
	}

	return "", Error{Value: value, Type: "string"}
}

func BytesToString(buf []byte) string {
	// From strings.Builder.String()
	return *(*string)(unsafe.Pointer(&buf))
}
