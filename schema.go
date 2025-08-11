package metrics

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/hamba/avro/v2"
)

// PrimitiveType returns the Avro type for a given Go kind.
// Based on https://github.com/hamba/avro/tree/main?tab=readme-ov-file#types-conversions
func primitiveType(t reflect.Kind) avro.Type {
	switch t {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16:
		return avro.Int
	case reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return avro.Long
	case reflect.Float32:
		return avro.Float
	case reflect.Float64:
		return avro.Double
	case reflect.Bool:
		return avro.Boolean
	case reflect.String:
		return avro.String
	case reflect.Interface:
		return avro.String
	default:
		return ""
	}
}

// FieldToSchema converts a struct field to an Avro schema.
func fieldToSchema(f reflect.StructField) (avro.Schema, error) {
	switch f.Type.Kind() {
	case reflect.String:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	case reflect.Uint, reflect.Uint64, reflect.Uintptr:
		//// Fixed schema with a size of 8 bytes for uint64 and uintptr
		//fs, err := avro.NewFixedSchema(strings.ToLower(f.Name), "", 8, nil)
		//if err != nil {
		//	return nil, fmt.Errorf("failed to create fixed schema: %w", err)
		//}
		//return fs, nil
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Int32, reflect.Int16, reflect.Int8,
		reflect.Uint16, reflect.Uint8:
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case reflect.Int64, reflect.Uint32, reflect.Int:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case reflect.Float32:
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case reflect.Float64:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case reflect.Bool:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case reflect.Array, reflect.Slice:
		if f.Type.Elem().Kind() == reflect.Uint8 {
			return avro.NewPrimitiveSchema(avro.Bytes, nil), nil
		}

		if f.Type.Elem().Kind() == reflect.Struct {
			fs, err := structToSchema(strings.ToLower(f.Name), f.Type.Elem())
			if err != nil {
				return nil, fmt.Errorf("failed to create struct schema: %w", err)
			}
			return avro.NewArraySchema(fs), nil
		}
		pType := primitiveType(f.Type.Elem().Kind())

		if pType != "" {
			return avro.NewArraySchema(avro.NewPrimitiveSchema(pType, nil)), nil
		}
	case reflect.Map:
		if f.Type.Key() != reflect.TypeOf("") {
			return nil, fmt.Errorf("unsupported map key type %v", f.Type.Key())
		}

		return avro.NewMapSchema(avro.NewPrimitiveSchema(primitiveType(f.Type.Elem().Kind()), nil)), nil
	case reflect.Struct:
		if f.Type.String() == "time.Time" {
			return avro.NewPrimitiveSchema(avro.Long, avro.NewPrimitiveLogicalSchema(avro.TimestampMillis)), nil
		}

		fs, err := structToSchema(strings.ToLower(f.Name), f.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to create struct schema: %w", err)
		}
		return fs, nil
	default:
		return nil, fmt.Errorf("unsupported kind %v", f.Type.Kind())
	}

	return nil, fmt.Errorf("unsupported kind %v", f.Type.Kind())
}

func structToSchema(schemaName string, s any) (avro.Schema, error) {
	var rt reflect.Type

	switch v := s.(type) {
	case reflect.Type:
		rt = v
	default:
		rt = reflect.TypeOf(s)
		if rt.Kind() == reflect.Pointer {
			rt = rt.Elem()
		}
	}

	if rt.Kind() != reflect.Struct {
		return nil, errors.New("need a struct")
	}

	var fields []*avro.Field
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("avro")
		if tag == "" {
			continue
		}

		fs, err := fieldToSchema(f)
		if err != nil {
			return nil, fmt.Errorf("failed to create field schema: %w", err)
		}

		field, err := avro.NewField(tag, fs)
		if err != nil {
			return nil, fmt.Errorf("failed to create field: %w", err)
		}
		fields = append(fields, field)
	}
	return avro.NewRecordSchema(schemaName, "", fields)
}
