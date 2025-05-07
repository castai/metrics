package metrics

import (
	"bytes"
	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"testing"
	"time"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name     string
		kind     reflect.Kind
		expected avro.Type
	}{
		{"Int8", reflect.Int8, avro.Int},
		{"Int16", reflect.Int16, avro.Int},
		{"Int32", reflect.Int32, avro.Int},
		{"Uint8", reflect.Uint8, avro.Int},
		{"Uint16", reflect.Uint16, avro.Int},
		{"Int", reflect.Int, avro.Long},
		{"Int64", reflect.Int64, avro.Long},
		{"Uint32", reflect.Uint32, avro.Long},
		{"Uint64", reflect.Uint64, avro.Long},
		{"Uint", reflect.Uint, avro.Long},
		{"Float32", reflect.Float32, avro.Float},
		{"Float64", reflect.Float64, avro.Double},
		{"Bool", reflect.Bool, avro.Boolean},
		{"String", reflect.String, avro.String},
		{"Interface", reflect.Interface, avro.String},
		{"Unsupported", reflect.Chan, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := primitiveType(tt.kind)
			if result != tt.expected {
				t.Errorf("primitiveType(%v) = %v, want %v", tt.kind, result, tt.expected)
			}
		})
	}
}

func TestFieldToSchema(t *testing.T) {
	type testStruct struct {
		StringField  string            `avro:"string_field"`
		IntField     int               `avro:"int_field"`
		Int8Field    int8              `avro:"int8_field"`
		Int16Field   int16             `avro:"int16_field"`
		Int32Field   int32             `avro:"int32_field"`
		Int64Field   int64             `avro:"int64_field"`
		UintField    uint              `avro:"uint_field"`
		Uint8Field   uint8             `avro:"uint8_field"`
		Uint16Field  uint16            `avro:"uint16_field"`
		Uint32Field  uint32            `avro:"uint32_field"`
		Uint64Field  uint64            `avro:"uint64_field"`
		Float32Field float32           `avro:"float32_field"`
		Float64Field float64           `avro:"float64_field"`
		BoolField    bool              `avro:"bool_field"`
		BytesField   []byte            `avro:"bytes_field"`
		TimeField    time.Time         `avro:"time_field"`
		StringSlice  []string          `avro:"string_slice"`
		IntSlice     []int             `avro:"int_slice"`
		StringMap    map[string]string `avro:"string_map"`
		IntMap       map[string]int    `avro:"int_map"`
	}

	type nestedStruct struct {
		NestedField string `avro:"nested_field"`
	}

	type structWithNested struct {
		NestedStructField nestedStruct   `avro:"nested_struct_field"`
		NestedSlice       []nestedStruct `avro:"nested_slice"`
	}

	tests := []struct {
		name      string
		fieldName string
		wantType  avro.Type
		wantErr   bool
	}{
		{"StringField", "StringField", "string", false},
		{"IntField", "IntField", "long", false},
		{"Int8Field", "Int8Field", "int", false},
		{"Int16Field", "Int16Field", "int", false},
		{"Int32Field", "Int32Field", "int", false},
		{"Int64Field", "Int64Field", "long", false},
		{"UintField", "UintField", "fixed", false},
		{"Uint8Field", "Uint8Field", "int", false},
		{"Uint16Field", "Uint16Field", "int", false},
		{"Uint32Field", "Uint32Field", "long", false},
		{"Uint64Field", "Uint64Field", "fixed", false},
		{"Float32Field", "Float32Field", "float", false},
		{"Float64Field", "Float64Field", "double", false},
		{"BoolField", "BoolField", "boolean", false},
		{"BytesField", "BytesField", "bytes", false},
		{"TimeField", "TimeField", "long", false},
		{"StringSlice", "StringSlice", "array", false},
		{"IntSlice", "IntSlice", "array", false},
		{"StringMap", "StringMap", "map", false},
		{"IntMap", "IntMap", "map", false},
		{"NestedStructField", "NestedStructField", "record", false},
		{"NestedSlice", "NestedSlice", "array", false},
	}

	testType := reflect.TypeOf(testStruct{})
	nestedType := reflect.TypeOf(structWithNested{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var f reflect.StructField
			if tt.name == "NestedStructField" || tt.name == "NestedSlice" {
				f, _ = nestedType.FieldByName(tt.fieldName)
			} else {
				f, _ = testType.FieldByName(tt.fieldName)
			}

			schema, err := fieldToSchema(f)
			if (err != nil) != tt.wantErr {
				t.Errorf("fieldToSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				schemaType := schema.Type()
				if schemaType != tt.wantType {
					t.Errorf("fieldToSchema() for %s returned schema type %v, want %v", tt.name, schemaType, tt.wantType)
				}

				// Additional check for time.Time fields
				if tt.name == "TimeField" {
					logicalType := schema.(*avro.PrimitiveSchema).Logical()
					if logicalType == nil || logicalType.Type() != "timestamp-millis" {
						t.Errorf("Expected time.Time field to have logical type timestamp-millis, got %v", logicalType)
					}
				}
			}
		})
	}

	// Test unsupported types
	type unsupportedStruct struct {
		ChanField           chan int       `avro:"chan_field"`
		MapWithNonStringKey map[int]string `avro:"invalid_map"`
	}
	unsupportedType := reflect.TypeOf(unsupportedStruct{})

	// Test channel field
	chanField, _ := unsupportedType.FieldByName("ChanField")
	_, err := fieldToSchema(chanField)
	if err == nil {
		t.Errorf("fieldToSchema() for channel field should return error")
	}

	// Test map with non-string key
	invalidMapField, _ := unsupportedType.FieldByName("MapWithNonStringKey")
	_, err = fieldToSchema(invalidMapField)
	if err == nil {
		t.Errorf("fieldToSchema() for map with non-string key should return error")
	}
}

func TestStructToSchema(t *testing.T) {
	type SimpleStruct struct {
		Name string `avro:"name"`
		Age  int    `avro:"age"`
	}

	type StructWithoutTags struct {
		Name string
		Age  int
	}

	type StructWithNestedStruct struct {
		Name   string       `avro:"name"`
		Nested SimpleStruct `avro:"nested"`
	}

	type StructWithSlice struct {
		Name string   `avro:"name"`
		Tags []string `avro:"tags"`
	}

	tests := []struct {
		name       string
		schemaName string
		input      any
		wantFields int
		wantErr    bool
	}{
		{"SimpleStruct", "test", SimpleStruct{}, 2, false},
		{"SimpleStructPointer", "test", &SimpleStruct{}, 2, false},
		{"StructWithoutTags", "test", StructWithoutTags{}, 0, false},
		{"StructWithNestedStruct", "test", StructWithNestedStruct{}, 2, false},
		{"StructWithSlice", "test", StructWithSlice{}, 2, false},
		{"NonStruct", "test", "not a struct", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, err := structToSchema(tt.schemaName, tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("structToSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if schema.Type() != "record" {
					t.Errorf("structToSchema() returned schema type %v, want record", schema.Type())
				}

				recordSchema := schema.(*avro.RecordSchema)
				if len(recordSchema.Fields()) != tt.wantFields {
					t.Errorf("structToSchema() returned schema with %d fields, want %d", len(recordSchema.Fields()), tt.wantFields)
				}

				if recordSchema.Name() != tt.schemaName {
					t.Errorf("structToSchema() returned schema with name %s, want %s", recordSchema.Name(), tt.schemaName)
				}
			}
		})
	}

	// Test error case with invalid field
	type StructWithInvalidField struct {
		Name       string      `avro:"name"`
		InvalidMap map[int]int `avro:"invalid_map"`
	}

	_, err := structToSchema("test", StructWithInvalidField{})
	if err == nil {
		t.Errorf("structToSchema() with invalid field should return error")
	}
}

func TestEncodeDecodeUsingSchema(t *testing.T) {
	type ComplexMetric struct {
		Name         string            `avro:"name"`
		IntField     int               `avro:"int_field"`
		Int8Field    int8              `avro:"int8_field"`
		Int16Field   int16             `avro:"int16_field"`
		Int32Field   int32             `avro:"int32_field"`
		Int64Field   int64             `avro:"int64_field"`
		Uint8Field   uint8             `avro:"uint8_field"`
		Uint16Field  uint16            `avro:"uint16_field"`
		Uint32Field  uint32            `avro:"uint32_field"`
		Uint64Field  uint64            `avro:"uint64_field"`
		BytesField   []byte            `avro:"bytes_field"`
		Float32Field float32           `avro:"float32_field"`
		Float64Field float64           `avro:"float_field"`
		BoolField    bool              `avro:"bool_field"`
		TimeField    time.Time         `avro:"time_field"`
		StringSlice  []string          `avro:"string_slice"`
		IntSlice     []int             `avro:"int_slice"`
		Int8Slice    []int8            `avro:"int8_slice"`
		Int16Slice   []int16           `avro:"int16_slice"`
		Int32Slice   []int32           `avro:"int32_slice"`
		Int64Slice   []int64           `avro:"int64_slice"`
		Uint8Slice   []uint8           `avro:"uint8_slice"`
		Uint16Slice  []uint16          `avro:"uint16_slice"`
		Uint32Slice  []uint32          `avro:"uint32_slice"`
		Float32Slice []float32         `avro:"float32_slice"`
		Float64Slice []float64         `avro:"float64_slice"`
		StringMap    map[string]string `avro:"string_map"`
	}

	testData := ComplexMetric{
		Name:         "Test",
		IntField:     42,
		Int8Field:    math.MaxInt8,
		Int16Field:   math.MaxInt16,
		Int32Field:   math.MaxInt32,
		Int64Field:   math.MaxInt64,
		Uint8Field:   math.MaxUint8,
		Uint16Field:  math.MaxUint16,
		Uint32Field:  math.MaxUint32,
		Uint64Field:  math.MaxUint64,
		BytesField:   []byte("test"),
		Float32Field: math.MaxFloat32,
		Float64Field: math.MaxFloat64,
		BoolField:    true,
		TimeField:    time.Now(),
		StringSlice:  []string{"a", "b", "c"},
		IntSlice:     []int{1, 2, 3, math.MaxInt},
		Int8Slice:    []int8{1, 2, 3, math.MaxInt8},
		Int16Slice:   []int16{1, 2, 3, math.MaxInt16},
		Int32Slice:   []int32{1, 2, 3, math.MaxInt32},
		Int64Slice:   []int64{1, 2, 3, math.MaxInt64},
		Uint8Slice:   []uint8{1, 2, 3, math.MaxUint8},
		Uint16Slice:  []uint16{1, 2, 3, math.MaxUint16},
		Uint32Slice:  []uint32{1, 2, 3, math.MaxUint32},
		Float32Slice: []float32{1.1, 2.2, 3.3, float32(math.MaxFloat32)},
		Float64Slice: []float64{1.1, 2.2, 3.3, math.MaxFloat64},
		StringMap:    map[string]string{"key1": "value1", "key2": "value2"},
	}
	schema, err := structToSchema("test", testData)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	var b bytes.Buffer
	encoder := avro.NewEncoderForSchema(schema, &b)
	err = encoder.Encode(testData)
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	decoder := avro.NewDecoderForSchema(schema, bytes.NewReader(b.Bytes()))
	var decodedData ComplexMetric
	err = decoder.Decode(&decodedData)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	require.Equal(t, testData.Name, decodedData.Name, "Name field does not match")
	require.Equal(t, testData.IntField, decodedData.IntField, "IntField does not match")
	require.Equal(t, testData.Int8Field, decodedData.Int8Field, "Int8Field does not match")
	require.Equal(t, testData.Int16Field, decodedData.Int16Field, "Int16Field does not match")
	require.Equal(t, testData.Int32Field, decodedData.Int32Field, "Int32Field does not match")
	require.Equal(t, testData.Int64Field, decodedData.Int64Field, "Int64Field does not match")
	require.Equal(t, testData.Uint8Field, decodedData.Uint8Field, "Uint8Field does not match")
	require.Equal(t, testData.Uint16Field, decodedData.Uint16Field, "Uint16Field does not match")
	require.Equal(t, testData.Uint32Field, decodedData.Uint32Field, "Uint32Field does not match")
	require.Equal(t, testData.Uint64Field, decodedData.Uint64Field, "Uint64Field does not match")
	require.Equal(t, testData.BytesField, decodedData.BytesField, "BytesField does not match")
	require.Equal(t, testData.Float32Field, decodedData.Float32Field, "Float32Field does not match")
	require.Equal(t, testData.Float64Field, decodedData.Float64Field, "Float64Field does not match")
	require.Equal(t, testData.BoolField, decodedData.BoolField, "BoolField does not match")
	require.Equal(t, testData.TimeField.Unix(), decodedData.TimeField.Unix(), "TimeField does not match")
	require.Equal(t, testData.StringSlice, decodedData.StringSlice, "StringSlice does not match")
	require.Equal(t, testData.IntSlice, decodedData.IntSlice, "IntSlice does not match")
	require.Equal(t, testData.Int8Slice, decodedData.Int8Slice, "Int8Slice does not match")
	require.Equal(t, testData.Int16Slice, decodedData.Int16Slice, "Int16Slice does not match")
	require.Equal(t, testData.Int32Slice, decodedData.Int32Slice, "Int32Slice does not match")
	require.Equal(t, testData.Int64Slice, decodedData.Int64Slice, "Int64Slice does not match")
	require.Equal(t, testData.Uint8Slice, decodedData.Uint8Slice, "Uint8Slice does not match")
	require.Equal(t, testData.Uint16Slice, decodedData.Uint16Slice, "Uint16Slice does not match")
	require.Equal(t, testData.Uint32Slice, decodedData.Uint32Slice, "Uint32Slice does not match")
	require.Equal(t, testData.Float32Slice, decodedData.Float32Slice, "Float32Slice does not match")
	require.Equal(t, testData.Float64Slice, decodedData.Float64Slice, "Float64Slice does not match")
	require.Equal(t, testData.StringMap, decodedData.StringMap, "StringMap does not match")
}
