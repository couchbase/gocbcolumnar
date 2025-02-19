package cbcolumnar

import "encoding/json"

type Unmarshaler interface {
	Unmarshal([]byte, interface{}) error
}

type JSONUnmarshaler struct{}

func NewJSONUnmarshaler() *JSONUnmarshaler {
	return &JSONUnmarshaler{}
}

func (ju *JSONUnmarshaler) Unmarshal(data []byte, v interface{}) error {
	if out, ok := v.(*[]byte); ok {
		*out = data

		return nil
	}

	err := json.Unmarshal(data, v)
	if err != nil {
		return unmarshalError{
			Reason: err.Error(),
		}
	}

	return nil
}
