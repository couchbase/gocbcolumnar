package cbcolumnar

import "encoding/json"

type Unmarshaler interface {
	Unmarshal([]byte, interface{}) error
}

type JSONUnmarshaler struct{}

func (ju JSONUnmarshaler) Unmarshal(data []byte, v interface{}) error {
	// TODO: Don't return the json package error
	return json.Unmarshal(data, v)
}
