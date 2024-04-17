package cbcolumnar

import (
	"encoding/json"
	"errors"
	"fmt"
)

type Error struct {
	Cause   error                  `json:"-"`
	Context map[string]interface{} `json:"context,omitempty"`
}

// MarshalJSON implements the Marshaler interface.
func (e Error) MarshalJSON() ([]byte, error) {
	var innerError string
	if e.Cause != nil {
		innerError = e.Cause.Error()
	}

	return json.Marshal(struct {
		InnerError string                 `json:"cause,omitempty"`
		Context    map[string]interface{} `json:"context,omitempty"`
	}{
		InnerError: innerError,
		Context:    e.Context,
	})
}

// Error returns the string representation of a kv error.
func (e Error) Error() string {
	errBytes, _ := json.Marshal(struct {
		InnerError error                  `json:"-"`
		Context    map[string]interface{} `json:"context,omitempty"`
	}{
		InnerError: e.Cause,
		Context:    e.Context,
	})
	// if serErr != nil {
	// 	logErrorf("failed to serialize error to json: %s", serErr.Error())
	// }

	return e.Cause.Error() + " | " + string(errBytes)
}

// Unwrap returns the underlying reason for the error.
func (e Error) Unwrap() error {
	return e.Cause
}

func makeError(baseErr error, context map[string]interface{}) *Error {
	return &Error{
		Cause:   baseErr,
		Context: context,
	}
}

type invalidArgumentError struct {
	ArgumentName string
	Reason       string
}

func (e invalidArgumentError) Error() string {
	return fmt.Sprintf("%s %s - %s", e.Unwrap(), e.ArgumentName, e.Reason)
}

func (e invalidArgumentError) Unwrap() error {
	return ErrInvalidArgument
}

var ErrInvalidArgument = errors.New("invalid argument")

// ErrNoResult occurs when no results are available to a query.
var ErrNoResult = errors.New("no result was available")

// ErrIllegalState occurs when an entity was used in an incorrect manner.
var ErrIllegalState = errors.New("illegal state")

// ErrClosed occurs when an entity was used after it was closed.
var ErrClosed = errors.New("closed")
