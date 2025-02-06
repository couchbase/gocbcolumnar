package cbcolumnar

import (
	"encoding/json"
	"errors"
	"fmt"
)

var ErrColumnar = errors.New("columnar error")

var ErrInvalidCredential = errors.New("invalid credential")

var ErrTimeout = errors.New("timeout error")

var ErrQuery = errors.New("query error")

var ErrInvalidArgument = errors.New("invalid argument")

// ErrNoResult occurs when no results are available to a query.
var ErrNoResult = errors.New("no result was available")

// ErrIllegalState occurs when an entity was used in an incorrect manner.
var ErrIllegalState = errors.New("illegal state")

// ErrClosed occurs when an entity was used after it was closed.
var ErrClosed = errors.New("closed")

// ErrUnmarshal occurs when an entity could not be unmarshalled.
var ErrUnmarshal = errors.New("unmarshalling error")

// ColumnarError occurs when an error is encountered while interacting with the Columnar service.
type ColumnarError struct {
	Cause   error
	context map[string]interface{}
}

// nolint: unused
func newColumnarError(context map[string]interface{}) ColumnarError {
	return ColumnarError{
		Cause:   nil,
		context: context,
	}
}

// nolint: unused
func newColumnarErrorWithCause(context map[string]interface{}, cause error) ColumnarError {
	return ColumnarError{
		Cause:   cause,
		context: context,
	}
}

// Error returns the string representation of a Columnar error.
func (e ColumnarError) Error() string {
	errBytes, _ := json.Marshal(struct {
		Context map[string]interface{} `json:"context,omitempty"`
	}{
		Context: e.context,
	})
	// if serErr != nil {
	// 	logErrorf("failed to serialize error to json: %s", serErr.Error())
	// }

	cause := e.Cause
	if cause == nil {
		cause = ErrColumnar
	}

	return cause.Error() + " | " + string(errBytes)
}

// Unwrap returns the underlying reason for the error.
func (e ColumnarError) Unwrap() error {
	if e.Cause == nil {
		return ErrColumnar
	} else {
		return e.Cause
	}
}

// QueryError occurs when an error is returned in the errors field of the response body of a response
// from the query server.
type QueryError struct {
	Cause   ColumnarError
	Code    int
	Message string
}

// Error returns the string representation of a query error.
func (e QueryError) Error() string {
	return fmt.Errorf("%w - message: %s code: %d", e.Cause, e.Message, e.Code).Error()
}

// Unwrap returns the underlying reason for the error.
func (e QueryError) Unwrap() error {
	return e.Cause
}

// nolint: unused
func newQueryError(context map[string]interface{}, code int, message string) QueryError {
	return QueryError{
		Cause: ColumnarError{
			Cause:   ErrQuery,
			context: context,
		},
		Code:    code,
		Message: message,
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

type unmarshalError struct {
	Reason string
}

func (e unmarshalError) Error() string {
	return fmt.Sprintf("failed to unmarshal - %s", e.Reason)
}

func (e unmarshalError) Unwrap() error {
	return ErrUnmarshal
}
