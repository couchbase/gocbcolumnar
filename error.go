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

// ErrClosed occurs when an entity was used after it was closed.
var ErrClosed = errors.New("closed")

// ErrUnmarshal occurs when an entity could not be unmarshalled.
var ErrUnmarshal = errors.New("unmarshalling error")

type columnarErrorDesc struct {
	Code    uint32
	Message string
}

func (e columnarErrorDesc) MarshalJSON() ([]byte, error) {
	b, err := json.Marshal(struct {
		Code    uint32 `json:"code"`
		Message string `json:"msg"`
	}{
		Code:    e.Code,
		Message: e.Message,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal columnar error description: %s", err) // nolint: err113, errorlint
	}

	return b, nil
}

// ColumnarError occurs when an error is encountered while interacting with the Columnar service.
type ColumnarError struct {
	Cause error

	errors           []columnarErrorDesc
	statement        string
	endpoint         string
	errorText        string
	httpResponseCode int
}

// nolint: unused
func newColumnarError(statement, endpoint string, statusCode int) ColumnarError {
	return ColumnarError{
		Cause:            nil,
		errors:           nil,
		statement:        statement,
		endpoint:         endpoint,
		errorText:        "",
		httpResponseCode: statusCode,
	}
}

func (e ColumnarError) withErrorText(errText string) *ColumnarError {
	e.errorText = errText

	return &e
}

func (e ColumnarError) withErrors(errors []columnarErrorDesc) *ColumnarError {
	e.errors = errors

	return &e
}

func (e ColumnarError) withCause(cause error) *ColumnarError {
	e.Cause = cause

	return &e
}

// Error returns the string representation of a Columnar error.
func (e ColumnarError) Error() string {
	errBytes, _ := json.Marshal(struct {
		Statement        string              `json:"statement,omitempty"`
		Errors           []columnarErrorDesc `json:"errors,omitempty"`
		ErrorText        string              `json:"errorText,omitempty"`
		Endpoint         string              `json:"endpoint,omitempty"`
		HTTPResponseCode int                 `json:"status_code,omitempty"`
	}{
		Statement:        e.statement,
		Errors:           e.errors,
		ErrorText:        e.errorText,
		Endpoint:         e.endpoint,
		HTTPResponseCode: e.httpResponseCode,
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
	Cause   *ColumnarError
	Code    int
	Message string
}

// Error returns the string representation of a query error.
func (e QueryError) Error() string {
	return fmt.Errorf("%w", e.Cause).Error()
}

// Unwrap returns the underlying reason for the error.
func (e QueryError) Unwrap() error {
	return e.Cause
}

func (e QueryError) withErrors(errors []columnarErrorDesc) *QueryError {
	e.Cause.errors = errors

	return &e
}

// nolint: unused
func newQueryError(statement, endpoint string, statusCode int, code int, message string) QueryError {
	return QueryError{
		Cause: &ColumnarError{
			Cause:            ErrQuery,
			errors:           nil,
			statement:        statement,
			endpoint:         endpoint,
			errorText:        "",
			httpResponseCode: statusCode,
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
