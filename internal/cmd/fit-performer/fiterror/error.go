package fiterror

import (
	"context"
	"errors"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	protoErrors "github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/errors"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/result"
)

func ParseSDKError(err error) *protoErrors.Error {
	var queryErr *cbcolumnar.QueryError
	if errors.As(err, &queryErr) {
		var subException *protoErrors.SubColumnarError
		if errors.Is(err, cbcolumnar.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_TimeoutException{
					TimeoutException: &protoErrors.TimeoutException{},
				},
			}
		} else {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_QueryException{
					QueryException: &protoErrors.QueryException{
						ErrorCode:     int32(queryErr.Code()),
						ServerMessage: queryErr.Message(),
					},
				},
			}
		}

		return &protoErrors.Error{
			Error: &protoErrors.Error_Columnar{
				Columnar: &protoErrors.ColumnarError{
					Cause:        nil, // TODO: is this right?
					SubException: subException,
					AsString:     err.Error(),
				},
			},
		}
	}

	var columnarErr *cbcolumnar.ColumnarError
	if errors.As(err, &columnarErr) {
		var subException *protoErrors.SubColumnarError
		if errors.Is(err, cbcolumnar.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_TimeoutException{
					TimeoutException: &protoErrors.TimeoutException{},
				},
			}
		} else if errors.Is(err, cbcolumnar.ErrInvalidCredential) {
			subException = &protoErrors.SubColumnarError{
				SubError: &protoErrors.SubColumnarError_InvalidCredentialException{
					InvalidCredentialException: &protoErrors.InvalidCredentialException{},
				},
			}
		}

		return &protoErrors.Error{
			Error: &protoErrors.Error_Columnar{
				Columnar: &protoErrors.ColumnarError{
					Cause:        nil, // TODO: is this right?
					SubException: subException,
					AsString:     err.Error(),
				},
			},
		}
	}

	return &protoErrors.Error{
		Error: &protoErrors.Error_Platform{
			Platform: &protoErrors.PlatformError{
				Type:     protoErrors.PlatformErrorType_PLATFORM_ERROR_OTHER, // TODO: expose other error types
				AsString: err.Error(),
			},
		},
	}
}

func ConvertSDKErrorToFailureResponse(err error) *result.EmptyResultOrFailureResponse_Error {
	return &result.EmptyResultOrFailureResponse_Error{
		Error: ParseSDKError(err),
	}
}
