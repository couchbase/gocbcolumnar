package main

import (
	"context"
	"fmt"
	"time"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/serialization"

	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/fiterror"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/options"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/metadata"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/query"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/result"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/queryresult"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/unmarshal"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (c *ColumnarPerformer) ExecuteQuery(_ctx context.Context, req *query.ExecuteQueryRequest) (*query.ExecuteQueryResponse, error) {
	var ctx context.Context

	var cancel context.CancelFunc

	var opts *cbcolumnar.QueryOptions

	if req.Options != nil {
		if req.Options.Timeout == nil {
			ctx, cancel = context.WithCancel(context.Background())
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), req.Options.Timeout.AsDuration())
		}

		var err error

		opts, err = options.ConvertProtoOptionsToSDK(req.Options)
		if err != nil {
			cancel()

			return nil, err
		}
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}

	var queryable queryresult.Queryable

	switch l := req.Level.(type) {
	case *query.ExecuteQueryRequest_ClusterLevel:
		cluster, ok := c.conns.Get(l.ClusterLevel.ClusterId)
		if !ok {
			cancel()

			return nil, fmt.Errorf("cluster not found: %s", l.ClusterLevel.ClusterId)
		}

		queryable = cluster
	case *query.ExecuteQueryRequest_ScopeLevel:
		cluster, ok := c.conns.Get(l.ScopeLevel.ClusterId)
		if !ok {
			cancel()

			return nil, fmt.Errorf("cluster not found: %s", l.ScopeLevel.ClusterId)
		}

		queryable = cluster.Database(l.ScopeLevel.DatabaseName).Scope(l.ScopeLevel.ScopeName)
	}

	queryHandle := uuid.NewString()
	queryResult := queryresult.NewQueryResult(cancel, queryable)
	c.queries.Set(queryHandle, queryResult)

	start := time.Now()

	go queryResult.ExecuteQuery(ctx, req.Statement, opts)

	return &query.ExecuteQueryResponse{
		QueryHandle: queryHandle,
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) QueryResult(_ctx context.Context, req *query.QueryResultRequest) (*result.EmptyResultOrFailureResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	err := res.AwaitResult()
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) QueryRow(ctx context.Context, req *query.QueryRowRequest) (*query.QueryRowResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	err := res.AwaitResult()
	if err != nil {
		return &query.QueryRowResponse{
			Result: &query.QueryRowResponse_ExecuteQueryFailure{
				ExecuteQueryFailure: fiterror.ParseSDKError(err),
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	row := res.NextRow()

	if row == nil {
		if err := res.Err(); err != nil {
			return &query.QueryRowResponse{
				Result: &query.QueryRowResponse_RowLevelFailure{
					RowLevelFailure: fiterror.ParseSDKError(err),
				},
				Metadata: &metadata.ResponseMetadata{
					ElapsedNanos: time.Since(start).Nanoseconds(),
					Initiated:    timestamppb.New(start),
				},
			}, nil
		}

		return &query.QueryRowResponse{
			Result: &query.QueryRowResponse_Success{
				Success: &query.QueryRowResponse_Result{
					Row:         nil,
					EndOfStream: true,
				},
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	var contentWas *serialization.ContentWas
	if req.ContentAs != nil {
		contentWas, err = unmarshal.ParseContentAs(req.ContentAs, row)
		if err != nil {
			return &query.QueryRowResponse{
				Result: &query.QueryRowResponse_RowLevelFailure{
					RowLevelFailure: fiterror.ParseSDKError(err),
				},
				Metadata: &metadata.ResponseMetadata{
					ElapsedNanos: time.Since(start).Nanoseconds(),
					Initiated:    timestamppb.New(start),
				},
			}, nil
		}
	}

	return &query.QueryRowResponse{
		Result: &query.QueryRowResponse_Success{
			Success: &query.QueryRowResponse_Result{
				Row: &query.QueryRowResponse_Row{
					RowContent: contentWas,
				},
				EndOfStream: false,
			},
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) QueryCancel(ctx context.Context, req *query.QueryCancelRequest) (*result.EmptyResultOrFailureResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	res.Cancel()

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) QueryMetadata(ctx context.Context, req *query.QueryMetadataRequest) (*query.QueryResultMetadataResponse, error) {
	res, ok := c.queries.Get(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	err := res.AwaitResult()
	if err != nil {
		return &query.QueryResultMetadataResponse{
			Result: &query.QueryResultMetadataResponse_Failure{
				Failure: fiterror.ParseSDKError(err),
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	meta, err := res.Metadata()
	if err != nil {
		return &query.QueryResultMetadataResponse{
			Result: &query.QueryResultMetadataResponse_Failure{
				Failure: fiterror.ParseSDKError(err),
			},
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &query.QueryResultMetadataResponse{
		Result: &query.QueryResultMetadataResponse_Success{
			Success: ParseMetadata(meta),
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) CloseQueryResult(ctx context.Context, req *query.CloseQueryResultRequest) (*result.EmptyResultOrFailureResponse, error) {
	res, ok := c.queries.Delete(req.QueryHandle)
	if !ok {
		return nil, fmt.Errorf("query handle not found: %s", req.QueryHandle)
	}

	start := time.Now()

	res.Cancel()

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) CloseAllQueryResults(ctx context.Context, req *query.CloseAllQueryResultsRequest) (*result.EmptyResultOrFailureResponse, error) {
	start := time.Now()

	handles := c.queries.Clear()
	for _, handle := range handles {
		handle.Cancel()
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}
