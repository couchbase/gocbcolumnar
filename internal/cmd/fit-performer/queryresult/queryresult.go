package queryresult

import (
	"context"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
)

type Queryable interface {
	ExecuteQuery(ctx context.Context, statement string, opts ...*cbcolumnar.QueryOptions) (*cbcolumnar.QueryResult, error)
}

type queryResultOrError struct {
	err    error
	result *cbcolumnar.QueryResult
}

type QueryResult struct {
	queryable Queryable

	awaitResult chan *queryResultOrError

	result *cbcolumnar.QueryResult

	cancel context.CancelFunc
}

func NewQueryResult(cancel context.CancelFunc, queryable Queryable) *QueryResult {
	return &QueryResult{
		queryable:   queryable,
		awaitResult: make(chan *queryResultOrError, 1),
		result:      nil,
		cancel:      cancel,
	}
}

func (q *QueryResult) ExecuteQuery(ctx context.Context, statement string, opts ...*cbcolumnar.QueryOptions) {
	res, err := q.queryable.ExecuteQuery(ctx, statement, opts...)
	q.awaitResult <- &queryResultOrError{
		err:    err,
		result: res,
	}
}

func (q *QueryResult) AwaitResult() error {
	if q.result != nil {
		return nil
	}

	res := <-q.awaitResult
	if res.err != nil {
		return res.err
	}

	q.awaitResult = nil
	q.result = res.result

	return nil
}

func (q *QueryResult) Cancel() {
	q.cancel()
}

func (q *QueryResult) Metadata() (*cbcolumnar.QueryMetadata, error) {
	return q.result.MetaData()
}

func (q *QueryResult) NextRow() *cbcolumnar.QueryResultRow {
	return q.result.NextRow()
}

func (q *QueryResult) Err() error {
	return q.result.Err()
}
