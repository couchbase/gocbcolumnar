package cbcolumnar

import "context"

type queryClient interface {
	Query(ctx context.Context, statement string, opts *QueryOptions) (*QueryResult, error)
}
