package cbcolumnar

import "context"

func (s *Scope) ExecuteQuery(ctx context.Context, statement string, opts ...*QueryOptions) (*QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	queryOpts := mergeQueryOptions(opts...)

	return s.client.QueryClient().Query(ctx, statement, queryOpts)
}
