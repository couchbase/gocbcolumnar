package cbcolumnar

import (
	"context"
)

// ExecuteQuery executes the query statement on the server.
func (c *Cluster) ExecuteQuery(ctx context.Context, statement string, opts ...*QueryOptions) (*QueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	queryOpts := mergeQueryOptions(opts...)

	return c.client.QueryClient().Query(ctx, statement, queryOpts)
}

func mergeQueryOptions(opts ...*QueryOptions) *QueryOptions {
	queryOpts := &QueryOptions{
		Priority:             nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ServerQueryTimeout:   nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		Unmarshaler:          nil,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		if opt.ScanConsistency != nil {
			queryOpts.ScanConsistency = opt.ScanConsistency
		}

		if opt.ReadOnly != nil {
			queryOpts.ReadOnly = opt.ReadOnly
		}

		if opt.Priority != nil {
			queryOpts.Priority = opt.Priority
		}

		if len(opt.PositionalParameters) > 0 {
			queryOpts.PositionalParameters = opt.PositionalParameters
		}

		if len(opt.NamedParameters) > 0 {
			queryOpts.NamedParameters = opt.NamedParameters
		}

		if len(opt.Raw) > 0 {
			queryOpts.Raw = opt.Raw
		}

		if opt.Unmarshaler != nil {
			queryOpts.Unmarshaler = opt.Unmarshaler
		}

		if opt.ServerQueryTimeout != nil {
			queryOpts.ServerQueryTimeout = opt.ServerQueryTimeout
		}
	}

	return queryOpts
}
