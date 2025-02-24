package cbcolumnar

import "time"

// QueryScanConsistency indicates the level of data consistency desired for an analytics query.
type QueryScanConsistency uint

const (
	// QueryScanConsistencyNotBounded indicates no data consistency is required.
	QueryScanConsistencyNotBounded QueryScanConsistency = iota + 1
	// QueryScanConsistencyRequestPlus indicates that request-level data consistency is required.
	QueryScanConsistencyRequestPlus
)

// QueryOptions is the set of options available to an Analytics query.
type QueryOptions struct {
	// Priority sets whether this query should be assigned as high priority by the analytics engine.
	Priority             *bool
	PositionalParameters []interface{}
	NamedParameters      map[string]interface{}
	ReadOnly             *bool
	ScanConsistency      *QueryScanConsistency

	ServerQueryTimeout *time.Duration

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	// Unmarshaler specifies the default unmarshaler to use for decoding rows from this query.
	Unmarshaler Unmarshaler
}

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		Priority:             nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ServerQueryTimeout:   nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		Unmarshaler:          nil,
	}
}

func (opts *QueryOptions) SetPriority(priority bool) *QueryOptions {
	opts.Priority = &priority

	return opts
}

func (opts *QueryOptions) SetPositionalParameters(params []interface{}) *QueryOptions {
	opts.PositionalParameters = params

	return opts
}

func (opts *QueryOptions) SetNamedParameters(params map[string]interface{}) *QueryOptions {
	opts.NamedParameters = params

	return opts
}

func (opts *QueryOptions) SetReadOnly(readOnly bool) *QueryOptions {
	opts.ReadOnly = &readOnly

	return opts
}

func (opts *QueryOptions) SetScanConsistency(scanConsistency QueryScanConsistency) *QueryOptions {
	opts.ScanConsistency = &scanConsistency

	return opts
}

func (opts *QueryOptions) SetServerQueryTimeout(timeout time.Duration) *QueryOptions {
	opts.ServerQueryTimeout = &timeout

	return opts
}

func (opts *QueryOptions) SetRaw(raw map[string]interface{}) *QueryOptions {
	opts.Raw = raw

	return opts
}
