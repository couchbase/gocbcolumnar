package cbcolumnar

import (
	"time"
)

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

	// Raw provides a way to provide extra parameters in the request body for the query.
	Raw map[string]interface{}

	ServerTimeout *time.Duration

	// Unmarshaler specifies the default unmarshaler to use for decoding rows from this query.
	Unmarshaler Unmarshaler
}

func NewQueryOptions() *QueryOptions {
	return &QueryOptions{
		Priority:             nil,
		PositionalParameters: nil,
		NamedParameters:      nil,
		ReadOnly:             nil,
		ScanConsistency:      nil,
		Raw:                  nil,
		ServerTimeout:        nil,
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

func (opts *QueryOptions) SetRaw(raw map[string]interface{}) *QueryOptions {
	opts.Raw = raw

	return opts
}

func (opts *QueryOptions) SetServerTimeout(timeout time.Duration) *QueryOptions {
	opts.ServerTimeout = &timeout

	return opts
}

// This will be required soon.
// func (opts *QueryOptions) toMap() (map[string]interface{}, error) {
// 	execOpts := make(map[string]interface{})
//
// 	execOpts["client_context_id"] = uuid.New().String()
//
// 	if opts.ScanConsistency != nil {
// 		switch *opts.ScanConsistency {
// 		case QueryScanConsistencyNotBounded:
// 			execOpts["scan_consistency"] = "not_bounded"
// 		case QueryScanConsistencyRequestPlus:
// 			execOpts["scan_consistency"] = "request_plus"
// 		default:
// 			return nil, makeInvalidArgumentsError("unexpected consistency option")
// 		}
// 	}
//
// 	if opts.PositionalParameters != nil && opts.NamedParameters != nil {
// 		return nil, makeInvalidArgumentsError("positional and named parameters must be used exclusively")
// 	}
//
// 	if opts.PositionalParameters != nil {
// 		execOpts["args"] = opts.PositionalParameters
// 	}
//
// 	if opts.NamedParameters != nil {
// 		for key, value := range opts.NamedParameters {
// 			if !strings.HasPrefix(key, "$") {
// 				key = "$" + key
// 			}
// 			execOpts[key] = value
// 		}
// 	}
//
// 	if opts.ReadOnly != nil {
// 		execOpts["readonly"] = *opts.ReadOnly
// 	}
//
// 	if opts.Raw != nil {
// 		for k, v := range opts.Raw {
// 			execOpts[k] = v
// 		}
// 	}
//
// 	if opts.ServerTimeout != nil {
// 		execOpts["timeout"] = opts.ServerTimeout.String()
// 	}
//
// 	return execOpts, nil
// }
