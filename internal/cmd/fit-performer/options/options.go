package options

import (
	"errors"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/query"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/unmarshal"
)

func ConvertProtoOptionsToSDK(opts *query.ExecuteQueryRequest_Options) (*cbcolumnar.QueryOptions, error) {
	sdkOpts := cbcolumnar.NewQueryOptions()

	// Priority is deprecated in the protocol as it is not supported by the Analytics SDKs, but it is supported by Columnar.
	if opts.Priority != nil { //nolint:staticcheck
		sdkOpts = sdkOpts.SetPriority(*opts.Priority) //nolint:staticcheck
	}

	if opts.ParametersPositional != nil {
		params := make([]interface{}, len(opts.ParametersPositional.Values))
		for i, param := range opts.ParametersPositional.Values {
			params[i] = param
		}

		sdkOpts = sdkOpts.SetPositionalParameters(params)
	}

	if opts.ParametersNamed != nil {
		params := make(map[string]interface{})
		for key, param := range opts.ParametersNamed.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetNamedParameters(params)
	}

	if opts.Readonly != nil {
		sdkOpts = sdkOpts.SetReadOnly(*opts.Readonly)
	}

	if opts.ScanConsistency != nil {
		switch *opts.ScanConsistency {
		case query.ExecuteQueryRequest_Options_SCAN_CONSISTENCY_NOT_BOUNDED:
			sdkOpts = sdkOpts.SetScanConsistency(cbcolumnar.QueryScanConsistencyNotBounded)
		case query.ExecuteQueryRequest_Options_SCAN_CONSISTENCY_REQUEST_PLUS:
			sdkOpts = sdkOpts.SetScanConsistency(cbcolumnar.QueryScanConsistencyRequestPlus)
		default:
			return nil, errors.New("unsupported scan consistency")
		}
	}

	if opts.Raw != nil {
		params := make(map[string]interface{})
		for key, param := range opts.Raw.Fields {
			params[key] = param
		}

		sdkOpts = sdkOpts.SetRaw(params)
	}

	if opts.Deserializer != nil {
		u, err := unmarshal.NewUnmarshaler(opts.Deserializer)
		if err != nil {
			return nil, err
		}

		sdkOpts = sdkOpts.SetUnmarshaler(u)
	}

	return sdkOpts, nil
}
