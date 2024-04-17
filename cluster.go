package cbcolumnar

import (
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/gocbconnstr"
)

type Cluster struct {
	client clusterClient

	timeoutsConfig TimeoutOptions
}

func Connect(connStr string, credential Credential, opts ConnectOptions) (*Cluster, error) {
	connSpec, err := gocbconnstr.Parse(connStr)
	if err != nil {
		return nil, err
	}

	if connSpec.Scheme != "couchbases" {
		return nil, invalidArgumentError{
			ArgumentName: "scheme",
			Reason:       "only couchbases scheme is supported",
		}
	}

	connectTimeout := 10000 * time.Millisecond
	dispatchTimeout := 30000 * time.Millisecond
	serverQueryTimeout := 10 * time.Minute
	useSrv := true

	if opts.TimeoutOptions.ConnectTimeout != nil {
		connectTimeout = *opts.TimeoutOptions.ConnectTimeout
	}

	if opts.TimeoutOptions.DispatchTimeout != nil {
		dispatchTimeout = *opts.TimeoutOptions.DispatchTimeout
	}

	if opts.TimeoutOptions.ServerQueryTimeout != nil {
		serverQueryTimeout = *opts.TimeoutOptions.ServerQueryTimeout
	}

	fetchOption := func(name string) (string, bool) {
		optValue := connSpec.Options[name]
		if len(optValue) == 0 {
			return "", false
		}

		return optValue[len(optValue)-1], true
	}

	if valStr, ok := fetchOption("srv"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "srv",
				Reason:       err.Error(),
			}
		}

		useSrv = val
	}

	if valStr, ok := fetchOption("timeout.connect_timeout"); ok {
		duration, err := time.ParseDuration(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "timeout.connect_timeout",
				Reason:       err.Error(),
			}
		}

		connectTimeout = duration
	}

	if valStr, ok := fetchOption("timeout.dispatch_timeout"); ok {
		duration, err := time.ParseDuration(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "timeout.dispatch_timeout",
				Reason:       err.Error(),
			}
		}

		dispatchTimeout = duration
	}

	if valStr, ok := fetchOption("timeout.server_query_timeout"); ok {
		duration, err := time.ParseDuration(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "timeout.server_query_timeout",
				Reason:       err.Error(),
			}
		}

		serverQueryTimeout = duration
	}

	if valStr, ok := fetchOption("security.trust_only_pem_file"); ok {
		opts.SecurityOptions.TrustOnly = TrustOnlyPemFile{
			Path: valStr,
		}
	}

	if valStr, ok := fetchOption("security.disable_server_certificate_verification"); ok {
		val, err := strconv.ParseBool(valStr)
		if err != nil {
			return nil, invalidArgumentError{
				ArgumentName: "disable_server_certificate_verification",
				Reason:       err.Error(),
			}
		}

		opts.SecurityOptions.DisableServerCertificateVerification = &val
	}

	if valStr, ok := fetchOption("security.cipher_suites"); ok {
		split := strings.Split(valStr, ",")
		suites := make([]uint16, len(split))

		for i, strSuite := range split {
			suite, err := strconv.ParseUint(strSuite, 0, 16)
			if err != nil {
				return nil, invalidArgumentError{
					ArgumentName: "security.cipher_suites",
					Reason:       err.Error(),
				}
			}

			suites[i] = uint16(suite)
		}

		opts.SecurityOptions.CipherSuites = suites
	}

	if connectTimeout == 0 {
		return nil, invalidArgumentError{
			ArgumentName: "ConnectTimeout",
			Reason:       "must be greater than 0",
		}
	}

	if dispatchTimeout == 0 {
		return nil, invalidArgumentError{
			ArgumentName: "DispatchTimeout",
			Reason:       "must be greater than 0",
		}
	}

	if serverQueryTimeout == 0 {
		return nil, invalidArgumentError{
			ArgumentName: "ServerQueryTimeout",
			Reason:       "must be greater than 0",
		}
	}

	opts.TimeoutOptions.ConnectTimeout = &connectTimeout
	opts.TimeoutOptions.DispatchTimeout = &dispatchTimeout
	opts.TimeoutOptions.ServerQueryTimeout = &serverQueryTimeout

	mgr, err := newClusterClient(clusterClientOptions{
		Spec:            connSpec,
		Credential:      &credential,
		TimeoutsConfig:  &opts.TimeoutOptions,
		SecurityConfig:  &opts.SecurityOptions,
		ForceDisableSrv: !useSrv,
	})
	if err != nil {
		return nil, err
	}

	c := &Cluster{
		client:         mgr,
		timeoutsConfig: opts.TimeoutOptions,
	}

	return c, nil
}
