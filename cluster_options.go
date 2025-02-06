package cbcolumnar

import (
	"crypto/x509"
	"time"
)

// TrustOnly specifies the trust mode to use within the SDK.
type TrustOnly interface {
	trustOnly()
}

// TrustOnlyCapella  tells the SDK to trust only the Capella CA certificate(s) bundled with the SDK.
// This is the default behavior.
type TrustOnlyCapella struct{}

func (t TrustOnlyCapella) trustOnly() {}

// TrustOnlyPemFile tells the SDK to trust only the PEM-encoded certificate(s) in the file at the given FS path.
type TrustOnlyPemFile struct {
	Path string
}

func (t TrustOnlyPemFile) trustOnly() {}

// TrustOnlyPemString tells the SDK to trust only the PEM-encoded certificate(s) in the given string.
type TrustOnlyPemString struct {
	Pem string
}

func (t TrustOnlyPemString) trustOnly() {}

// TrustOnlyCertificates tells the SDK to trust only the specified certificates.
type TrustOnlyCertificates struct {
	Certificates *x509.CertPool
}

func (t TrustOnlyCertificates) trustOnly() {}

// TrustOnlySystem tells the SDK to trust only the certificates trusted by the system cert pool.
type TrustOnlySystem struct{}

func (t TrustOnlySystem) trustOnly() {}

// SecurityOptions specifies options for controlling security related
// items such as TLS root certificates and verification skipping.
type SecurityOptions struct {
	// TrustOnly specifies the trust mode to use within the SDK.
	TrustOnly TrustOnly

	// DisableServerCertificateVerification when specified causes the SDK to trust ANY certificate
	// regardless of validity.
	DisableServerCertificateVerification *bool

	// CipherSuites specifies the TLS cipher suites the SDK is allowed to use when negotiating TLS
	// settings, or an empty list to use any cipher suite supported by the runtime environment.
	// See: https://go.dev/src/crypto/tls/cipher_suites.go
	CipherSuites []string
}

func (opts *SecurityOptions) SetTrustOnly(trustOnly TrustOnly) *SecurityOptions {
	opts.TrustOnly = trustOnly

	return opts
}

func (opts *SecurityOptions) SetDisableServerCertificateVerification(disabled bool) *SecurityOptions {
	opts.DisableServerCertificateVerification = &disabled

	return opts
}

func (opts *SecurityOptions) SetCipherSuites(cipherSuites []string) *SecurityOptions {
	opts.CipherSuites = cipherSuites

	return opts
}

// TimeoutOptions specifies options for various operation timeouts.
type TimeoutOptions struct {
	// ConnectTimeout specifies the socket connection timeout, or more broadly the timeout
	// for establishing an individual authenticated connection.
	// Default = 10 seconds
	ConnectTimeout *time.Duration

	// DispatchTimeout specifies how long to wait for the SDK to retry a request due to network
	// connectivity issues or unexpected cluster topology changes.
	// Default = 30 seconds
	DispatchTimeout *time.Duration

	// ServerQueryTimeout specifies how long the server will spend executing a query before timing it out.
	// Default = 10 minutes
	ServerQueryTimeout *time.Duration
}

func (opts *TimeoutOptions) SetConnectTimeout(timeout time.Duration) *TimeoutOptions {
	opts.ConnectTimeout = &timeout

	return opts
}

func (opts *TimeoutOptions) SetDispatchTimeout(timeout time.Duration) *TimeoutOptions {
	opts.DispatchTimeout = &timeout

	return opts
}

func (opts *TimeoutOptions) SetServerQueryTimeout(timeout time.Duration) *TimeoutOptions {
	opts.ServerQueryTimeout = &timeout

	return opts
}

type ConnectOptions struct {
	// TimeoutOptions specifies various operation timeouts.
	TimeoutOptions TimeoutOptions

	// SecurityOptions specifies security related configuration options.
	SecurityOptions SecurityOptions
}

func (co *ConnectOptions) SetTimeoutOptions(timeoutOptions TimeoutOptions) *ConnectOptions {
	co.TimeoutOptions = timeoutOptions

	return co
}

func (co *ConnectOptions) SetSecurityOptions(securityOptions SecurityOptions) *ConnectOptions {
	co.SecurityOptions = securityOptions

	return co
}
