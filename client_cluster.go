package cbcolumnar

import (
	"crypto/tls"

	"github.com/couchbaselabs/gocbconnstr"
)

type clusterClient interface {
	QueryClient() queryClient
	Database(name string) databaseClient

	Close() error
}

type clusterClientOptions struct {
	Spec                                 gocbconnstr.ConnSpec
	Credential                           *Credential
	TimeoutsConfig                       *TimeoutOptions
	TrustOnly                            TrustOnly
	DisableServerCertificateVerification *bool
	CipherSuites                         []tls.CipherSuite
	ForceDisableSrv                      bool
}

func newClusterClient(opts clusterClientOptions) (clusterClient, error) {
	return nil, nil //nolint:nilnil
}
