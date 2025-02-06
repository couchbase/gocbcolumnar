package cbcolumnar_test

import (
	"testing"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/stretchr/testify/assert"
)

func TestInvalidCipherSuites(t *testing.T) {
	_, err := cbcolumnar.Connect("couchbases://localhost", cbcolumnar.NewCredential("username", "password"), cbcolumnar.ClusterOptions{
		TimeoutOptions: cbcolumnar.TimeoutOptions{
			ConnectTimeout:     nil,
			DispatchTimeout:    nil,
			ServerQueryTimeout: nil,
		},
		SecurityOptions: cbcolumnar.SecurityOptions{
			TrustOnly:                            nil,
			DisableServerCertificateVerification: nil,
			CipherSuites:                         []string{"bad"},
		},
		Unmarshaler: nil,
	})

	assert.ErrorIs(t, err, cbcolumnar.ErrInvalidArgument)
}
