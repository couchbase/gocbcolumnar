package cbcolumnar_test

import (
	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidCipherSuites(t *testing.T) {
	_, err := cbcolumnar.Connect("couchbases://localhost", cbcolumnar.NewCredential("username", "password"), cbcolumnar.ConnectOptions{
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
	})

	assert.ErrorIs(t, err, cbcolumnar.ErrInvalidArgument)
}
