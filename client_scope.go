package cbcolumnar

import (
	"time"

	"github.com/couchbase/gocbcore/v10"
)

type scopeClient interface {
	Name() string
	QueryClient() queryClient
}

type gocbcoreScopeClient struct {
	agent                     *gocbcore.ColumnarAgent
	name                      string
	defaultServerQueryTimeout time.Duration
}

func newGocbcoreScopeClient(agent *gocbcore.ColumnarAgent, name string, defaultServerQueryTimeout time.Duration) *gocbcoreScopeClient {
	return &gocbcoreScopeClient{
		agent:                     agent,
		name:                      name,
		defaultServerQueryTimeout: defaultServerQueryTimeout,
	}
}

func (c *gocbcoreScopeClient) Name() string {
	return c.name
}

func (c *gocbcoreScopeClient) QueryClient() queryClient {
	return newGocbcoreQueryClient(c.agent, c.defaultServerQueryTimeout)
}
