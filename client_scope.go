package cbcolumnar

type scopeClient interface {
	Name() string
	QueryClient() queryClient
}
