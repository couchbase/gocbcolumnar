package cbcolumnar

type databaseClient interface {
	Name() string
	Scope(name string) scopeClient
}
