package cbcolumnar

type Scope struct {
	client scopeClient
}

func (d *Database) Scope(name string) *Scope {
	return &Scope{
		d.client.Scope(name),
	}
}

func (s *Scope) Name() string {
	return s.client.Name()
}
