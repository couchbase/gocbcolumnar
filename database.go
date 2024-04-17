package cbcolumnar

type Database struct {
	client databaseClient
}

func (c *Cluster) Database(name string) *Database {
	return &Database{
		client: c.client.Database(name),
	}
}

func (d *Database) Name() string {
	return d.client.Name()
}
