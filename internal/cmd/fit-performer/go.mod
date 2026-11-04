module github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer

go 1.25.0

require (
	github.com/couchbase/gocbcolumnar v1.0.0
	github.com/google/uuid v1.6.0
	github.com/sirupsen/logrus v1.10.2
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/couchbase/gocbcore/v10 v10.6.0 // indirect
	github.com/couchbaselabs/gocbconnstr v1.0.5 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
)

replace github.com/couchbase/gocbcolumnar => ../../../
