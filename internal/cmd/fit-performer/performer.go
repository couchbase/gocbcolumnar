package main

import (
	"context"
	"fmt"
	"time"

	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/fiterror"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/caps"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/clustermanagement"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/metadata"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/result"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/protocol/shared"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/queryresult"
	"github.com/couchbase/gocbcolumnar/internal/cmd/fit-performer/unmarshal"
	"google.golang.org/protobuf/types/known/timestamppb"

	cbcolumnar "github.com/couchbase/gocbcolumnar"
	"github.com/sirupsen/logrus"
)

type ColumnarPerformer struct {
	conns   *MutexProtectedResources[*cbcolumnar.Cluster]
	queries *MutexProtectedResources[*queryresult.QueryResult]

	logger *logrus.Logger

	protocol.UnimplementedColumnarCrossServiceServer
	protocol.UnimplementedColumnarServiceServer
}

func NewColumnarPerformer(logger *logrus.Logger) *ColumnarPerformer {
	cbcolumnar.SetLogger(cbcolumnar.VerboseStdioLogger())
	cbcolumnar.SetLogRedactionLevel(cbcolumnar.RedactPartial)

	return &ColumnarPerformer{
		conns:   NewMutexProtectedResources[*cbcolumnar.Cluster](),
		queries: NewMutexProtectedResources[*queryresult.QueryResult](),
		logger:  logger,

		UnimplementedColumnarCrossServiceServer: protocol.UnimplementedColumnarCrossServiceServer{},
		UnimplementedColumnarServiceServer:      protocol.UnimplementedColumnarServiceServer{},
	}
}

func (c *ColumnarPerformer) FetchPerformerCaps(ctx context.Context, in *caps.FetchPerformerCapsRequest) (*caps.FetchPerformerCapsResponse, error) {
	return &caps.FetchPerformerCapsResponse{
		Sdk:        caps.SDK_SDK_GO,
		SdkVersion: "0.0.1",
		ClusterNewInstance: map[int32]*caps.PerApiElementClusterNewInstance{
			0: {
				SupportsDispatchTimeout: false,
			},
		},
		ClusterClose: map[int32]*caps.PerApiElementClusterClose{
			0: {},
		},
		ClusterExecuteQuery: map[int32]*caps.PerApiElementExecuteQuery{
			0: {
				ExecuteQueryReturns:             caps.PerApiElementExecuteQuery_EXECUTE_QUERY_RETURNS_QUERY_RESULT,
				RowIteration:                    caps.PerApiElementExecuteQuery_ROW_ITERATION_STREAMING_PUSH_BASED,
				RowDeserialization:              caps.PerApiElementExecuteQuery_ROW_DESERIALIZATION_STATIC_ROW_TYPING_INDIVIDUAL,
				SupportsPassthroughDeserializer: false,
				SupportsCustomDeserializer:      true,
			},
		},
		ScopeExecuteQuery: map[int32]*caps.PerApiElementExecuteQuery{
			0: {
				ExecuteQueryReturns:             caps.PerApiElementExecuteQuery_EXECUTE_QUERY_RETURNS_QUERY_RESULT,
				RowIteration:                    caps.PerApiElementExecuteQuery_ROW_ITERATION_STREAMING_PUSH_BASED,
				RowDeserialization:              caps.PerApiElementExecuteQuery_ROW_DESERIALIZATION_STATIC_ROW_TYPING_INDIVIDUAL,
				SupportsPassthroughDeserializer: false,
				SupportsCustomDeserializer:      true,
			},
		},
		SdkConnectionError: map[int32]*caps.SdkConnectionError{
			0: {
				InvalidCredErrorType: caps.SdkConnectionError_AS_INVALID_CREDENTIAL_EXCEPTION,
				BootstrapErrorType:   caps.SdkConnectionError_AS_COLUMNAR_ERROR,
			},
		},
		AnalyticsProduct:           caps.AnalyticsProduct_COLUMNAR,
		SupportsServerAsyncQueries: false,
		CredentialSupport: &caps.CredentialSupport{
			SupportsJwtCredential:         false,
			SupportsCertificateCredential: false,
			SupportsSetCredential:         false,
		},
	}, nil
}

func (c *ColumnarPerformer) Echo(ctx context.Context, in *shared.EchoRequest) (*shared.EchoResponse, error) {
	c.logger.Logf(logrus.InfoLevel, "================ %s : %s ================ ", in.TestName, in.Message)

	return &shared.EchoResponse{}, nil
}

func (c *ColumnarPerformer) ClusterNewInstance(ctx context.Context, in *clustermanagement.ClusterNewInstanceRequest) (*result.EmptyResultOrFailureResponse, error) {
	var creds cbcolumnar.Credential

	switch c := in.Credential.Type.(type) {
	case *clustermanagement.ClusterNewInstanceRequest_Credential_UsernameAndPassword_:
		creds = cbcolumnar.NewCredential(c.UsernameAndPassword.Username, c.UsernameAndPassword.Password)
	default:
		return nil, fmt.Errorf("unsupported credential type: %T", in.Credential.Type)
	}

	var timeoutOpts *cbcolumnar.TimeoutOptions
	if in.Options.Timeout != nil {
		timeoutOpts = cbcolumnar.NewTimeoutOptions()
		if in.Options.Timeout.ConnectTimeout != nil {
			timeoutOpts = timeoutOpts.SetConnectTimeout(in.Options.Timeout.GetConnectTimeout().AsDuration())
		}

		if in.Options.Timeout.QueryTimeout != nil {
			timeoutOpts = timeoutOpts.SetQueryTimeout(in.Options.Timeout.GetQueryTimeout().AsDuration())
		}
	}

	var securityOpts *cbcolumnar.SecurityOptions

	if in.Options.Security != nil {
		var trustOnly cbcolumnar.TrustOnly

		switch {
		case in.Options.Security.GetTrustOnlyCapella():
			trustOnly = cbcolumnar.TrustOnlyCapella{}
		case in.Options.Security.GetTrustOnlyPlatform():
			trustOnly = cbcolumnar.TrustOnlySystem{}
		case in.Options.Security.TrustOnlyPemString != nil:
			trustOnly = cbcolumnar.TrustOnlyPemString{
				Pem: *in.Options.Security.TrustOnlyPemString,
			}
		}

		securityOpts = &cbcolumnar.SecurityOptions{
			TrustOnly:                            trustOnly,
			DisableServerCertificateVerification: in.Options.Security.DisableServerCertificateVerification,
			CipherSuites:                         in.Options.Security.CipherSuites,
		}
	}

	var unmarshaler cbcolumnar.Unmarshaler

	if in.Options.Deserializer != nil {
		var err error

		unmarshaler, err = unmarshal.NewUnmarshaler(in.Options.Deserializer)
		if err != nil {
			return nil, err
		}
	}

	start := time.Now()

	cluster, err := cbcolumnar.NewCluster(in.ConnectionString, creds, &cbcolumnar.ClusterOptions{
		TimeoutOptions:  timeoutOpts,
		SecurityOptions: securityOpts,
		Unmarshaler:     unmarshaler,
	})
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	c.conns.Set(in.ClusterConnectionId, cluster)

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) ClusterClose(ctx context.Context, in *clustermanagement.ClusterCloseRequest) (*result.EmptyResultOrFailureResponse, error) {
	cluster, ok := c.conns.Delete(in.ExecutionContext.ClusterId)

	if !ok {
		return nil, fmt.Errorf("no cluster found with id %s", in.ExecutionContext.ClusterId)
	}

	start := time.Now()

	err := cluster.Close()
	if err != nil {
		return &result.EmptyResultOrFailureResponse{
			Result: fiterror.ConvertSDKErrorToFailureResponse(err),
			Metadata: &metadata.ResponseMetadata{
				ElapsedNanos: time.Since(start).Nanoseconds(),
				Initiated:    timestamppb.New(start),
			},
		}, nil
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}

func (c *ColumnarPerformer) CloseAllClusters(ctx context.Context, in *clustermanagement.CloseAllColumnarClustersRequest) (*result.EmptyResultOrFailureResponse, error) {
	clusters := c.conns.Clear()

	start := time.Now()

	for _, cluster := range clusters {
		_ = cluster.Close()
	}

	return &result.EmptyResultOrFailureResponse{
		Result: &result.EmptyResultOrFailureResponse_EmptySuccess{
			EmptySuccess: true,
		},
		Metadata: &metadata.ResponseMetadata{
			ElapsedNanos: time.Since(start).Nanoseconds(),
			Initiated:    timestamppb.New(start),
		},
	}, nil
}
