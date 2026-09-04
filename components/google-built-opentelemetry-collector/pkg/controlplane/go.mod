module github.com/GoogleCloudPlatform/opentelemetry-operations-collector/components/google-built-opentelemetry-collector/pkg/controlplane

go 1.26.0

replace github.com/GoogleCloudPlatform/opentelemetry-operations-collector => ../../../../

require (
	github.com/GoogleCloudPlatform/opentelemetry-operations-collector v0.0.0-00010101000000-000000000000
	github.com/envoyproxy/go-control-plane/envoy v1.37.0
	github.com/fsnotify/fsnotify v1.10.1
	go.uber.org/zap v1.28.0
	golang.org/x/sync v0.22.0
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260803160001-6ac0973c030d
	google.golang.org/grpc v1.83.2
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cncf/xds/go v0.0.0-20260202195803-dba9d589def2 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.3 // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)
