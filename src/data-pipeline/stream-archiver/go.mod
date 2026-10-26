module github.com/phuchoang2603/payment-gateway-fraud-detection/stream-archiver

go 1.26.0

require (
	github.com/aws/aws-sdk-go-v2 v1.47.4
	github.com/aws/aws-sdk-go-v2/credentials v1.20.10
	github.com/aws/aws-sdk-go-v2/service/s3 v1.114.5
	github.com/aws/smithy-go v1.28.6
	github.com/parquet-go/parquet-go v0.32.0
	github.com/phuchoang2603/payment-gateway-fraud-detection/shared v0.0.0
	github.com/twmb/franz-go v1.22.2
	github.com/twmb/franz-go/pkg/kmsg v1.15.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.24 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.11.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.20.7 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261005182115-fad411399dd8 // indirect
	google.golang.org/grpc v1.83.2 // indirect
)

replace github.com/phuchoang2603/payment-gateway-fraud-detection/shared => ../../payment-gateway/shared
