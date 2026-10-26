package landing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type S3 struct {
	Client *s3.Client
	Bucket string
}

func (s S3) Create(ctx context.Context, key string, body []byte) error {
	if !strings.HasPrefix(key, "streaming/") {
		return fmt.Errorf("object outside streaming prefix")
	}
	hash := sha256.Sum256(body)
	checksum := Digest(body)
	_, err := s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))), IfNoneMatch: aws.String("*"), ChecksumSHA256: aws.String(base64.StdEncoding.EncodeToString(hash[:])), Metadata: map[string]string{"sha256": checksum, "landing-format": "decoded-payments-parquet-v1"}})
	if err != nil {
		var api smithy.APIError
		if !errors.As(err, &api) || (api.ErrorCode() != "PreconditionFailed" && api.ErrorCode() != "ConditionalRequestConflict") {
			return fmt.Errorf("conditional publication failed: %w", err)
		}
	}
	head, headErr := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.Bucket), Key: aws.String(key)})
	if headErr != nil {
		return fmt.Errorf("publication confirmation failed: %w", headErr)
	}
	metadata := map[string]string{}
	for k, v := range head.Metadata {
		metadata[strings.ToLower(k)] = v
	}
	if aws.ToInt64(head.ContentLength) != int64(len(body)) || metadata["sha256"] != checksum || metadata["landing-format"] != "decoded-payments-parquet-v1" {
		return fmt.Errorf("immutable object collision: %s", key)
	}
	return nil
}
