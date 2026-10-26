package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/phuchoang2603/payment-gateway-fraud-detection/stream-archiver/internal/landing"
	"github.com/twmb/franz-go/pkg/kgo"
)

func env(key, value string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return value
}

func positive(key string, value int) (int, error) {
	s := env(key, strconv.Itoa(value))
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("invalid %s", key)
	}
	return n, nil
}

func run(ctx context.Context) error {
	maxRecords, err := positive("LANDING_MAX_RECORDS", 10000)
	if err != nil {
		return err
	}
	maxBytes, err := positive("LANDING_MAX_BYTES", 32*1024*1024)
	if err != nil {
		return err
	}
	if maxBytes > 256*1024*1024 || maxRecords > 1000000 {
		return fmt.Errorf("landing capacity exceeds supported limit")
	}
	flush, err := time.ParseDuration(env("LANDING_FLUSH_INTERVAL", "10s"))
	if err != nil || flush <= 0 || flush > time.Minute {
		return fmt.Errorf("invalid landing flush interval")
	}
	labelTopic := env("LABEL_TOPIC", "fraud.labels.v1")
	topics := strings.Split(env("LANDING_TOPICS", "payments.events.v1,fraud.labels.v1"), ",")
	pattern := regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	for _, topic := range topics {
		if !pattern.MatchString(topic) || topic == "." || topic == ".." {
			return fmt.Errorf("invalid topic")
		}
	}
	access, secret := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if access == "" || secret == "" {
		return fmt.Errorf("missing object store credentials")
	}
	store := landing.S3{Bucket: env("LANDING_BUCKET", "synthetic-source"), Client: s3.New(s3.Options{Region: env("AWS_REGION", "us-east-1"), BaseEndpoint: aws.String(env("S3_ENDPOINT", "https://minio.home.phuchoang.sbs")), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(access, secret, "")})}
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(env("KAFKA_BROKERS", "payment-gateway-kafka-kafka-bootstrap.payment-gateway.svc:9092"), ",")...), kgo.ConsumerGroup(env("LANDING_GROUP", "payments-parquet-landing-v1")), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchMaxBytes(int32(maxBytes)), kgo.FetchMaxPartitionBytes(int32(maxBytes)))
	if err != nil {
		return err
	}
	defer client.CloseAllowingRebalance()
	var carried *kgo.Record
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(ctx, flush)
		records := []*kgo.Record{}
		size := 0
		for len(records) < maxRecords && size < maxBytes && batchCtx.Err() == nil {
			if carried == nil {
				fetched := client.PollRecords(batchCtx, 1)
				for _, fetchErr := range fetched.Errors() {
					if !errors.Is(fetchErr.Err, context.DeadlineExceeded) && !errors.Is(fetchErr.Err, context.Canceled) {
						cancel()
						return fmt.Errorf("kafka fetch failed")
					}
				}
				fetched.EachRecord(func(r *kgo.Record) { carried = r })
				if carried == nil {
					continue
				}
			}
			n := len(carried.Key) + len(carried.Value)
			for _, h := range carried.Headers {
				n += len(h.Key) + len(h.Value)
			}
			if n > maxBytes {
				cancel()
				return fmt.Errorf("record exceeds landing byte capacity; no offsets committed")
			}
			if size+n > maxBytes {
				break
			}
			records = append(records, carried)
			size += n
			carried = nil
		}
		cancel()
		if len(records) > 0 && ctx.Err() == nil {
			manifests, err := landing.Publish(ctx, store, records, labelTopic)
			if err != nil {
				client.AllowRebalance()
				return err
			}
			if err = client.CommitRecords(ctx, records...); err != nil {
				client.AllowRebalance()
				return fmt.Errorf("kafka commit failed after publication; replay possible")
			}
			slog.Info("landing_batch_committed", "records", len(records), "source_bytes", size, "completed_manifests", manifests, "recovery", "at-least-once")
		}
		if carried != nil {
			// Rewind the polled overflow record before permitting ownership changes.
			client.SetOffsets(map[string]map[int32]kgo.EpochOffset{carried.Topic: {carried.Partition: {Epoch: carried.LeaderEpoch, Offset: carried.Offset}}})
			carried = nil
		}
		client.AllowRebalance()
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		slog.Error("archiver_failed", "error", err)
		os.Exit(1)
	}
}
