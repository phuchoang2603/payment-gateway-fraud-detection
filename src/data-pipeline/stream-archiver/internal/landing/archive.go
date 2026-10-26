package landing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ObjectStore interface {
	Create(context.Context, string, []byte) error
}
type File struct {
	Key           string `json:"key"`
	SHA256        string `json:"sha256"`
	Bytes         int    `json:"bytes"`
	Rows          int    `json:"rows"`
	Table         string `json:"table"`
	SchemaVersion uint32 `json:"schema_version"`
}
type Manifest struct {
	Format      string `json:"format"`
	Topic       string `json:"topic"`
	Partition   int32  `json:"partition"`
	FirstOffset int64  `json:"first_offset"`
	LastOffset  int64  `json:"last_offset"`
	Rows        int    `json:"rows"`
	Files       []File `json:"files"`
}

func Digest(body []byte) string { sum := sha256.Sum256(body); return hex.EncodeToString(sum[:]) }

func Publish(ctx context.Context, store ObjectStore, records []*kgo.Record, labelTopic string) ([]string, error) {
	partitions := map[string][]*kgo.Record{}
	for _, r := range records {
		key := fmt.Sprintf("%s/%010d", r.Topic, r.Partition)
		partitions[key] = append(partitions[key], r)
	}
	keys := make([]string, 0, len(partitions))
	for k := range partitions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	manifests := []string{}
	for _, key := range keys {
		batch := partitions[key]
		first, last := batch[0], batch[len(batch)-1]
		manifest := Manifest{Format: "decoded-payments-parquet-v1", Topic: first.Topic, Partition: first.Partition, FirstOffset: first.Offset, LastOffset: last.Offset, Rows: len(batch), Files: []File{}}
		groups := map[string][]Decoded{}
		for i, r := range batch {
			if i > 0 && r.Offset <= batch[i-1].Offset {
				return nil, fmt.Errorf("non-increasing partition offsets")
			}
			row, err := Decode(r, labelTopic)
			if err != nil {
				return nil, fmt.Errorf("decode %s partition %d offset %d: %w", r.Topic, r.Partition, r.Offset, err)
			}
			group := fmt.Sprintf("%s/v%d", row.Table, row.Version)
			groups[group] = append(groups[group], row)
		}
		names := make([]string, 0, len(groups))
		for name := range groups {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			rows := groups[name]
			body, err := Encode(rows)
			if err != nil {
				return nil, err
			}
			checksum := Digest(body)
			path := fmt.Sprintf("streaming/%s/topic=%s/partition=%d/%020d-%020d-v%d-%s.parquet", rows[0].Table, first.Topic, first.Partition, first.Offset, last.Offset, rows[0].Version, checksum)
			if err = store.Create(ctx, path, body); err != nil {
				return nil, err
			}
			manifest.Files = append(manifest.Files, File{path, checksum, len(body), len(rows), rows[0].Table, rows[0].Version})
		}
		body, err := json.Marshal(manifest)
		if err != nil {
			return nil, err
		}
		path := fmt.Sprintf("streaming/_batches/topic=%s/partition=%d/%020d-%020d-%s.json", first.Topic, first.Partition, first.Offset, last.Offset, Digest(body))
		if err = store.Create(ctx, path, body); err != nil {
			return nil, err
		}
		manifests = append(manifests, path)
	}
	return manifests, nil
}
