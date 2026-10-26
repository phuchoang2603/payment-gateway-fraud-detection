package landing

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestConditionalPublicationConfirmsReplayAndRejectsCollision(t *testing.T) {
	var mu sync.Mutex
	body := []byte("immutable")
	present, collision := false, false
	writes := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if r.Header.Get("If-None-Match") != "*" {
				t.Error("unconditional publication")
			}
			if present {
				w.WriteHeader(412)
				_, _ = fmt.Fprint(w, "<Error><Code>PreconditionFailed</Code></Error>")
				return
			}
			present = true
			writes++
			w.WriteHeader(200)
		case http.MethodHead:
			if !present {
				w.WriteHeader(404)
				return
			}
			checksum := Digest(body)
			if collision {
				checksum = "different"
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.Header().Set("X-Amz-Meta-Sha256", checksum)
			w.Header().Set("X-Amz-Meta-Landing-Format", "decoded-payments-parquet-v1")
			w.WriteHeader(200)
		default:
			t.Errorf("unexpected object operation %s", r.Method)
			w.WriteHeader(405)
		}
	}))
	defer server.Close()
	store := S3{Bucket: "synthetic-source", Client: s3.New(s3.Options{BaseEndpoint: aws.String(server.URL), Region: "us-east-1", UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")})}
	for range 2 {
		if err := store.Create(context.Background(), "streaming/file", body); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	collision = true
	mu.Unlock()
	if err := store.Create(context.Background(), "streaming/file", body); err == nil {
		t.Fatal("accepted metadata collision")
	}
	if writes != 1 {
		t.Fatalf("overwrote existing object: %d", writes)
	}
	if err := store.Create(context.Background(), "historical/file", body); err == nil {
		t.Fatal("wrote outside landing prefix")
	}
}
