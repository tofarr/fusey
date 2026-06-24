package chunks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3TestStore returns an S3Store pointed at a real S3-compatible endpoint, or
// skips the test if FUSEY_TEST_ENDPOINT is not set.
//
// To run these tests against a local MinIO instance:
//
//	docker run -p 9000:9000 -e MINIO_ROOT_USER=test -e MINIO_ROOT_PASSWORD=testtest minio/minio server /data
//	FUSEY_TEST_ENDPOINT=http://localhost:9000 \
//	FUSEY_ACCESS_KEY=test FUSEY_SECRET_KEY=testtest \
//	FUSEY_TEST_BUCKET=fusey-test \
//	go test ./internal/chunks/ -run TestS3
func s3TestStore(t *testing.T) *S3Store {
	t.Helper()
	endpoint := os.Getenv("FUSEY_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("FUSEY_TEST_ENDPOINT not set; skipping S3 integration tests")
	}
	bucket := os.Getenv("FUSEY_TEST_BUCKET")
	if bucket == "" {
		bucket = "fusey-test"
	}
	accessKey := os.Getenv("FUSEY_ACCESS_KEY")
	secretKey := os.Getenv("FUSEY_SECRET_KEY")
	// Use a test-run-specific prefix to isolate parallel runs.
	prefix := fmt.Sprintf("test-%d/", os.Getpid())

	store, err := NewS3Store(
		context.Background(),
		bucket, endpoint, "us-east-1", accessKey, secretKey, prefix,
		true, // force path style for MinIO
	)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}

	// Create the bucket if it does not already exist. MinIO does not create
	// buckets automatically, so the first test run against a fresh instance
	// would fail with NoSuchBucket without this step.
	_, cerr := store.client.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if cerr != nil {
		var alreadyExists *types.BucketAlreadyExists
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if !errors.As(cerr, &alreadyExists) && !errors.As(cerr, &alreadyOwned) {
			t.Fatalf("CreateBucket %q: %v", bucket, cerr)
		}
	}
	return store
}

func TestS3PutAndGetRange(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	data := []byte("hello, fusey S3!")
	if err := s.Put(ctx, "chunk-00000001", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { s.Delete(ctx, "chunk-00000001") })

	got, err := s.GetRange(ctx, "chunk-00000001", 7, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if !bytes.Equal(got, []byte("fusey")) {
		t.Errorf("GetRange: got %q, want %q", got, "fusey")
	}
}

func TestS3GetRangeFullObject(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	data := bytes.Repeat([]byte{0xAB}, 1024)
	if err := s.Put(ctx, "chunk-00000002", data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { s.Delete(ctx, "chunk-00000002") })

	got, err := s.GetRange(ctx, "chunk-00000002", 0, int64(len(data)))
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("GetRange full: content mismatch (len %d vs %d)", len(got), len(data))
	}
}

func TestS3Delete(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	if err := s.Put(ctx, "chunk-00000003", []byte("delete me")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "chunk-00000003"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Deleting a non-existent key must not error.
	if err := s.Delete(ctx, "chunk-00000003"); err != nil {
		t.Errorf("second Delete: %v", err)
	}
}

func TestS3ListAndSize(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	chunks := map[string][]byte{
		"chunk-00000010": []byte("aaa"),
		"chunk-00000011": []byte("bb"),
		"chunk-00000012": []byte("c"),
	}
	for id, data := range chunks {
		if err := s.Put(ctx, id, data); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
		t.Cleanup(func() { s.Delete(ctx, id) })
	}

	ids, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sort.Strings(ids)
	want := []string{"chunk-00000010", "chunk-00000011", "chunk-00000012"}
	if len(ids) != len(want) {
		t.Fatalf("List: got %v, want %v", ids, want)
	}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("List[%d]: got %q, want %q", i, id, want[i])
		}
	}

	sz, err := s.Size(ctx, "chunk-00000010")
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if sz != 3 {
		t.Errorf("Size: got %d, want 3", sz)
	}
}

func TestS3ListExcludesIndex(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	// Store an index object and a chunk; List must only return the chunk.
	if err := s.Put(ctx, "chunk-00000020", []byte("data")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Delete(ctx, "chunk-00000020") })

	indexKey := s.IndexKey()
	if err := s.PutRaw(ctx, indexKey, []byte(`{"inodes":{}}`)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.GetRaw(ctx, indexKey) }) // harmless read; real cleanup below
	t.Cleanup(func() {
		// Delete via raw S3 call since there's no DeleteRaw method.
		s.client.DeleteObject(ctx, &s3.DeleteObjectInput{ //nolint:errcheck
			Bucket: aws.String(s.bucket),
			Key:    aws.String(indexKey),
		})
	})

	ids, err := s.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, id := range ids {
		if id == "index.cbor" {
			t.Error("List must not return index.cbor as a chunk ID")
		}
	}
}

func TestS3PutRawAndGetRaw(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	payload := []byte(`{"version":1}`)
	key := s.IndexKey()
	if err := s.PutRaw(ctx, key, payload); err != nil {
		t.Fatalf("PutRaw: %v", err)
	}
	t.Cleanup(func() {
		s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(key),
		})
	})

	got, err := s.GetRaw(ctx, key)
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("GetRaw: got %q, want %q", got, payload)
	}
}

func TestS3GetRawNotFound(t *testing.T) {
	ctx := context.Background()
	s := s3TestStore(t)

	_, err := s.GetRaw(ctx, s.prefix+"no-such-object")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("GetRaw missing object: got %v, want ErrNotFound", err)
	}
}

// TestS3ImplementsStore verifies the interface at compile time.
var _ Store = (*S3Store)(nil)
