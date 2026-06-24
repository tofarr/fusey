package chunks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ErrNotFound is returned by S3Store when a requested object does not exist.
var ErrNotFound = errors.New("object not found")

// S3Store is a Store backed by an S3-compatible object store.
// All chunk objects are stored under Prefix (e.g. "chunks/") within the bucket.
// The index snapshot is managed separately via PutRaw / GetRaw.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string // prepended to every chunk key, e.g. "" or "pod-abc/"
}

// NewS3Store constructs an S3Store. If accessKey/secretKey are empty the
// default AWS credential chain is used (env vars, ~/.aws, IAM role, etc.).
// Set forcePathStyle=true for MinIO and most self-hosted S3-compatible stores.
// prefix is prepended to every object key; use it to isolate multiple Fusey
// instances within a shared bucket (e.g. "pod-abc/"). May be empty.
func NewS3Store(ctx context.Context, bucket, endpoint, region, accessKey, secretKey, prefix string, forcePathStyle bool) (*S3Store, error) {
	var loadOpts []func(*awsconfig.LoadOptions) error
	loadOpts = append(loadOpts, awsconfig.WithRegion(region))
	if accessKey != "" && secretKey != "" {
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	var clientOpts []func(*s3.Options)
	if endpoint != "" {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}
	if forcePathStyle {
		clientOpts = append(clientOpts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	return &S3Store{
		client: s3.NewFromConfig(awsCfg, clientOpts...),
		bucket: bucket,
		prefix: prefix,
	}, nil
}

// chunkKey returns the full S3 key for a chunk ID.
func (s *S3Store) chunkKey(id string) string {
	return s.prefix + id
}

// Put writes data as a new chunk object. Concurrent or duplicate puts for the
// same id will silently overwrite (S3 PutObject semantics); callers that need
// strict once-only semantics must ensure uniqueness themselves.
func (s *S3Store) Put(ctx context.Context, id string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.chunkKey(id)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("put chunk %q: %w", id, err)
	}
	return nil
}

// GetRange reads length bytes at offset from the chunk with id.
func (s *S3Store) GetRange(ctx context.Context, id string, offset, length int64) ([]byte, error) {
	rangeHdr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.chunkKey(id)),
		Range:  aws.String(rangeHdr),
	})
	if err != nil {
		return nil, fmt.Errorf("get range chunk %q [%d+%d]: %w", id, offset, length, err)
	}
	defer resp.Body.Close()
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read chunk %q body: %w", id, err)
	}
	return buf, nil
}

// Delete removes the chunk object with id. Deleting a non-existent key is not
// an error (S3 semantics).
func (s *S3Store) Delete(ctx context.Context, id string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.chunkKey(id)),
	})
	if err != nil {
		return fmt.Errorf("delete chunk %q: %w", id, err)
	}
	return nil
}

// List returns the IDs of all chunk objects in the bucket under the configured
// prefix. The index object (index.cbor) is excluded from the result.
// Handles S3 pagination transparently.
func (s *S3Store) List(ctx context.Context) ([]string, error) {
	var ids []string
	var contToken *string
	for {
		resp, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(s.prefix),
			ContinuationToken: contToken,
		})
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}
			id := strings.TrimPrefix(*obj.Key, s.prefix)
			if id == "index.cbor" {
				continue // never expose the index as a chunk
			}
			ids = append(ids, id)
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		contToken = resp.NextContinuationToken
	}
	return ids, nil
}

// Size returns the byte length of the chunk object with id.
func (s *S3Store) Size(ctx context.Context, id string) (int64, error) {
	resp, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.chunkKey(id)),
	})
	if err != nil {
		return 0, fmt.Errorf("head chunk %q: %w", id, err)
	}
	if resp.ContentLength == nil {
		return 0, nil
	}
	return *resp.ContentLength, nil
}

// PutRaw writes data to an arbitrary key in the bucket without the chunk
// prefix. Used to persist the index snapshot (key = "{prefix}index.cbor").
func (s *S3Store) PutRaw(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("put object %q: %w", key, err)
	}
	return nil
}

// GetRaw reads the full contents of the object at key.
// Returns ErrNotFound if the object does not exist.
func (s *S3Store) GetRaw(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read object %q body: %w", key, err)
	}
	return data, nil
}

// IndexKey returns the S3 key used to store the index snapshot.
// It lives at {prefix}index.cbor within the bucket.
func (s *S3Store) IndexKey() string {
	return s.prefix + "index.cbor"
}

// isS3NotFound reports whether err represents a missing object (HTTP 404 /
// NoSuchKey). Covers both real AWS S3 and S3-compatible stores.
func isS3NotFound(err error) bool {
	var noKey *types.NoSuchKey
	if errors.As(err, &noKey) {
		return true
	}
	// S3-compatible stores may surface a generic HTTP 404 instead of NoSuchKey.
	type httpCoder interface{ HTTPStatusCode() int }
	var hc httpCoder
	if errors.As(err, &hc) {
		return hc.HTTPStatusCode() == 404
	}
	return false
}
