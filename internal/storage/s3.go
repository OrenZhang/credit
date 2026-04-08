/*
Copyright 2025 linux.do

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/linux-do/credit/internal/config"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

var (
	client     *s3.Client
	bucket     string
	keyPrefix  string
	cdnURL     string
	httpClient *http.Client
)

func init() {
	cfg := config.Config.S3
	if !cfg.Enabled {
		log.Println("[Storage] S3 storage disabled")
		return
	}

	bucket = cfg.Bucket
	keyPrefix = cfg.KeyPrefix
	cdnURL = strings.TrimRight(cfg.CdnURL, "/")

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		),
	)
	if err != nil {
		log.Fatalf("[Storage] failed to load AWS config: %v\n", err)
	}

	client = s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle
	})

	// HTTP client for CDN requests
	httpClient = &http.Client{
		Timeout: 10 * time.Second,
		Transport: otelhttp.NewTransport(&http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     60 * time.Second,
		}),
	}

	log.Printf("[Storage] S3 storage initialized (bucket: %s, prefix: %s, cdn: %s)\n", bucket, keyPrefix, cdnURL)
}

// BuildKey constructs a full S3 object key with the configured prefix.
func BuildKey(path string) string {
	return keyPrefix + path
}

// PutObject uploads a file to S3.
func PutObject(ctx context.Context, key string, body io.Reader, size int64, contentType string) error {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	}

	_, err := client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("s3 put object failed: %w", err)
	}
	return nil
}

// ObjectInfo holds metadata about a retrieved object.
type ObjectInfo struct {
	Body          io.ReadCloser
	ContentLength int64
	ContentType   string
}

// GetObject retrieves a file directly from S3.
func GetObject(ctx context.Context, key string) (*ObjectInfo, error) {
	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 get object failed: %w", err)
	}

	contentType := "application/octet-stream"
	if output.ContentType != nil {
		contentType = *output.ContentType
	}

	var contentLength int64
	if output.ContentLength != nil {
		contentLength = *output.ContentLength
	}

	return &ObjectInfo{
		Body:          output.Body,
		ContentLength: contentLength,
		ContentType:   contentType,
	}, nil
}

// GetObjectViaProxy retrieves a file via CDN if configured, otherwise falls back to S3.
func GetObjectViaProxy(ctx context.Context, key string) (*ObjectInfo, error) {
	if cdnURL == "" {
		return GetObject(ctx, key)
	}

	url := cdnURL + "/" + key
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create cdn request failed: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cdn request failed: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("cdn returned status %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return &ObjectInfo{
		Body:          resp.Body,
		ContentLength: resp.ContentLength,
		ContentType:   contentType,
	}, nil
}

// DeleteObject deletes a file from S3.
func DeleteObject(ctx context.Context, key string) error {
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 delete object failed: %w", err)
	}
	return nil
}
