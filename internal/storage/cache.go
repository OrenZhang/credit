package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/linux-do/credit/internal/config"
	"github.com/linux-do/credit/internal/logger"
	"github.com/linux-do/credit/internal/otel_trace"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var localCacheEnabled = false
var localCacheDir = ""
var cacheFilePath = "%s/%s"
var cacheMetaFilePath = "%s/%s.meta"

type metaInfo struct {
	ContentType   string `json:"content_type"`
	ContentLength int64  `json:"content_length"`
}

func init() {
	cfg := config.Config.S3.LocalCache
	localCacheEnabled = cfg.Enabled && cfg.CacheDir != ""
	localCacheDir = strings.TrimSuffix(cfg.CacheDir, "/")
	if localCacheEnabled {
		if err := os.MkdirAll(cfg.CacheDir, 0755); err != nil {
			log.Fatalf("[Storage] failed to create local cache directory: %v\n", err)
		}
	}
}

func GetObjectViaCache(ctx context.Context, key string) (*ObjectInfo, error) {
	ctx, span := otel_trace.Start(ctx, "S3.GetObjectViaCache", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	var localPath, metaPath string

	// 检查本地缓存
	if localCacheEnabled {
		key = strings.TrimPrefix(key, "/")
		localPath = fmt.Sprintf(cacheFilePath, localCacheDir, key)
		metaPath = fmt.Sprintf(cacheMetaFilePath, localCacheDir, key)
		objInfo, err := GetLocalCacheFile(ctx, localPath, metaPath)
		if err != nil {
			return nil, err
		}
		if objInfo != nil {
			return objInfo, nil
		}
	}

	// 没有缓存，通过 CDN 获取
	objInfo, err := GetObjectViaProxy(ctx, key)
	if err != nil {
		return nil, err
	}

	// 如果启用了本地缓存，异步保存到本地
	if localCacheEnabled {
		bodyBytes, err := io.ReadAll(objInfo.Body)
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			logger.ErrorF(ctx, "Failed to read object body for caching: %v", err)
			return objInfo, nil
		}
		_ = objInfo.Body.Close()
		objCopy := *objInfo
		objCopy.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
		objInfo.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
		go SaveToLocalCache(ctx, localPath, metaPath, &objCopy)
	}

	return objInfo, nil
}

func GetLocalCacheFile(ctx context.Context, localPath, metaPath string) (*ObjectInfo, error) {
	ctx, span := otel_trace.Start(ctx, "S3.GetLocalCacheFile", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	// 尝试打开本地缓存文件
	file, err := os.Open(localPath)

	// 文件不存在
	if err != nil && os.IsNotExist(err) {
		return nil, nil
	}

	// 判断是否为其他异常
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to open local cache file %s: %v", localPath, err)
		return nil, LocalCacheError{}
	}

	// 读取元信息
	metaData, err := os.ReadFile(metaPath)

	// 文件不存在
	if err != nil && os.IsNotExist(err) {
		return nil, nil
	}

	// 判断是否为其他异常
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to read local cache meta file %s: %v", metaPath, err)
		return nil, LocalCacheError{}
	}

	// 解析元信息
	meta := &metaInfo{}
	if err := json.Unmarshal(metaData, meta); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to parse local cache meta file %s: %v", metaPath, err)
		return nil, LocalCacheError{}
	}

	return &ObjectInfo{Body: file, ContentLength: meta.ContentLength, ContentType: meta.ContentType}, nil
}

func SaveToLocalCache(ctx context.Context, localPath, metaPath string, objInfo *ObjectInfo) {
	ctx, span := otel_trace.Start(ctx, "S3.SaveToLocalCache", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	// 创建目录
	localDir := filepath.Dir(localPath)
	if err := os.MkdirAll(localDir, 0755); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to create local cache directory %s: %v", localDir, err)
		return
	}

	// 创建临时文件
	tempFile, err := os.CreateTemp(localDir, "cache_temp_*")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to create temp file for local cache: %v", err)
		return
	}
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()

	// 将内容写入临时文件
	if _, err := tempFile.ReadFrom(objInfo.Body); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to write to temp file for local cache: %v", err)
		return
	}

	// 写入元信息
	meta := &metaInfo{ContentType: objInfo.ContentType, ContentLength: objInfo.ContentLength}
	metaData, err := json.Marshal(meta)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to marshal meta info for local cache: %v", err)
		return
	}

	// 创建临时元信息文件
	tempMetaFile, err := os.CreateTemp(localDir, "cache_meta_temp_*")
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to create temp meta file for local cache: %v", err)
		return
	}
	defer os.Remove(tempMetaFile.Name())
	defer tempMetaFile.Close()

	// 将元信息写入临时文件
	if _, err := tempMetaFile.Write(metaData); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to write to temp meta file for local cache: %v", err)
		return
	}

	// 将临时文件重命名为最终文件
	if err := os.Rename(tempFile.Name(), localPath); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to rename temp file to local cache file %s: %v", localPath, err)
		return
	}

	// 将临时元信息文件重命名为最终元信息文件
	if err := os.Rename(tempMetaFile.Name(), metaPath); err != nil {
		span.SetStatus(codes.Error, err.Error())
		logger.ErrorF(ctx, "Failed to rename temp meta file to local cache meta file %s: %v", metaPath, err)
		return
	}
}
