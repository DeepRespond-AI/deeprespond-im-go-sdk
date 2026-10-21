package im

// 媒体上传（《功能概述》11.1：创建 → 预签名 PUT 直传 → 完成）与下载地址。

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
)

// UploadInput 上传入参。
type UploadInput struct {
	Name        string
	ContentType string
	Data        []byte
	// Purpose 用途：attachment（随消息附件，默认）| user_avatar | group_avatar | group_file | chatroom_avatar。
	// Kind 类型：image | voice | video | file（为空时按 Content-Type 推断）。
	Purpose string
	Kind    string
	// 可选元数据（图片尺寸/时长），写入 FileInfo 后随消息下发
	Width, Height, DurationMs int64
}

// uploadKind 按 Content-Type 推断文件的 kind。
func uploadKind(contentType string) string {
	switch {
	case strings.HasPrefix(contentType, "image/"):
		return "image"
	case strings.HasPrefix(contentType, "audio/"):
		return "voice"
	case strings.HasPrefix(contentType, "video/"):
		return "video"
	default:
		return "file"
	}
}

// Upload 单文件直传（≤32MB）：创建上传 → PUT 预签名地址 → 完成。
func (c *Client) Upload(ctx context.Context, in UploadInput) (*FileInfo, error) {
	purpose := in.Purpose
	if purpose == "" {
		purpose = "attachment"
	}
	kind := in.Kind
	if kind == "" {
		kind = uploadKind(in.ContentType)
	}
	create := map[string]any{
		"purpose":      purpose,
		"kind":         kind,
		"name":         in.Name,
		"size":         len(in.Data),
		"content_type": in.ContentType,
	}
	var created struct {
		FileID string     `json:"file_id"`
		Upload *PutTarget `json:"upload"` // 服务端把预签名 PUT 目标放在 upload 字段
	}
	if err := c.request(ctx, http.MethodPost, "/client/v1/media/uploads", create, &created); err != nil {
		return nil, err
	}
	if created.Upload != nil && created.Upload.URL != "" {
		if err := c.putPresigned(ctx, created.Upload, in.Data, in.ContentType); err != nil {
			return nil, err
		}
	}
	var info FileInfo
	if err := c.request(ctx, http.MethodPost, "/client/v1/media/uploads/"+created.FileID+"/complete", nil, &info); err != nil {
		return nil, err
	}
	if in.Width > 0 {
		info.Width = in.Width
	}
	if in.Height > 0 {
		info.Height = in.Height
	}
	if in.DurationMs > 0 {
		info.DurationMs = in.DurationMs
	}
	if in.Name != "" && info.Name == "" {
		info.Name = in.Name
	}
	return &info, nil
}

// putPresigned PUT 直传（不带 IM 认证头）。
func (c *Client) putPresigned(ctx context.Context, t *PutTarget, data []byte, contentType string) error {
	method := t.Method
	if method == "" {
		method = http.MethodPut
	}
	req, err := http.NewRequestWithContext(ctx, method, t.URL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	for k, v := range t.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Content-Type") == "" && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return &APIError{Code: "upload_failed", Message: string(body), HTTPStatus: resp.StatusCode}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// DownloadURLs 批量换取媒体下载签名直链（≤100）。
func (c *Client) DownloadURLs(ctx context.Context, urls []string) ([]DownloadURL, error) {
	var resp struct {
		Items []DownloadURL `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/media/download-urls", map[string]any{"urls": urls}, &resp)
	return resp.Items, err
}

// Download 通过消息里的 url（FileInfo.URL / thumbnail_url）下载内容。
// 服务端按逐项返回签名直链（download_url）或错误（file_deleted / forbidden 等），逐项错误原样上抛。
func (c *Client) Download(ctx context.Context, mediaURL string) ([]byte, error) {
	list, err := c.DownloadURLs(ctx, []string{mediaURL})
	if err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, &APIError{Code: "download_failed", Message: "download-urls 返回为空"}
	}
	item := list[0]
	if item.DownloadURL == "" {
		return nil, &APIError{Code: item.Code, Message: item.Message}
	}
	return c.httpGet(ctx, item.DownloadURL)
}

func (c *Client) httpGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, &APIError{Code: "download_failed", HTTPStatus: resp.StatusCode}
	}
	return io.ReadAll(io.LimitReader(resp.Body, 128<<20))
}
