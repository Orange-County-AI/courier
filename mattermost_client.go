package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrMMAttachmentTooLarge = errors.New("Mattermost attachment exceeds download cap")

type MMUser struct {
	ID       string `json:"id"`
	Username string `json:"username"`
}

type MMCreatedPost struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	RootID    string `json:"root_id"`
	Message   string `json:"message"`
	CreateAt  int64  `json:"create_at"`
}

type MMPostList struct {
	Order []string          `json:"order"`
	Posts map[string]MMPost `json:"posts"`
}

type MMTeam struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

type MMChannel struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	TeamID      string `json:"team_id,omitempty"`
}

type MMFileInfo struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Extension string `json:"extension"`
	MIMEType  string `json:"mime_type"`
	Size      int64  `json:"size"`
	Width     int64  `json:"width,omitempty"`
	Height    int64  `json:"height,omitempty"`
}

type MMCreatePost struct {
	ChannelID string `json:"channel_id"`
	Message   string `json:"message"`
	RootID    string `json:"root_id,omitempty"`
}

type MattermostAPI interface {
	WebSocketURL() string
	Me(context.Context) (MMUser, error)
	CreatePost(context.Context, MMCreatePost) (MMCreatedPost, error)
	GetChannelPosts(context.Context, string, int) (MMPostList, error)
	GetThread(context.Context, string) (MMPostList, error)
	GetChannelPostsSince(context.Context, string, int64) (MMPostList, error)
	GetMyTeams(context.Context) ([]MMTeam, error)
	GetMyTeamChannels(context.Context, string) ([]MMChannel, error)
	GetAllMyChannels(context.Context) ([]MMChannel, error)
	GetChannel(context.Context, string) (MMChannel, error)
	UsersByIDs(context.Context, []string) ([]MMUser, error)
	GetFileInfo(context.Context, string) (MMFileInfo, error)
	GetFileBytes(context.Context, string, int64) ([]byte, error)
}

type MattermostClient struct {
	baseURL string
	token   string
	http    *http.Client
}

func NewMattermostClient(baseURL, token string, client *http.Client) (*MattermostClient, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Mattermost URL %q", baseURL)
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &MattermostClient{baseURL: baseURL, token: token, http: client}, nil
}

func (c *MattermostClient) WebSocketURL() string {
	parsed, err := url.Parse(c.baseURL)
	if err != nil {
		return ""
	}
	if parsed.Scheme == "https" {
		parsed.Scheme = "wss"
	} else {
		parsed.Scheme = "ws"
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/v4/websocket"
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func mmRequestJSON[T any](ctx context.Context, client *MattermostClient, method, path string, body any) (T, error) {
	var zero T
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return zero, err
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, client.baseURL+"/api/v4"+path, bytes.NewReader(encoded))
	if err != nil {
		return zero, err
	}
	request.Header.Set("Authorization", "Bearer "+client.token)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.http.Do(request)
	if err != nil {
		return zero, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 300))
		return zero, fmt.Errorf("MM %s %s: HTTP %d %s", method, path, response.StatusCode, string(message))
	}
	if err := json.NewDecoder(response.Body).Decode(&zero); err != nil {
		return zero, fmt.Errorf("decode MM %s %s: %w", method, path, err)
	}
	return zero, nil
}

func (c *MattermostClient) Me(ctx context.Context) (MMUser, error) {
	return mmRequestJSON[MMUser](ctx, c, http.MethodGet, "/users/me", nil)
}

func (c *MattermostClient) CreatePost(ctx context.Context, post MMCreatePost) (MMCreatedPost, error) {
	return mmRequestJSON[MMCreatedPost](ctx, c, http.MethodPost, "/posts", post)
}

func (c *MattermostClient) GetChannelPosts(ctx context.Context, channelID string, perPage int) (MMPostList, error) {
	if perPage <= 0 {
		perPage = 30
	}
	path := "/channels/" + url.PathEscape(channelID) + "/posts?per_page=" + strconv.Itoa(perPage)
	return mmRequestJSON[MMPostList](ctx, c, http.MethodGet, path, nil)
}

func (c *MattermostClient) GetThread(ctx context.Context, postID string) (MMPostList, error) {
	return mmRequestJSON[MMPostList](ctx, c, http.MethodGet, "/posts/"+url.PathEscape(postID)+"/thread", nil)
}

func (c *MattermostClient) GetChannelPostsSince(ctx context.Context, channelID string, since int64) (MMPostList, error) {
	path := "/channels/" + url.PathEscape(channelID) + "/posts?since=" + strconv.FormatInt(since, 10)
	return mmRequestJSON[MMPostList](ctx, c, http.MethodGet, path, nil)
}

func (c *MattermostClient) GetMyTeams(ctx context.Context) ([]MMTeam, error) {
	return mmRequestJSON[[]MMTeam](ctx, c, http.MethodGet, "/users/me/teams", nil)
}

func (c *MattermostClient) GetMyTeamChannels(ctx context.Context, teamID string) ([]MMChannel, error) {
	return mmRequestJSON[[]MMChannel](ctx, c, http.MethodGet, "/users/me/teams/"+url.PathEscape(teamID)+"/channels", nil)
}

func (c *MattermostClient) GetAllMyChannels(ctx context.Context) ([]MMChannel, error) {
	return mmRequestJSON[[]MMChannel](ctx, c, http.MethodGet, "/users/me/channels", nil)
}

func (c *MattermostClient) GetChannel(ctx context.Context, channelID string) (MMChannel, error) {
	return mmRequestJSON[MMChannel](ctx, c, http.MethodGet, "/channels/"+url.PathEscape(channelID), nil)
}

func (c *MattermostClient) UsersByIDs(ctx context.Context, ids []string) ([]MMUser, error) {
	return mmRequestJSON[[]MMUser](ctx, c, http.MethodPost, "/users/ids", ids)
}

func (c *MattermostClient) GetFileInfo(ctx context.Context, fileID string) (MMFileInfo, error) {
	return mmRequestJSON[MMFileInfo](ctx, c, http.MethodGet, "/files/"+url.PathEscape(fileID)+"/info", nil)
}

func (c *MattermostClient) GetFileBytes(ctx context.Context, fileID string, maxBytes int64) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/api/v4/files/"+url.PathEscape(fileID), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("MM GET /files/%s: HTTP %d", fileID, response.StatusCode)
	}
	if maxBytes <= 0 {
		maxBytes = 50 * 1024 * 1024
	}
	content, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maxBytes {
		return nil, ErrMMAttachmentTooLarge
	}
	return content, nil
}
