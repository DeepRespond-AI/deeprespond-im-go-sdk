package im

// 群组（REST）。

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// CreateGroupInput 建群入参。
type CreateGroupInput struct {
	Name        string   `json:"name,omitempty"`
	Description string   `json:"description,omitempty"`
	AvatarURL   string   `json:"avatar_url,omitempty"`
	Public      bool     `json:"public,omitempty"`
	Members     []string `json:"members,omitempty"` // 初始成员（除自己）
}

// CreateGroup 建群，返回群信息与逐个成员结果。
func (c *Client) CreateGroup(ctx context.Context, in CreateGroupInput) (*GroupInfo, error) {
	body := map[string]any{"name": in.Name}
	if in.Description != "" {
		body["description"] = in.Description
	}
	if in.AvatarURL != "" {
		body["avatar_url"] = in.AvatarURL
	}
	if in.Public {
		body["public"] = true
	}
	if len(in.Members) > 0 {
		body["members"] = in.Members
	}
	var resp struct {
		GroupInfo
		Results []GroupMemberResult `json:"results"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/groups", body, &resp)
	if err != nil {
		return nil, err
	}
	g := resp.GroupInfo
	return &g, nil
}

// Group 群详情。
func (c *Client) Group(ctx context.Context, groupID string) (*GroupInfo, error) {
	var g GroupInfo
	err := c.request(ctx, http.MethodGet, "/client/v1/groups/"+url.PathEscape(groupID), nil, &g)
	return &g, err
}

// LookupGroups 批量查群（≤100）。
func (c *Client) LookupGroups(ctx context.Context, groupIDs []string) ([]GroupInfo, error) {
	var resp struct {
		Items []GroupInfo `json:"items"`
	}
	err := c.request(ctx, http.MethodPost, "/client/v1/groups/lookup", map[string]any{"group_ids": groupIDs}, &resp)
	return resp.Items, err
}

// MyGroups 我加入的群（digest）。
func (c *Client) MyGroups(ctx context.Context) ([]GroupInfo, error) {
	var resp struct {
		Items []struct {
			GroupID string `json:"group_id"`
		} `json:"items"`
	}
	if err := c.getJSON(ctx, "/client/v1/groups/digest", url.Values{"limit": {"1000"}}, &resp); err != nil {
		return nil, err
	}
	if len(resp.Items) == 0 {
		return nil, nil
	}
	ids := make([]string, len(resp.Items))
	for i, it := range resp.Items {
		ids[i] = it.GroupID
	}
	return c.LookupGroups(ctx, ids)
}

// PublicGroups 公开群列表。
func (c *Client) PublicGroups(ctx context.Context, cursor string, limit int64) (*Page[GroupInfo], error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", fmt.Sprint(limit))
	}
	var p Page[GroupInfo]
	err := c.getJSON(ctx, "/client/v1/public-groups", q, &p)
	return &p, err
}

// UpdateGroup 更新群资料（仅管理员/群主）。
func (c *Client) UpdateGroup(ctx context.Context, groupID string, name, description, avatarURL *string) (*GroupInfo, error) {
	body := map[string]any{}
	if name != nil {
		body["name"] = *name
	}
	if description != nil {
		body["description"] = *description
	}
	if avatarURL != nil {
		body["avatar_url"] = *avatarURL
	}
	var g GroupInfo
	err := c.request(ctx, http.MethodPatch, "/client/v1/groups/"+url.PathEscape(groupID), body, &g)
	return &g, err
}

// DismissGroup 解散群（仅群主）。
func (c *Client) DismissGroup(ctx context.Context, groupID string) error {
	return c.request(ctx, http.MethodDelete, "/client/v1/groups/"+url.PathEscape(groupID), nil, nil)
}

// LeaveGroup 退群。
func (c *Client) LeaveGroup(ctx context.Context, groupID string) error {
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/groups/%s/leave", url.PathEscape(groupID)), nil, nil)
}

// GroupMembers 成员列表（known_version 增量）。
func (c *Client) GroupMembers(ctx context.Context, groupID string) ([]GroupMember, error) {
	var out []GroupMember
	q := url.Values{"limit": {"500"}}
	for {
		var p struct {
			Items      []GroupMember `json:"items"`
			NextCursor string        `json:"next_cursor,omitempty"`
		}
		if err := c.getJSON(ctx, fmt.Sprintf("/client/v1/groups/%s/members", url.PathEscape(groupID)), q, &p); err != nil {
			return out, err
		}
		out = append(out, p.Items...)
		if p.NextCursor == "" {
			break
		}
		q.Set("cursor", p.NextCursor)
	}
	return out, nil
}

// AddGroupMembers 邀请成员（按群的 invite_mode 可能直接入群或产生邀请）。
func (c *Client) AddGroupMembers(ctx context.Context, groupID string, usernames []string) ([]GroupMemberResult, error) {
	var resp struct {
		Results []GroupMemberResult `json:"results"`
	}
	err := c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/groups/%s/members", url.PathEscape(groupID)), map[string]any{"usernames": usernames}, &resp)
	return resp.Results, err
}

// RemoveGroupMember 移除成员（仅管理员/群主）。
func (c *Client) RemoveGroupMember(ctx context.Context, groupID, username string) error {
	_, err := requestRaw[map[string]any](c, ctx, http.MethodDelete,
		fmt.Sprintf("/client/v1/groups/%s/members/%s", url.PathEscape(groupID), url.PathEscape(username)), nil)
	return err
}

// SetGroupMemberRole 设置成员角色（admin|member）。
func (c *Client) SetGroupMemberRole(ctx context.Context, groupID, username, role string) error {
	return c.request(ctx, http.MethodPut,
		fmt.Sprintf("/client/v1/groups/%s/members/%s", url.PathEscape(groupID), url.PathEscape(username)),
		map[string]any{"role": role}, nil)
}

// GroupInvitations 收到的群邀请。
func (c *Client) GroupInvitations(ctx context.Context) ([]GroupInvitation, error) {
	var p Page[GroupInvitation]
	err := c.getJSON(ctx, "/client/v1/group-invitations", nil, &p)
	return p.Items, err
}

// AcceptGroupInvitation 接受邀请。
func (c *Client) AcceptGroupInvitation(ctx context.Context, invitationID string) (*GroupInfo, error) {
	var g GroupInfo
	err := c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/group-invitations/%s/accept", url.PathEscape(invitationID)), map[string]any{}, &g)
	return &g, err
}

// DeclineGroupInvitation 拒绝邀请。
func (c *Client) DeclineGroupInvitation(ctx context.Context, invitationID string) error {
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/group-invitations/%s/decline", url.PathEscape(invitationID)), map[string]any{}, nil)
}

// MyGroupApplications 我发出的入群申请。
func (c *Client) MyGroupApplications(ctx context.Context) (*Page[map[string]any], error) {
	var p Page[map[string]any]
	err := c.getJSON(ctx, "/client/v1/group-applications", nil, &p)
	return &p, err
}

// WithdrawGroupApplication 撤回入群申请。
func (c *Client) WithdrawGroupApplication(ctx context.Context, groupID string) error {
	return c.request(ctx, http.MethodDelete, "/client/v1/group-applications/"+url.PathEscape(groupID), nil, nil)
}

// ApplyJoinGroup 申请加入群。
func (c *Client) ApplyJoinGroup(ctx context.Context, groupID, reason string) error {
	body := map[string]any{}
	if reason != "" {
		body["reason"] = reason
	}
	return c.request(ctx, http.MethodPost, fmt.Sprintf("/client/v1/groups/%s/applications", url.PathEscape(groupID)), body, nil)
}

// GroupOnlineCount 群在线人数。
func (c *Client) GroupOnlineCount(ctx context.Context, groupID string) (int64, error) {
	var resp struct {
		OnlineCount int64 `json:"online_count"`
	}
	err := c.getJSON(ctx, fmt.Sprintf("/client/v1/groups/%s/online-count", url.PathEscape(groupID)), nil, &resp)
	return resp.OnlineCount, err
}

// JoinLink 入群链接。
func (c *Client) JoinLink(ctx context.Context, groupID string) (string, error) {
	var resp struct {
		Link string `json:"link"`
	}
	err := c.getJSON(ctx, fmt.Sprintf("/client/v1/groups/%s/join-link", url.PathEscape(groupID)), nil, &resp)
	return resp.Link, err
}

// JoinByLink 通过链接入群。
func (c *Client) JoinByLink(ctx context.Context, link string) (*GroupInfo, error) {
	var g GroupInfo
	err := c.request(ctx, http.MethodPost, "/client/v1/group-links/join", map[string]any{"link": link}, &g)
	return &g, err
}
