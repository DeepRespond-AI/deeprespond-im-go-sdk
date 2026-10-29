package im

import (
	"encoding/json"
	"testing"
)

func TestGroupMemberResultsKeepPerMemberFailures(t *testing.T) {
	var result struct {
		Results []GroupMemberResult `json:"results"`
	}
	err := json.Unmarshal([]byte(`{"results":[{"username":"alice","status":"joined"},{"username":"bob","code":"rate_limited","message":"冷却中","details":{"reason":"declined_cooldown"}}]}`), &result)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 2 || result.Results[0].Status != "joined" || result.Results[1].Code != "rate_limited" || result.Results[1].Message != "冷却中" {
		t.Fatalf("lost member outcomes: %+v", result.Results)
	}
	var details map[string]any
	if err := json.Unmarshal(result.Results[1].Details, &details); err != nil || details["reason"] != "declined_cooldown" {
		t.Fatalf("lost cooldown reason: %s, %v", result.Results[1].Details, err)
	}
}
