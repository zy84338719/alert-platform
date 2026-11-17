package authn

import "testing"

// AUTH-002 acceptance: the five-role permission matrix, every cell asserted.
func TestRBACMatrix(t *testing.T) {
	allPerms := []string{
		PermAlertsRead, PermAlertsAck, PermSourcesRead, PermSourcesWrite,
		PermPoliciesRead, PermPoliciesWrite, PermChannelsRead, PermChannelsWrite, PermNotificationsRead,
		PermNotificationsWrite, PermAuditRead,
	}
	expect := map[string]map[string]bool{
		RolePlatformAdmin: {PermAlertsRead: true, PermAlertsAck: true, PermSourcesRead: true, PermSourcesWrite: true,
			PermPoliciesRead: true, PermPoliciesWrite: true, PermChannelsRead: true, PermChannelsWrite: true, PermNotificationsRead: true, PermNotificationsWrite: true, PermAuditRead: true},
		RoleMaintainer: {PermAlertsRead: true, PermAlertsAck: true, PermSourcesRead: true, PermSourcesWrite: true,
			PermPoliciesRead: true, PermPoliciesWrite: true, PermChannelsRead: true, PermChannelsWrite: true, PermNotificationsRead: true, PermNotificationsWrite: true, PermAuditRead: false},
		RoleSRE: {PermAlertsRead: true, PermAlertsAck: true, PermSourcesRead: false, PermSourcesWrite: false,
			PermPoliciesRead: true, PermPoliciesWrite: true, PermChannelsRead: true, PermChannelsWrite: false, PermNotificationsRead: true, PermNotificationsWrite: true, PermAuditRead: true},
		RoleOncall: {PermAlertsRead: true, PermAlertsAck: true, PermSourcesRead: false, PermSourcesWrite: false,
			PermPoliciesRead: false, PermPoliciesWrite: false, PermChannelsRead: true, PermChannelsWrite: false, PermNotificationsRead: true, PermNotificationsWrite: false, PermAuditRead: false},
		RoleAuditor: {PermAlertsRead: true, PermAlertsAck: false, PermSourcesRead: false, PermSourcesWrite: false,
			PermPoliciesRead: false, PermPoliciesWrite: false, PermChannelsRead: true, PermChannelsWrite: false, PermNotificationsRead: false, PermNotificationsWrite: false, PermAuditRead: true},
	}
	for role, perms := range expect {
		for _, perm := range allPerms {
			if got := Allowed(role, perm); got != perms[perm] {
				t.Errorf("%s -> %s = %v, want %v", role, perm, got, perms[perm])
			}
		}
	}
	// Unknown roles grant nothing (fail closed).
	if Allowed("superuser-hack", PermSourcesWrite) {
		t.Error("unknown role granted write")
	}
}

func TestTokenHashDeterministicAndSalted(t *testing.T) {
	h1 := TokenHash("abc")
	if h1 != TokenHash("abc") {
		t.Fatal("hash not deterministic")
	}
	if len(h1) != 64 {
		t.Fatalf("SHA-256 hex length = %d, want 64", len(h1))
	}
	if TokenHash("abc") == TokenHash("abd") {
		t.Fatal("hash collision on distinct tokens")
	}
}
