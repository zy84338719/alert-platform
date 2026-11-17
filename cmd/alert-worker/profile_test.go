package main

import "testing"

// P10-008: scaling processor replicas must not start singleton maintenance.
func TestParseProfileMatrix(t *testing.T) {
	cases := []struct {
		raw                        string
		processor, notifier, maint bool
	}{
		{"", true, true, true},
		{"all", true, true, true},
		{"processor", true, false, false},
		{"notifier", false, true, false},
		{"maintenance", false, false, true},
		{" Processor ", true, false, false},
	}
	for _, tc := range cases {
		p, err := parseProfile(tc.raw)
		if err != nil {
			t.Fatalf("%q: %v", tc.raw, err)
		}
		if p.Processor != tc.processor || p.Notifier != tc.notifier || p.Maintenance != tc.maint {
			t.Fatalf("%q -> %+v, want processor=%v notifier=%v maintenance=%v",
				tc.raw, p, tc.processor, tc.notifier, tc.maint)
		}
	}
	if _, err := parseProfile("processor,notifier"); err == nil {
		t.Fatal("typo/unknown profile accepted; it would silently run all singleton loops")
	}
}
