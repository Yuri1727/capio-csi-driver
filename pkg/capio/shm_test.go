package capio

import "testing"

func TestIsWorkflowSHM(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"wf", true},                    // canary
		{"wf_request_buffer", true},     // queue
		{"wf_first_elem_request", true}, // queue bookkeeping
		{"sem.wf_mutex_request", true},  // named semaphore
		{"sem.wf", true},
		{"wf2", false}, // a different workflow sharing the prefix
		{"wf2_request_buffer", false},
		{"sem.wf2_mutex_request", false},
		{"wfx", false},
		{"other_wf_request", false},
		{"capio_snapshot_123", false},
		{"sem.other", false},
	}
	for _, c := range cases {
		if got := isWorkflowSHM(c.name, "wf"); got != c.want {
			t.Errorf("isWorkflowSHM(%q, \"wf\") = %v, want %v", c.name, got, c.want)
		}
	}
}
