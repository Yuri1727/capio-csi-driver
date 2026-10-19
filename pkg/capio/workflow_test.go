package capio

import "testing"

func TestValidateCapioDir(t *testing.T) {
	cases := []struct {
		dir string
		ok  bool
	}{
		{"/capio", true},
		{"/data/sim", true},
		{"/tmp/wf1", true},
		{"/var/data", true},
		{"/library", true}, // only /lib and paths under it are reserved
		{"/shared-data", true},
		{"capio", false}, // relative
		{"./capio", false},
		{"/", false},
		{"/capio/", false}, // not clean
		{"/data/../etc", false},
		{"//capio", false},
		{"/etc", false},
		{"/etc/capio", false},
		{"/usr/local/capio", false},
		{"/proc/1", false},
		{"/dev/shm", false},
		{"/root/.ssh", false},
		{"/var/lib/capio/volumes", false},
		{"/csi", false},
		{"/shared", false},
		{"/shared/x", false},
	}
	for _, c := range cases {
		err := validateCapioDir(c.dir)
		if (err == nil) != c.ok {
			t.Errorf("validateCapioDir(%q) error = %v, want ok=%v", c.dir, err, c.ok)
		}
	}
}

func TestParseVolumeParamsCapioDir(t *testing.T) {
	base := map[string]string{
		"workflowName": "wf",
		"producerStep": "writer",
		"consumerStep": "reader1",
		"files":        "shared.dat",
	}

	p, err := parseVolumeParams(base)
	if err != nil {
		t.Fatalf("parseVolumeParams without capioDir: %v", err)
	}
	if p.capioDir != DefaultCapioDir {
		t.Errorf("default capioDir = %q, want %q", p.capioDir, DefaultCapioDir)
	}

	withDir := map[string]string{"capioDir": " /data/sim "}
	for k, v := range base {
		withDir[k] = v
	}
	p, err = parseVolumeParams(withDir)
	if err != nil {
		t.Fatalf("parseVolumeParams with capioDir: %v", err)
	}
	if p.capioDir != "/data/sim" {
		t.Errorf("capioDir = %q, want %q", p.capioDir, "/data/sim")
	}

	withDir["capioDir"] = "/etc"
	if _, err := parseVolumeParams(withDir); err == nil {
		t.Errorf("parseVolumeParams accepted capioDir %q", "/etc")
	}
}

func TestBuildMPIJobCapioDir(t *testing.T) {
	params := &volumeParams{workflowName: "wf", capioDir: "/data/sim", nodeCount: 2}
	job := buildMPIJob("ns", "mnode-pvc-1", "image", "secret", params)

	for role, spec := range job.Spec.MPIReplicaSpecs {
		c := spec.Template.Spec.Containers[0]
		found := false
		for _, e := range c.Env {
			if e.Name == "CAPIO_DIR" {
				found = true
				if e.Value != "/data/sim" {
					t.Errorf("%s: CAPIO_DIR = %q, want %q", role, e.Value, "/data/sim")
				}
			}
		}
		if !found {
			t.Errorf("%s: CAPIO_DIR not set", role)
		}
	}

	mounted := false
	for _, m := range job.Spec.MPIReplicaSpecs["Worker"].Template.Spec.Containers[0].VolumeMounts {
		if m.Name == "capio-dir" {
			mounted = m.MountPath == "/data/sim"
		}
	}
	if !mounted {
		t.Errorf("worker does not mount the capio-dir volume at %q", "/data/sim")
	}
}

func TestParseVolumeParamsRules(t *testing.T) {
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{
			"workflowName": "wf",
			"producerStep": "writer",
			"consumerStep": "reader1",
			"files":        "shared.dat",
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}

	p, err := parseVolumeParams(with(nil))
	if err != nil {
		t.Fatalf("parseVolumeParams with defaults: %v", err)
	}
	if p.commitRule != "on_close" || p.fireRule != "no_update" {
		t.Errorf("defaults = (%q, %q), want (on_close, no_update)", p.commitRule, p.fireRule)
	}

	cases := []struct {
		key, value string
		ok         bool
	}{
		{"commitRule", "on_close", true},
		{"commitRule", "on_close:2", true},
		{"commitRule", "on_close:10", true},
		{"commitRule", "on_termination", true},
		{"commitRule", "on_close:0", false},
		{"commitRule", "on_close:", false},
		{"commitRule", "on_file", false},
		{"commitRule", "on_n_files", false},
		{"commitRule", "onclose", false},
		{"fireRule", "no_update", true},
		{"fireRule", "update", true},
		{"fireRule", "streaming", false},
		{"fireRule", "NO_UPDATE", false},
		{"files", "a.dat,b.dat", true},
		{"files", "out/result.dat", true},
		{"files", "/capio/shared.dat", false},
		{"files", "../shared.dat", false},
		{"files", "a/../../b.dat", false},
		{"files", "./shared.dat", false},
		{"files", "..", false},
	}
	for _, c := range cases {
		_, err := parseVolumeParams(with(map[string]string{c.key: c.value}))
		if (err == nil) != c.ok {
			t.Errorf("%s=%q: error = %v, want ok=%v", c.key, c.value, err, c.ok)
		}
	}
}

func TestBuildWorkflowConfig(t *testing.T) {
	p := &volumeParams{
		workflowName:  "wf",
		producerSteps: []string{"writer"},
		consumerSteps: []string{"reader1", "reader2"},
		files:         []string{"shared.dat", "out/result.dat"},
		commitRule:    "on_termination",
		fireRule:      "no_update",
		capioDir:      "/data/sim",
	}
	cfg := buildWorkflowConfig(p)
	want := []string{"/data/sim/shared.dat", "/data/sim/out/result.dat"}

	equal := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	if cfg.Name != "wf" || len(cfg.IOGraph) != 3 {
		t.Fatalf("got name %q with %d steps, want wf with 3", cfg.Name, len(cfg.IOGraph))
	}
	writer := cfg.IOGraph[0]
	if !equal(writer.OutputStream, want) {
		t.Errorf("writer output_stream = %v, want %v", writer.OutputStream, want)
	}
	if len(writer.Streaming) != len(want) {
		t.Fatalf("writer has %d streaming rules, want %d", len(writer.Streaming), len(want))
	}
	for i, r := range writer.Streaming {
		if !equal(r.Name, want[i:i+1]) || r.Committed != "on_termination" || r.Mode != "no_update" {
			t.Errorf("streaming rule %d = %+v", i, r)
		}
	}
	for _, step := range cfg.IOGraph[1:] {
		if !equal(step.InputStream, want) {
			t.Errorf("%s input_stream = %v, want %v", step.Name, step.InputStream, want)
		}
	}
}
