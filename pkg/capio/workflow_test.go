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
