package capio

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDeleteOrphanVolume(t *testing.T) {
	root := t.TempDir()
	m := &volumeManager{rootDir: root, volumes: map[string]*volume{}}

	// A volume left behind by a previous Controller: only its directory and
	// workflow config are still there. The workflow name is unique to this
	// test, so cleaning its shared memory can't touch anything real.
	id := "pvc-orphan"
	volDir := filepath.Join(root, id)
	if err := os.MkdirAll(volDir, 0o750); err != nil {
		t.Fatal(err)
	}
	cfg := `{"name": "capio-unit-test-orphan-7f3a", "IO_Graph": []}`
	if err := os.WriteFile(filepath.Join(volDir, "workflow.json"), []byte(cfg), 0o640); err != nil {
		t.Fatal(err)
	}

	if err := m.delete(id); err != nil {
		t.Fatalf("delete(%q): %v", id, err)
	}
	if _, err := os.Stat(volDir); !os.IsNotExist(err) {
		t.Errorf("orphan volume directory still exists (stat error: %v)", err)
	}

	// Deleting a volume that exists nowhere must succeed, as CSI requires.
	if err := m.delete("pvc-unknown"); err != nil {
		t.Errorf("delete of an unknown volume: %v", err)
	}
}

func TestWorkflowInUse(t *testing.T) {
	m := &volumeManager{volumes: map[string]*volume{
		"pvc-a": {id: "pvc-a", workflowName: "wf"},
	}}
	if !m.workflowInUse("wf") {
		t.Errorf("workflowInUse(%q) = false, want true", "wf")
	}
	if m.workflowInUse("wf2") {
		t.Errorf("workflowInUse(%q) = true, want false", "wf2")
	}
}
