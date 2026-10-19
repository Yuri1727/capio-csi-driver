package capio

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

// workflowConfig mirrors the CAPIO-CL JSON schema (see capio/schema/v1.1.json
// in the CAPIO repository): a named workflow made of steps, each one
// declaring which files it produces (output_stream) and/or consumes
// (input_stream), plus the streaming semantics for produced files.
type workflowConfig struct {
	Name    string         `json:"name"`
	IOGraph []workflowStep `json:"IO_Graph"`
}

type workflowStep struct {
	Name         string          `json:"name"`
	InputStream  []string        `json:"input_stream"`
	OutputStream []string        `json:"output_stream"`
	Streaming    []streamingRule `json:"streaming,omitempty"`
}

type streamingRule struct {
	Name      []string `json:"name"`
	Committed string   `json:"committed"`
}

// volumeParams is what a StorageClass is expected to set in `parameters`
// for a CAPIO-backed volume. producerSteps/consumerSteps are lists of step
// names: every pod that wants to write sets CAPIO_APP_NAME to one of
// producerSteps, every pod that wants to read to one of consumerSteps.
//
// CAPIO requires each concurrent reader (or writer) of the same file to use
// a distinct step name — confirmed by testing: two processes sharing one
// consumer step name for the same on_close file left one of them blocked
// forever, since only one waiter gets woken up when the file commits. So a
// StorageClass has to declare up front how many concurrent
// producers/consumers it expects, one name each.
type volumeParams struct {
	workflowName  string
	producerSteps []string
	consumerSteps []string
	files         []string
	commitRule    string

	// capioDir is the CAPIO_DIR every participant of this volume uses: the
	// capio_server instance(s) and every client pod, which must mount the
	// volume at this same path and set CAPIO_DIR to it. Defaults to
	// DefaultCapioDir when the StorageClass doesn't set it.
	capioDir string

	// nodeCount is the number of MPI ranks (one per physical node) CAPIO's
	// capio_server needs for this volume. 1 (the default when the parameter
	// is absent) keeps the existing single-node behavior: capio_server runs
	// as a local subprocess of the Controller, backend "none". Anything
	// greater switches to the MPIJob-backed path: an MPI Operator job with
	// one capio_server per node, backend "mpi", one rank each.
	nodeCount int
}

const defaultCommitRule = "on_close"
const defaultNodeCount = 1

// DefaultCapioDir is the CAPIO_DIR used when a StorageClass doesn't set
// capioDir, so manifests written before the parameter existed keep working.
const DefaultCapioDir = "/capio"

// reservedCapioDirs are paths capioDir may not be, nor be inside of. The
// server side mounts the volume at capioDir inside the driver's container
// (single-node) or a worker pod (multi-node), so it must not shadow system
// directories those processes need, or paths the driver itself uses.
var reservedCapioDirs = []string{
	"/bin", "/boot", "/dev", "/etc", "/lib", "/lib64", "/proc", "/root",
	"/run", "/sbin", "/sys", "/usr",
	"/csi", "/var/lib/capio", sharedDiscoveryMountPath,
}

// validateCapioDir checks that dir is usable as CAPIO_DIR: an absolute,
// already-clean path, other than "/" and outside reservedCapioDirs.
func validateCapioDir(dir string) error {
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%q is not an absolute path", dir)
	}
	if filepath.Clean(dir) != dir {
		return fmt.Errorf("%q is not a clean path (expected %q)", dir, filepath.Clean(dir))
	}
	if dir == "/" {
		return fmt.Errorf("%q cannot be the root directory", dir)
	}
	for _, r := range reservedCapioDirs {
		if dir == r || strings.HasPrefix(dir, r+"/") {
			return fmt.Errorf("%q is reserved (%s)", dir, r)
		}
	}
	return nil
}

func splitCommaList(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

// parseVolumeParams validates the raw StorageClass parameters coming in a
// CreateVolumeRequest and turns them into a volumeParams.
func parseVolumeParams(params map[string]string) (*volumeParams, error) {
	get := func(key string) string { return strings.TrimSpace(params[key]) }

	p := &volumeParams{
		workflowName:  get("workflowName"),
		producerSteps: splitCommaList(get("producerStep")),
		consumerSteps: splitCommaList(get("consumerStep")),
		files:         splitCommaList(get("files")),
		commitRule:    get("commitRule"),
	}

	if p.workflowName == "" {
		return nil, fmt.Errorf("missing required StorageClass parameter %q", "workflowName")
	}
	if len(p.producerSteps) == 0 {
		return nil, fmt.Errorf("missing required StorageClass parameter %q", "producerStep")
	}
	if len(p.consumerSteps) == 0 {
		return nil, fmt.Errorf("missing required StorageClass parameter %q", "consumerStep")
	}
	if len(p.files) == 0 {
		return nil, fmt.Errorf("missing required StorageClass parameter %q", "files")
	}
	if p.commitRule == "" {
		p.commitRule = defaultCommitRule
	}

	p.capioDir = get("capioDir")
	if p.capioDir == "" {
		p.capioDir = DefaultCapioDir
	}
	if err := validateCapioDir(p.capioDir); err != nil {
		return nil, fmt.Errorf("invalid StorageClass parameter %q: %v", "capioDir", err)
	}

	p.nodeCount = defaultNodeCount
	if raw := get("nodeCount"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("invalid StorageClass parameter %q: %q is not a positive integer", "nodeCount", raw)
		}
		p.nodeCount = n
	}

	return p, nil
}

// buildWorkflowConfig turns volumeParams into the CAPIO-CL workflow used
// for the volume: one step per declared producer, one per declared
// consumer, all producing/consuming the same set of files.
func buildWorkflowConfig(p *volumeParams) workflowConfig {
	streaming := make([]streamingRule, 0, len(p.files))
	for _, f := range p.files {
		streaming = append(streaming, streamingRule{
			Name:      []string{f},
			Committed: p.commitRule,
		})
	}

	steps := make([]workflowStep, 0, len(p.producerSteps)+len(p.consumerSteps))
	for _, name := range p.producerSteps {
		steps = append(steps, workflowStep{
			Name:         name,
			InputStream:  []string{},
			OutputStream: p.files,
			Streaming:    streaming,
		})
	}
	for _, name := range p.consumerSteps {
		steps = append(steps, workflowStep{
			Name:         name,
			InputStream:  p.files,
			OutputStream: []string{},
		})
	}

	return workflowConfig{
		Name:    p.workflowName,
		IOGraph: steps,
	}
}

func (w workflowConfig) marshalIndent() ([]byte, error) {
	return json.MarshalIndent(w, "", "  ")
}
