package capio

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// shmDir is where POSIX shared memory objects and named semaphores live.
// Every container that calls cleanWorkflowSHM runs with hostIPC, so this is
// the node's /dev/shm, the one capio_server and its clients actually use.
const shmDir = "/dev/shm"

// CleanSHMArg is the hidden argv[1] that tells main() to run RunCleanSHM
// instead of the normal driver. It is the preStop hook of multi-node worker
// pods (see buildMPIJob): their capio_server's segments live on the worker's
// node, which neither the Controller nor, if no client pod ran there, any
// Node service instance can reach.
const CleanSHMArg = "__clean_capio_shm"

// cleanWorkflowSHM removes the shared memory objects and named semaphores
// that capio_server and its clients create for workflowName on this node.
//
// CAPIO names all of them after the workflow: the canary is exactly the
// workflow name, every queue, mutex and semaphore is "<workflow>_...", and
// named semaphores show up as "sem.<name>". Matching only those, rather than
// a bare "<workflow>*" prefix, keeps the cleanup of a workflow called "wf"
// from also wiping a live one called "wf2". Names are compared in Go instead
// of being expanded by a shell glob, so the workflow name never reaches a
// shell.
func cleanWorkflowSHM(workflowName string) error {
	if workflowName == "" {
		// An empty name would match every object starting with "_".
		return nil
	}
	entries, err := os.ReadDir(shmDir)
	if err != nil {
		return err
	}
	var errs []error
	for _, e := range entries {
		if !isWorkflowSHM(e.Name(), workflowName) {
			continue
		}
		if err := os.Remove(filepath.Join(shmDir, e.Name())); err != nil && !os.IsNotExist(err) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func isWorkflowSHM(name, workflowName string) bool {
	name = strings.TrimPrefix(name, "sem.")
	return name == workflowName || strings.HasPrefix(name, workflowName+"_")
}

// RunCleanSHM is the body of the CleanSHMArg entrypoint: args is the single
// workflow name whose objects must be removed from this node's /dev/shm.
func RunCleanSHM(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: %s <workflow name>", CleanSHMArg)
	}
	return cleanWorkflowSHM(args[0])
}
