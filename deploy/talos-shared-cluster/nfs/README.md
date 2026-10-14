# In-namespace NFS server for CAPIO's multi-node discovery mechanism

CAPIO's MPI backend needs `CAPIO_DIR` to be genuinely shared storage across nodes for its
file-location bookkeeping (`files_location_<node>.txt`, written to the server's working
directory) — not for the data itself, which stays in RAM and travels over MPI, but for
nodes to discover which of them holds a given file.

## Why this exists instead of using the cluster's own RWX storage

The cluster's only RWX-capable StorageClass, `csi-manila-nfs`, is broken (OpenStack Keystone
endpoint misconfigured — HTTP instead of HTTPS, not something we can fix, out of scope of
this project). `csi-cinder-*` (RWO block storage) is **also broken**, same root cause — the
whole OpenStack integration on this cluster is down, not just Manila. Reported to the
cluster owners along with the other pre-existing issues found (see main project docs).

So this stands up a small NFS server **inside our own namespace**, using the CSI driver
`nfs.csi.k8s.io` that the cluster owners already have installed and working (confirmed
separately from the broken OpenStack drivers) — nothing here touches shared infrastructure.

## Two attempts before this worked

1. `itsthenetwork/nfs-server-alpine` (a kernel-nfsd-based image): failed with
   `rpc.nfsd: Unable to access /proc/fs/nfsd errno 2`. Talos nodes don't expose kernel NFS
   server support to a pod (consistent with Talos being an immutable, minimal OS — no
   `modprobe`, no host shell to check further).
2. `ghcr.io/nfs-ganesha/nfs-ganesha:V5-stable` (should be a userspace server, no kernel nfsd
   needed): image pull failed, 403 on the anonymous pull token — the image/tag doesn't
   actually exist at that reference.

**What works**: `docker.io/rmohr/nfs-ganesha:latest` — genuinely userspace (NFS-Ganesha),
ships its own `/opt/start_nfs.sh` that generates a working `ganesha.conf` from the
`EXPORT_PATH` env var. Verified end to end: wrote a file from a pod on `talos-worker-1`,
read the identical content from a pod on `talos-worker-2` through the resulting
`capio-nfs` StorageClass (small NFS close-to-open lag on the very first read after write —
expected NFSv4 behaviour, not a bug).

## What's here

- `02-nfs-server.yaml`: the nfs-ganesha Deployment + Service. Backing storage is an
  `emptyDir`, not a PVC — deliberately, since no PVC-backed storage works on this cluster
  right now, and the only things stored here are small bookkeeping text files regenerated
  each time a workflow runs, not data whose loss matters. If the cluster's OpenStack
  integration gets fixed later, swap the `emptyDir` for a `csi-cinder-work` PVC to survive
  pod restarts.
- `03-storageclass.yaml`: `capio-nfs` StorageClass, provisioner `nfs.csi.k8s.io`, pointing
  at the Service above. Any PVC using this StorageClass gets its own subdirectory on the
  export (confirmed via the PV's `subdir` volume attribute), genuinely shared RWX across
  nodes.
