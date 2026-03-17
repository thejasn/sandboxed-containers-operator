# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenShift Sandboxed Containers Operator manages the lifecycle (install/configure/update) of the Kata Containers runtime on OpenShift clusters. It provides workload isolation using lightweight VMs via the Kata Containers project.

## Common Commands

### Build
```bash
make build                    # Build manager and metrics-server binaries
make docker-build             # Build container image
```

### Test
```bash
make test                     # Run unit tests with envtest
go test ./controllers/... -run TestFunctionName  # Run a specific test
```

### Lint
```bash
make lint                     # Run golangci-lint
make lint-fix                 # Run golangci-lint with auto-fix
```

### Code Generation
```bash
make generate                 # Generate DeepCopy methods
make manifests                # Generate CRDs, RBAC, webhooks
make bundle                   # Generate OLM bundle
```

### Deploy (requires OpenShift cluster)
```bash
make install                  # Install CRDs
make deploy                   # Deploy operator to cluster
```

## Architecture

### CRDs and Controllers
- **KataConfig** (`api/v1/kataconfig_types.go`): Cluster-scoped CRD that triggers Kata runtime installation on worker nodes
- **KataConfigOpenShiftReconciler** (`controllers/openshift_controller.go`): Main controller that orchestrates installation via MachineConfig
- **SecretReconciler** (`controllers/credentials_controller.go`): Handles cloud provider credentials for PeerPods
- **RuntimeClassReconciler** (`controllers/runtimeclass_controller.go`): Creates RuntimeClasses (kata, kata-nvidia-gpu)

### Key Integration Points
- **Machine Config Operator (MCO)**: The operator creates MachineConfigs to configure CRI-O with Kata runtime on selected nodes
- **PeerPods/Confidential Containers**: Optional feature (`spec.enablePeerPods`) for running pods in remote VMs using cloud-api-adaptor
- **Node Feature Discovery (NFD)**: Optional node eligibility checking via `spec.checkNodeEligibility`

### Deployment Modes
The operator supports multiple deployment modes handled in `controllers/deployment_mode_handler.go`:
- Standard Kata installation via MCO extensions
- PeerPods mode for cloud-based confidential containers
- Layered image mode for custom kernel configurations

### Related Images
Controller environment variables define related images (RELATED_IMAGE_*) in `config/manager/manager.yaml`. When updating versions, tag image references with `## OSC_VERSION` comment.

## Project Structure
- `cmd/manager/`: Operator entrypoint
- `cmd/metrics/`: Prometheus metrics server
- `controllers/`: Reconciliation logic
- `api/v1/`: KataConfig CRD types and webhook
- `config/`: Kustomize manifests (CRDs, RBAC, manager deployment, samples)
- `config/peerpods/`: PeerPods/CAA related configurations
- `scripts/bump-osc-version.sh`: Script to bump operator version

## Build Requirements
- Go 1.22+
- Operator SDK v1.39.1
- Docker or Podman
- Access to registry.ci.openshift.org (requires token via `oc registry login`)

## Version Management
Version is defined in `Makefile` as `VERSION`. When bumping versions:
1. Update locations tagged with `## OSC_VERSION`
2. Update version labels in Dockerfiles
3. Update `olm.skipRange` in ClusterServiceVersion
4. Run `scripts/bump-osc-version.sh`
