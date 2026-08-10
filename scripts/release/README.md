# Release Candidate Automation Scripts

This directory contains executable scripts supporting the Release Candidate (RC) end-to-end automation pipeline.

## Release note: `PLATFORM_AGENT_PERMISSION_SET=gke-admin` now fails the deploy

**Action required before the next RC deploy** for any GitHub environment whose
`PLATFORM_AGENT_PERMISSION_SET` variable is set to `gke-admin`. That value has been removed and
provisioning now exits non-zero on it, so the deploy hard-fails at
`provision_rc_environment.sh` rather than falling back to a default.

`rc-deploy-environment.yml` forwards `vars.PLATFORM_AGENT_PERMISSION_SET` verbatim to both
`validate_and_log_deploy_summary.sh` and `provision_rc_environment.sh`, and a variable that is
already set is passed straight through the provisioner's prompt-or-default logic. The failure is
loud and fail-closed by design — `roles/container.admin` authorizes the agent through IAM
regardless of its Kubernetes RBAC, and its `container.clusters.impersonate` permission cannot be
scoped by IAM — but nothing warns you ahead of the run.

Fix it by editing the environment variable to `read-only`, or to `custom` with
`PLATFORM_AGENT_CUSTOM_ROLES` naming every role, if you accept that risk explicitly. The reasoning
is on the site's [Security & IAM](../../docs/site/src/content/docs/reference/security-and-iam.md)
page under "Why there is no `gke-admin` set".

## Overview of Scripts

- `common.sh`: Shared helper functions for Git operations and automated bot tagging (`ensure_git_tag`).
- `resolve_rc_tag.sh`: Validates candidate commit SHAs, resolves input tags/commit inputs, and sets workflow step outputs.
- `verify_candidate_images.sh`: Verifies that prebuilt container images (`k8s-operator`, `platform-agent`) exist in GHCR for the target candidate SHA.
- `create_release_tag.sh`: Creates and pushes candidate release tags (`rc_YYMMDDHHMM_<short_sha>`) safely and idempotently.
- `validate_and_log_deploy_summary.sh`: Validates required environment variables and secrets, then logs a formatted deployment matrix and GCP cluster target overview for auditing before provisioning.
- `provision_rc_environment.sh`: Orchestrates cluster teardown and fresh provisioning against the dedicated RC GCP project.
- `tag_validated_release.sh`: Attaches the `*_validated` tag to a candidate commit upon 100% test pass.

## Workflow Mapping

These modular scripts back the corresponding child workflows in `.github/workflows/`:

| GitHub Workflow                                  | Release Step                            | Executed Scripts                                                                         |
| ------------------------------------------------ | --------------------------------------- | ---------------------------------------------------------------------------------------- |
| `rc-create-tag.yml`                              | Step 1 - Create Candidate Tag           | `resolve_rc_tag.sh`, `verify_candidate_images.sh`, `create_release_tag.sh`               |
| `rc-deploy-environment.yml`                      | Step 2 - Deploy Environment             | `resolve_rc_tag.sh`, `validate_and_log_deploy_summary.sh`, `provision_rc_environment.sh` |
| `e2e-gchat-test.yml` / `rc-release-pipeline.yml` | Step 3 - GKE Readiness & E2E Validation | `install_e2e_deps.sh`, `wait_for_gke_readiness.sh`, `execute_e2e_tests.sh`               |
| `rc-tag-validated.yml`                           | Step 4 - Validate Candidate Commit      | `resolve_rc_tag.sh`, `tag_validated_release.sh`                                          |
