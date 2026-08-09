#!/usr/bin/env bash
set -euo pipefail

# The sandbox runs as a different user (see the UID constants in the operator's
# platformagent_manifests.go) and shares only the agent PVC with this container.
# Proxied commands run here but write there — a clone, a commit, a kubeconfig pin
# in a profile home — and the sandbox has to be able to change what they leave
# behind. The shared fsGroup gives it the group; this gives the group write.
# Credential state lives on this container's own emptyDir volumes, which nothing
# else mounts, so the wider mode does not widen who can read a credential.
umask 0002

runtime_pid=""
envoy_pid=""

terminate() {
  [[ -z "${runtime_pid}" ]] || kill "${runtime_pid}" 2>/dev/null || true
  [[ -z "${envoy_pid}" ]] || kill "${envoy_pid}" 2>/dev/null || true
}
trap terminate EXIT INT TERM

/opt/hermes/.venv/bin/python3 /opt/defaults/scripts/credential_proxy.py &
runtime_pid=$!

/usr/local/bin/envoy --config-path /etc/envoy/envoy-credential-proxy.yaml --log-level info &
envoy_pid=$!

wait -n "${runtime_pid}" "${envoy_pid}"
