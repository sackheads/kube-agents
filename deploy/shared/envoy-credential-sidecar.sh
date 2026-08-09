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

# The baked config binds 127.0.0.1, which is the access control for as long as
# the broker is a sidecar in the agent's Pod. When the operator puts the broker
# in a Pod of its own the listener has to accept the Pod IP, and the config is
# baked into the image rather than rendered by the operator -- so the one line
# that has to differ is substituted here rather than by shipping two configs
# that would drift. The value is checked against a character class first: it
# reaches sed, and sed would happily accept a replacement carrying its own
# delimiter or newline.
envoy_config=/etc/envoy/envoy-credential-proxy.yaml
if [[ -n "${CREDENTIAL_PROXY_ENVOY_ADDRESS:-}" ]]; then
  if [[ ! "${CREDENTIAL_PROXY_ENVOY_ADDRESS}" =~ ^[0-9a-fA-F.:]+$ ]]; then
    echo "CREDENTIAL_PROXY_ENVOY_ADDRESS is not an IP address" >&2
    exit 1
  fi
  rendered=/tmp/envoy-credential-proxy.yaml
  sed "s|address: 127\.0\.0\.1|address: ${CREDENTIAL_PROXY_ENVOY_ADDRESS}|" \
    "${envoy_config}" >"${rendered}"
  # A substitution that silently matched nothing would leave the broker bound
  # to loopback in a Pod nothing else can reach, which reads as a hang.
  grep -q "address: ${CREDENTIAL_PROXY_ENVOY_ADDRESS}" "${rendered}"
  envoy_config="${rendered}"
fi

/usr/local/bin/envoy --config-path "${envoy_config}" --log-level info &
envoy_pid=$!

wait -n "${runtime_pid}" "${envoy_pid}"
