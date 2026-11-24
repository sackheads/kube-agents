package main

import (
	"fmt"
	"io"
	"log/slog"

	"github.com/gke-labs/kube-agents/a2a/lib/persona"
	workeradapter "github.com/gke-labs/kube-agents/a2a/worker-adapter"
)

// The env names the Antigravity CLI reads for gateway mode (#6714).
const (
	agyGatewayURLEnv    = "AGY_LLM_GATEWAY_URL"
	agyGatewayModelsEnv = "AGY_LLM_GATEWAY_MODELS"
	agyGatewayCACertEnv = "AGY_LLM_GATEWAY_CA_CERT"
)

// agyWithheldEnv is what agyBuilder.env drops from the pod env.
//
//   - The gateway trio: agy's gateway is always the adapter's loopback proxy
//     (startAgyGateway), so a pod or image value can't point agy past the
//     proxy's path allowlist, its caps and the gateway's redaction. Loopback
//     is plain http, so there is no CA cert either.
//   - The model credentials and the direct gateway URL: the caller key lives
//     in the adapter, never in agy's process env, so neither agy nor a shell
//     the model runs can call the gateway directly and skip the proxy.
var agyWithheldEnv = []string{
	agyGatewayURLEnv, agyGatewayModelsEnv, agyGatewayCACertEnv,
	modelAPIKeyEnv, modelAuthTokenEnv, modelBaseURLEnv,
}

// gatewayProxyStarter is workeradapter.StartGatewayProxy; tests replace it.
type gatewayProxyStarter func(gateway string, log *slog.Logger) (*workeradapter.GatewayProxy, error)

// startAgyGateway starts the loopback proxy an agy worker's model calls go
// through and returns env with agy's gateway pointed at it, plus the proxy
// to close at worker exit. Any other harness gets env back unchanged and a
// nil closer.
//
// One proxy per worker: agy is respawned per turn inside workeradapter.Run
// (#6710) with the same env, so every agy process uses this one proxy, which
// lives in the adapter's own process.
//
// The gateway is the pod's ANTHROPIC_BASE_URL, else the install's
// inference-gateway: the same gateway, by the same rule, as Claude's. The
// model is A2A_MODEL, else model-default, the name Claude's argv uses.
func startAgyGateway(harness persona.Harness, env []string, getenv func(string) string, start gatewayProxyStarter, log *slog.Logger) ([]string, io.Closer, error) {
	if harness != persona.Antigravity {
		return env, nil, nil
	}
	gateway := getenv(modelBaseURLEnv)
	if gateway == "" {
		gateway = defaultModelBaseURL
	}
	model := getenv("A2A_MODEL")
	if model == "" {
		model = defaultModel
	}
	proxy, err := start(gateway, log)
	if err != nil {
		return nil, nil, fmt.Errorf("agy's gateway proxy: %w", err)
	}
	log.Info("agy's model calls go through the loopback gateway proxy", "proxy", proxy.URL, "gateway", gateway, "model", model)
	// agyBuilder.env already dropped any pod value for these two.
	out := append(append([]string(nil), env...),
		agyGatewayURLEnv+"="+proxy.URL,
		agyGatewayModelsEnv+"="+model,
	)
	return out, proxy, nil
}
