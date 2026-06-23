# gmountie-server Helm chart

Deploys the gMountie network-filesystem server (FUSE-over-gRPC): a
Deployment (Recreate — the data PVC is RWO), a ConfigMap for the server
config, a Secret for the auth credentials, a Service, and optionally a
PVC, an Ingress, and Gateway API exposure (GRPCRoute + BackendTLSPolicy).

## Installing

From the OCI registry (appVersion is stamped with the matching release):

```sh
helm install gmountie oci://ghcr.io/gmountie/charts/gmountie-server
```

**From a repo checkout, `--set image.tag` is required.** The in-repo
`Chart.yaml` carries `appVersion: dev`, which is never published as an
image tag — without an explicit tag the pod lands in ImagePullBackOff:

```sh
helm install gmountie deployments/charts/gmountie-server \
  --set image.tag=v0.13.0
```

## Credentials

The `config.auth` block is rendered into a chart-managed **Secret**
(never the ConfigMap) and concatenated with the rest of the config at
container start. The default values ship a published demo credential
(`demo`/`demo`); the chart **refuses to render** when that hash is still
present and the install is externally exposed (`service.type`
LoadBalancer/NodePort, `ingress.enabled`, `gateway.enabled`, or the
`exposedExternally` opt-in). Generate a real hash with `gmountie genpass`.

To keep credentials out of Helm values entirely, pre-create a Secret
with an `auth.yaml` key containing a top-level `auth:` block and set
`auth.existingSecret` (see `values.yaml` for the exact shape).

## Exposure

Three options (use one):

- **Service type** — `service.type: LoadBalancer` (or `NodePort`) for a
  raw L4 endpoint.
- **Ingress** — `ingress.enabled` for an nginx-style gRPC ingress.
- **Gateway API** — `gateway.enabled` attaches the ClusterIP Service to an
  existing Gateway via a **GRPCRoute** plus a **BackendTLSPolicy** that
  re-encrypts to the server's own TLS cert (gMountie refuses cleartext on a
  non-loopback bind). Set `gateway.parentRef` and `gateway.hostnames`, and
  provide the backend cert via `config.server.tls.cert_file/key_file` (mount
  it through `volumes`/`volumeMounts`, or let the chart issue it with
  `gateway.certificate.enabled`).

  Two things are **not** portable Gateway API and are handled separately:
  - gMountie's long-lived server streams (session keepalive, cache Subscribe)
    never complete, so a gateway's default per-request timeout resets them.
    GRPCRoute v1 has no timeout field. On **Envoy Gateway**, enable
    `gateway.envoy.backendTrafficPolicy` (renders a `BackendTrafficPolicy`
    with `requestTimeout: 0s`). Other implementations need their own equivalent.
  - HTTP/2 (h2) ALPN on the gateway **listener** is shared infrastructure for
    every route on it — configure it on the Gateway itself, not from this chart.

## Hardening

The pod runs non-root, drops all capabilities, and uses a **read-only root
filesystem** (`securityContext.readOnlyRootFilesystem`); HOME and the XDG base
dirs are redirected to a writable `state` emptyDir so the default self-signed
TLS path still works. The ServiceAccount token is **not** mounted by default
(`serviceAccount.automount: false`) — the server never calls the Kubernetes
API. Set `persistence.retain: true` to annotate the data PVC
`helm.sh/resource-policy: keep` so it survives `helm uninstall`.

## Ops endpoint (metrics/health)

`server.ops.addr` defaults to loopback: reachable with
`kubectl port-forward deploy/<name> 9090:9090` but not through the
Service (the chart omits the metrics Service/container ports in that
case). To scrape in-cluster, use the non-loopback `ops:` preset in
`values.yaml` — the server requires auth off loopback, and basic auth
off loopback additionally requires ops TLS.

## Identity / AssumeUser

The hardened default `securityContext` (non-root, drop ALL) disables
the identity feature: `system`/`static`/`passthrough` volume mapping
modes need the server to run as root with a small capability set. Use
the commented `securityContext` preset in `values.yaml` when a volume
configures one of those modes.

## Shutdown

`terminationGracePeriodSeconds` defaults to 45s — sized to the server's
own shutdown budget (30s drain + 5s session stop + 5s ops stop). Keep it
at least that large if overridden.
