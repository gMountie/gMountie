# gmountie-server Helm chart

Deploys the gMountie network-filesystem server (FUSE-over-gRPC): a
Deployment (Recreate — the data PVC is RWO), a ConfigMap for the server
config, a Secret for the auth credentials, a Service, and optionally a
PVC and an Ingress.

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
LoadBalancer/NodePort or `ingress.enabled`). Generate a real hash with
`gmountie genpass`.

To keep credentials out of Helm values entirely, pre-create a Secret
with an `auth.yaml` key containing a top-level `auth:` block and set
`auth.existingSecret` (see `values.yaml` for the exact shape).

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
