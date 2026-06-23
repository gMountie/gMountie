{{/*
Expand the name of the chart.
*/}}
{{- define "gmountie-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "gmountie-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "gmountie-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "gmountie-server.labels" -}}
helm.sh/chart: {{ include "gmountie-server.chart" . }}
{{ include "gmountie-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "gmountie-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gmountie-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "gmountie-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "gmountie-server.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Effective server.ops.addr (falls back to the server default).
*/}}
{{- define "gmountie-server.opsAddr" -}}
{{- dig "server" "ops" "addr" "127.0.0.1:9090" .Values.config -}}
{{- end }}

{{/*
Non-empty ("true") when the ops endpoint binds a non-loopback address —
only then is there anything for a containerPort / Service port to route to.
*/}}
{{- define "gmountie-server.opsExposed" -}}
{{- $addr := include "gmountie-server.opsAddr" . -}}
{{- if not (or (hasPrefix "127." $addr) (hasPrefix "localhost" $addr) (hasPrefix "[::1]" $addr)) -}}
true
{{- end -}}
{{- end }}

{{/*
Port component of server.ops.addr.
*/}}
{{- define "gmountie-server.opsPort" -}}
{{- include "gmountie-server.opsAddr" . | splitList ":" | last -}}
{{- end }}

{{/*
Name of the Secret carrying the auth config block.
*/}}
{{- define "gmountie-server.authSecretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- include "gmountie-server.fullname" . }}-auth
{{- end }}
{{- end }}

{{/*
Non-empty ("true") when this install is reachable from outside the cluster —
via a LoadBalancer/NodePort Service, an enabled ingress, an enabled Gateway
route, or the explicit exposedExternally opt-in. Drives the demo-credential
refusal so a chart-rendered external entrypoint can't ship the demo password.
*/}}
{{- define "gmountie-server.exposedExternally" -}}
{{- if or (eq .Values.service.type "LoadBalancer") (eq .Values.service.type "NodePort") .Values.ingress.enabled .Values.gateway.enabled .Values.exposedExternally -}}
true
{{- end -}}
{{- end }}

{{/*
The PUBLISHED argon2id hash of the demo password ("demo") that values.yaml
ships. Installs that expose the server externally must not keep it.
*/}}
{{- define "gmountie-server.demoHash" -}}
$argon2id$v=19$m=65536,t=3,p=4$Zu3uhLZjJNWQzIBQNevo3w$iqBrNtSWBuE6I7dCzQNRHZtPd/zUA8tTG3KnpyQAnew
{{- end }}
