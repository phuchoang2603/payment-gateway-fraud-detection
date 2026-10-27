{{- define "metadata-integrations.runtime" -}}
env:
  - name: NATIVE_TIMEOUT_SECONDS
    value: {{ sub .Values.activeDeadlineSeconds 60 | quote }}
envFrom:
  - configMapRef:
      name: metadata-client
  - secretRef:
      name: {{ .Values.openmetadata.externalSecret.name }}
  {{- range .secretRefs }}
  - secretRef:
      name: {{ . }}
  {{- end }}
volumeMounts:
  - name: connectors
    mountPath: /etc/metadata
    readOnly: true
resources:
  requests: {cpu: 250m, memory: 1Gi}
  limits: {cpu: "2", memory: 2Gi}
{{- end -}}

{{- define "metadata-integrations.image" -}}
{{- $repository := required "image.repository is required" .repository -}}
{{- if .digest -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .digest) -}}
{{- fail "image.digest must be a sha256 digest" -}}
{{- end -}}
{{- printf "%s@%s" $repository .digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (.tag | default "latest") -}}
{{- end -}}
{{- end -}}
