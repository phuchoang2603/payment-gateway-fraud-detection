{{- define "metadata-integrations.runtime" -}}
env:
  - name: METADATA_STAGE
    value: {{ .stage | quote }}
  {{- with .connector }}
  - name: METADATA_CONNECTOR
    value: {{ . | quote }}
  {{- end }}
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
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required (printf "image tag missing for %s" $repository) (index .global.imageTags $repository) -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
