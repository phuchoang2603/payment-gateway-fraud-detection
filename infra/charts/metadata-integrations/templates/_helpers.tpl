{{- define "metadata-integrations.runtime" -}}
env:
  - name: NATIVE_TIMEOUT_SECONDS
    value: {{ sub .Values.activeDeadlineSeconds 60 | quote }}
envFrom:
  - configMapRef:
      name: metadata-client
  - secretRef:
      name: openmetadata
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
