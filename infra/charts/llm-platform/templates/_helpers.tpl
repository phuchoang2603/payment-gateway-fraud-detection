{{- define "llm-platform.principal" -}}
{{- printf "cluster.local/ns/%s/sa/%s" .namespace .name -}}
{{- end -}}

{{- define "llm-platform.gpuPlacement" -}}
{{- with .Values.gpu.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.gpu.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "llm-platform.podSecurity" -}}
runAsNonRoot: true
runAsUser: {{ .uid }}
runAsGroup: {{ .uid }}
fsGroup: {{ .uid }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "llm-platform.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "llm-platform.modelURI" -}}
{{- printf "s3://%s/%s/%s/%s" .store.bucket .store.prefix .model.repo .model.revision -}}
{{- end -}}

{{- define "llm-platform.modelStoreEnv" -}}
- name: AWS_ENDPOINT_URL
  value: {{ .endpoint }}
- name: AWS_DEFAULT_REGION
  value: us-east-1
- name: AWS_EC2_METADATA_DISABLED
  value: "true"
- name: RUNAI_STREAMER_S3_USE_VIRTUAL_ADDRESSING
  value: {{ ternary "1" "0" .virtualAddressing | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef: {name: {{ .credentialsSecret }}, key: accesskey}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: {name: {{ .credentialsSecret }}, key: secretkey}
{{- end -}}
