{{- define "llm-platform.image" -}}
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required (printf "image tag missing for %s" $repository) (index .global.imageTags $repository) -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

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

{{- define "llm-platform.vllmEnv" -}}
- name: USER
  value: vllm
- name: HF_HUB_OFFLINE
  value: "1"
- name: VLLM_CACHE_ROOT
  value: /home/.cache/vllm
{{- end -}}

{{- define "llm-platform.modelURI" -}}
{{- printf "pvc://llm-models/%s" .repo -}}
{{- end -}}
