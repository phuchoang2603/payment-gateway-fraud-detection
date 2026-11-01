{{- define "fraud-features.image" -}}
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required (printf "image tag missing for %s" $repository) (index .global.imageTags $repository) -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{- define "fraud-features.feastEnv" -}}
- name: FEAST_S3_ENDPOINT_URL
  value: {{ .Values.storage.endpoint | quote }}
- name: FEAST_REGISTRY
  value: {{ .Values.feast.registry | quote }}
- name: FEAST_STREAM_EPOCHS
  value: {{ .Values.feast.epochs | quote }}
- name: FEAST_REDIS_ADDR
  value: {{ printf "%s:6379" .Values.redis.name | quote }}
- name: FEAST_REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redis.password.secretName }}
      key: {{ .Values.redis.password.secretKey }}
- name: AWS_REGION
  value: {{ .Values.storage.region | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.storage.endpoint | quote }}
- name: S3_PATH_STYLE
  value: {{ .Values.storage.pathStyle | quote }}
{{- range list "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" }}
- name: {{ . }}
  valueFrom:
    secretKeyRef:
      name: {{ $.Values.storage.secretName }}
      key: {{ . }}
{{- end }}
- name: ICEBERG_REST_URI
  value: {{ .Values.catalog.uri | quote }}
- name: ICEBERG_WAREHOUSE
  value: {{ .Values.catalog.warehouse | quote }}
- name: ICEBERG_CLIENT_ID
  value: {{ .Values.catalog.clientId | quote }}
- name: ICEBERG_OAUTH2_URI
  value: {{ .Values.catalog.oauth2Uri | quote }}
- name: ICEBERG_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.catalog.secretName }}
      key: ICEBERG_CLIENT_SECRET
{{- end -}}
