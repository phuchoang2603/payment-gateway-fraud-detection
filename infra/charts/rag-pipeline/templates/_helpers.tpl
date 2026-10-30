{{- define "rag-pipeline.image" -}}
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required (printf "image tag missing for %s" $repository) (index .global.imageTags $repository) -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{- define "rag-pipeline.podSecurity" -}}
runAsNonRoot: true
runAsUser: {{ .Values.uid }}
runAsGroup: {{ .Values.uid }}
fsGroup: {{ .Values.uid }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "rag-pipeline.containerSecurity" -}}
allowPrivilegeEscalation: false
runAsNonRoot: true
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "rag-pipeline.dbEnv" -}}
- name: RAG_DB_HOST
  value: {{ .root.Values.database.name }}-rw
- name: RAG_DB_NAME
  value: {{ .root.Values.database.dbname }}
- name: RAG_DB_USER
  valueFrom:
    secretKeyRef: {name: {{ .secret }}, key: username}
- name: RAG_DB_PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ .secret }}, key: password}
{{- end -}}

{{- define "rag-pipeline.gatewayEnv" -}}
- name: GATEWAY_URL
  value: {{ .root.Values.gateway.url }}
- name: GATEWAY_API_KEY
  valueFrom:
    secretKeyRef: {name: {{ .secret }}, key: key}
- name: EMBEDDING_MODEL
  value: {{ .root.Values.embedding.model }}
{{- end -}}

{{- define "rag-pipeline.storageEnv" -}}
- name: AWS_REGION
  value: {{ .Values.storage.region }}
- name: S3_ENDPOINT
  value: {{ .Values.storage.endpoint }}
- name: S3_PATH_STYLE
  value: {{ .Values.storage.pathStyle | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef: {name: rag-storage, key: AWS_ACCESS_KEY_ID}
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: {name: rag-storage, key: AWS_SECRET_ACCESS_KEY}
- name: ICEBERG_REST_URI
  value: {{ .Values.catalog.restUri }}
- name: ICEBERG_WAREHOUSE
  value: {{ .Values.catalog.warehouse }}
- name: ICEBERG_CLIENT_ID
  value: {{ .Values.catalog.clientId }}
- name: ICEBERG_OAUTH2_URI
  value: {{ .Values.catalog.oauth2Uri }}
- name: ICEBERG_CLIENT_SECRET
  valueFrom:
    secretKeyRef: {name: rag-iceberg, key: ICEBERG_CLIENT_SECRET}
{{- end -}}

{{- define "rag-pipeline.metadataEnv" -}}
- name: OPENMETADATA_URL
  value: {{ .Values.openmetadata.url }}
- name: OPENMETADATA_TOKEN
  valueFrom:
    secretKeyRef: {name: rag-openmetadata, key: OPENMETADATA_TOKEN}
- name: CLICKHOUSE_DATABASE
  value: {{ .Values.openmetadata.clickhouseDatabase }}
- name: DATA_PRODUCTS_ROOT
  value: /contracts/data-products
- name: HOME
  value: /tmp
{{- end -}}
