{{- define "data-pipelines.image" -}}
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required (printf "image tag missing for %s" $repository) (index .global.imageTags $repository) -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}

{{- define "data-pipelines.generator.step" -}}
{{- $values := .root.Values -}}
{{- $image := include "data-pipelines.image" (dict "image" $values.images.simulator "global" .root.Values.global) -}}
activeDeadlineSeconds: {{ $values.generator.activeDeadlineSeconds }}
retryStrategy:
  limit: {{ $values.generator.retryLimit }}
  retryPolicy: OnError
  backoff:
    duration: 30s
    factor: 2
    maxDuration: 10m
{{- with $values.generator.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
container:
  image: {{ $image | quote }}
  command: [simulator]
  env:
    - name: GENERATOR_MODE
      value: {{ .mode | quote }}
    - name: GENERATOR_WORKERS
      value: {{ $values.generator.workers | toString | quote }}
    - name: SIMULATOR_CONFIG
      value: {{ printf "/app/configs/%s" $values.generator.config | quote }}
{{- if .shard }}
    - name: SHARD_INDEX
      value: {{ "{{inputs.parameters.index}}" | quote }}
    - name: SHARD_COUNT
      value: {{ $values.generator.shards | toString | quote }}
{{- end }}
    - name: AWS_REGION
      value: {{ $values.config.AWS_REGION | quote }}
    - name: S3_ENDPOINT
      value: {{ $values.config.S3_ENDPOINT | quote }}
    - name: S3_PATH_STYLE
      value: {{ $values.config.S3_PATH_STYLE | quote }}
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: {{ $values.storage.externalSecret.name }}
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: {{ $values.storage.externalSecret.name }}
          key: AWS_SECRET_ACCESS_KEY
  resources:
    requests:
      {{- toYaml $values.generator.resources.requests | nindent 6 }}
    limits:
      {{- toYaml $values.generator.resources.limits | nindent 6 }}
{{- end -}}

{{- define "data-pipelines.env" -}}
- name: AWS_REGION
  value: {{ .Values.config.AWS_REGION | quote }}
- name: S3_ENDPOINT
  value: {{ .Values.config.S3_ENDPOINT | quote }}
- name: S3_PATH_STYLE
  value: {{ .Values.config.S3_PATH_STYLE | quote }}
- name: AWS_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: {{ .Values.storage.externalSecret.name }}
      key: AWS_ACCESS_KEY_ID
- name: AWS_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ .Values.storage.externalSecret.name }}
      key: AWS_SECRET_ACCESS_KEY
- name: ICEBERG_REST_URI
  value: {{ .Values.catalog.icebergRestUri | quote }}
- name: ICEBERG_WAREHOUSE
  value: {{ .Values.catalog.icebergWarehouse | quote }}
- name: ICEBERG_CLIENT_ID
  value: {{ .Values.catalog.icebergClientId | quote }}
- name: ICEBERG_OAUTH2_URI
  value: {{ .Values.catalog.icebergOauth2Uri | quote }}
- name: ICEBERG_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ .Values.catalog.externalSecret.name }}
      key: ICEBERG_CLIENT_SECRET
- name: OPENMETADATA_URL
  value: {{ .Values.catalog.openmetadataUrl | quote }}
- name: OPENMETADATA_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.catalog.externalSecret.name }}
      key: OPENMETADATA_TOKEN
- name: STARROCKS_CATALOG
  value: {{ .Values.catalog.starRocksCatalog | quote }}
- name: DATA_PRODUCTS_ROOT
  value: /contracts/data-products
{{- end -}}
