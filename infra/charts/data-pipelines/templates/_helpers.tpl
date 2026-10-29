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
      name: {{ .Values.catalog.icebergSecret.name }}
      key: ICEBERG_CLIENT_SECRET
- name: OPENMETADATA_URL
  value: {{ .Values.catalog.openmetadataUrl | quote }}
- name: OPENMETADATA_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.catalog.openmetadataSecret.name }}
      key: OPENMETADATA_TOKEN
- name: CLICKHOUSE_DATABASE
  value: {{ .Values.catalog.clickhouseDatabase | quote }}
- name: DATA_PRODUCTS_ROOT
  value: /contracts/data-products
{{- end -}}

{{- define "data-pipelines.kafkaClientLabels" -}}
app.kubernetes.io/name: {{ .Values.catalog.kafkaClient | quote }}
{{- end -}}

{{- define "data-pipelines.placement" -}}
{{- with .Values.spark.nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.spark.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "data-pipelines.spark.retry" -}}
retryStrategy:
  limit: {{ .Values.spark.retry.limit }}
  retryPolicy: {{ .Values.spark.retry.policy }}
{{- end -}}

{{- define "data-pipelines.publish" -}}
- name: publish
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata_jobs.publish]
    env:
      - name: PIPELINES
        value: {{ .pipelines | quote }}
      - name: STEPS
        value: {{ .steps | default "" | quote }}
      - name: DATASET_VERSION
        value: {{ .dataset | quote }}
      - name: WORKFLOW_STATUS
        value: {{ "{{workflow.status}}" | quote }}
      - name: FAILED_STEPS
        value: {{ "{{workflow.failures}}" | quote }}
      - name: RUN_ID
        value: {{ "{{workflow.name}}" | quote }}
{{- include "data-pipelines.env" .root | nindent 6 }}
{{- end -}}

{{- define "data-pipelines.checks" -}}
- name: checks
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata_jobs.checks]
    env:
      - name: CHECK_PRODUCT
        value: {{ .product | quote }}
      - name: METADATA_CONNECTORS_FILE
        value: /etc/metadata/connectors.json
{{- include "data-pipelines.env" .root | nindent 6 }}
    envFrom:
      - secretRef:
          name: {{ .root.Values.checks.connectorSecret }}
    volumeMounts:
      - name: metadata-connectors
        mountPath: /etc/metadata
        readOnly: true
  volumes:
    - name: metadata-connectors
      configMap:
        name: {{ .root.Values.checks.connectorsConfigMap }}
{{- end -}}

{{- define "data-pipelines.spark-application" -}}
{{- $sizing := .sizing -}}
{{- $image := include "data-pipelines.image" (dict "image" .root.Values.images.spark "global" .root.Values.global) -}}
apiVersion: spark.apache.org/v1
kind: SparkApplication
metadata:
  name: {{ printf "%s-%s-%s" .stem "{{workflow.uid}}" "{{retries}}" }}
  namespace: {{ .root.Values.namespace }}
  labels:
    dp-product: {{ .product }}
    dp-stage: {{ .stage }}
    dp-workflow: {{ "{{workflow.name}}" | quote }}
spec:
  pyFiles: {{ printf "local:///opt/data-pipeline/batch/.venv/lib/python3.12/site-packages/batch/%s.py" .main | quote }}
  runtimeVersions:
    sparkVersion: "4.1.3"
  sparkConf:
    spark.kubernetes.container.image: {{ $image | quote }}
    spark.kubernetes.authenticate.driver.serviceAccountName: data-pipelines-spark
    spark.kubernetes.executor.deleteOnTermination: "true"
    spark.kubernetes.driver.request.cores: {{ $sizing.driver.requestCores | quote }}
    spark.driver.cores: {{ $sizing.driver.cores | quote }}
    spark.driver.memory: {{ $sizing.driver.memory | quote }}
    spark.driver.memoryOverhead: {{ $sizing.driver.memoryOverhead | quote }}
    spark.executor.instances: {{ $sizing.executors | quote }}
    spark.executor.cores: {{ $sizing.executor.cores | quote }}
    spark.executor.memory: {{ $sizing.executor.memory | quote }}
    spark.executor.memoryOverhead: {{ $sizing.executor.memoryOverhead | quote }}
    spark.sql.shuffle.partitions: {{ $sizing.shufflePartitions | quote }}
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.adaptive.advisoryPartitionSizeInBytes: {{ $sizing.advisoryPartitionSize | quote }}
    spark.decommission.enabled: "true"
    spark.storage.decommission.enabled: "true"
    spark.storage.decommission.shuffleBlocks.enabled: "true"
    spark.storage.decommission.rddBlocks.enabled: "true"
    spark.pyspark.python: /opt/data-pipeline/batch/.venv/bin/python
  applicationTolerations:
    restartConfig:
      restartPolicy: Never
    resourceRetainPolicy: OnFailure
    resourceRetainDurationMillis: 300000
    ttlAfterStopMillis: 3600000
  driverSpec:
    podTemplateSpec:
      metadata:
        labels:
          {{- include "data-pipelines.kafkaClientLabels" .root | nindent 10 }}
          dp-product: {{ .product }}
          dp-stage: {{ .stage }}
      spec:
        {{- include "data-pipelines.placement" .root | nindent 8 }}
        containers:
          - name: spark-kubernetes-driver
            resources:
              requests:
                ephemeral-storage: {{ $sizing.driver.ephemeralStorage | quote }}
              limits:
                ephemeral-storage: {{ $sizing.driver.ephemeralStorage | quote }}
            env:
              {{- include "data-pipelines.env" .root | nindent 14 }}
              {{- range .extraEnv }}
              - name: {{ .name }}
                value: {{ .value | quote }}
              {{- end }}
  executorSpec:
    podTemplateSpec:
      metadata:
        labels:
          {{- include "data-pipelines.kafkaClientLabels" .root | nindent 10 }}
          dp-product: {{ .product }}
          dp-stage: {{ .stage }}
      spec:
        {{- include "data-pipelines.placement" .root | nindent 8 }}
        containers:
          - name: spark-kubernetes-executor
            resources:
              requests:
                ephemeral-storage: {{ $sizing.executor.ephemeralStorage | quote }}
              limits:
                ephemeral-storage: {{ $sizing.executor.ephemeralStorage | quote }}
            env:
              {{- include "data-pipelines.env" .root | nindent 14 }}
{{- end -}}
