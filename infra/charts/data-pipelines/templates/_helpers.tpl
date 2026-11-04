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

{{- define "data-pipelines.storageEnv" -}}
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
{{- end -}}

{{- define "data-pipelines.martsEnv" -}}
{{ include "data-pipelines.storageEnv" . }}
- name: CLICKHOUSE_URL
  value: {{ .Values.marts.clickhouseUrl | quote }}
- name: CLICKHOUSE_USER
  value: {{ .Values.marts.user | quote }}
- name: CLICKHOUSE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.marts.passwordSecret }}
      key: password
- name: MARTS_RETENTION_DAYS
  value: {{ .Values.marts.retentionDays | int64 | toString | quote }}
{{- end -}}

{{- define "data-pipelines.lakeEnv" -}}
{{ include "data-pipelines.storageEnv" . }}
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
{{- end -}}

{{- define "data-pipelines.env" -}}
{{ include "data-pipelines.lakeEnv" . }}
- name: OPENMETADATA_URL
  value: {{ .Values.catalog.openmetadataUrl | quote }}
- name: OPENMETADATA_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.catalog.openmetadataSecret.name }}
      key: OPENMETADATA_TOKEN
- name: DATA_PRODUCTS_ROOT
  value: /contracts/data-products
{{- end -}}

{{- define "data-pipelines.kafkaClientLabels" -}}
app.kubernetes.io/name: {{ .Values.catalog.kafkaClient | quote }}
{{- end -}}

{{- define "data-pipelines.driverPlacement" -}}
{{- with (.Values.spark.driverNodeSelector | default .Values.spark.nodeSelector) }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.spark.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "data-pipelines.workflowPlacement" -}}
{{- with .Values.workflowNodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- /* A large feature-store step: on the Spark executors' pool, with a scratch volume for disk spill. Node root
disks are too small for it, so the scratch is a claim on features.scratchClass when one is set. */}}
{{- define "data-pipelines.featureScratch" -}}
- name: {{ .name }}
  {{- if .root.Values.features.scratchClass }}
  ephemeral:
    volumeClaimTemplate:
      metadata:
        labels:
          app.kubernetes.io/part-of: data-pipelines
          data-pipelines.fraud-detection.dev/scratch: "true"
          data-pipelines.fraud-detection.dev/scratch-role: {{ .name }}
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: {{ .root.Values.features.scratchClass }}
        resources:
          requests:
            storage: {{ .size }}
  {{- else }}
  emptyDir:
    sizeLimit: {{ .size }}
  {{- end }}
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
    command: [python, -m, openmetadata.publish]
    env:
      - name: PIPELINES
        value: {{ .pipelines | quote }}
      - name: DATASET_VERSION
        value: {{ .dataset | quote }}
      - name: WORKFLOW_STATUS
        value: {{ "{{workflow.status}}" | quote }}
      - name: RUN_ID
        value: {{ "{{workflow.name}}" | quote }}
      - name: RUN_REPORT
        value: {{ .report | default "" | quote }}
{{- include "data-pipelines.env" .root | nindent 6 }}
{{- end -}}

{{- /* The batch image that writes Silver, Gold and the marts; a stage whose output on main came from the same range and
inputs by this image is skipped. */}}
{{- define "data-pipelines.codeVersion" -}}
{{ include "data-pipelines.image" (dict "image" .Values.images.spark "global" .Values.global) }}
{{- end -}}
{{- define "data-pipelines.plan" -}}
- name: {{ .name }}
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  inputs:
    parameters:
      - name: start
      - name: end
  outputs:
    parameters:
      - name: decision
        valueFrom:
          path: /tmp/plan/decision
      - name: silver-snapshots
        valueFrom:
          path: /tmp/plan/silver_snapshots
          default: "{}"
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata.plan]
    env:
      - name: PLAN_LAYER
        value: {{ .layer | quote }}
      - name: DP_START
        value: {{ "{{inputs.parameters.start}}" | quote }}
      - name: DP_END
        value: {{ "{{inputs.parameters.end}}" | quote }}
      - name: DP_CODE_VERSION
        value: {{ include "data-pipelines.codeVersion" .root | quote }}
{{- include "data-pipelines.env" .root | nindent 6 }}
{{- end -}}
{{- define "data-pipelines.wapBranch" -}}
audit-{{ "{{workflow.name}}" }}
{{- end -}}

{{- define "data-pipelines.publish-tables" -}}
- name: {{ .name | default "publish-tables" }}
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata.wap]
    env:
      - name: CHECK_PRODUCT
        value: {{ .product | quote }}
      - name: WAP_BRANCH
        value: {{ include "data-pipelines.wapBranch" . | quote }}
{{- include "data-pipelines.env" .root | nindent 6 }}
{{- end -}}

{{- define "data-pipelines.checks" -}}
- name: {{ .name | default "checks" }}
{{- with .mutex }}
  synchronization:
    mutexes:
      - name: {{ . }}
{{- end }}
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata.checks]
    env:
      - name: CHECK_PRODUCT
        value: {{ .product | quote }}
      - name: METADATA_CONNECTORS_FILE
        value: /etc/metadata/connectors.json
      - name: NATIVE_TIMEOUT_SECONDS
        value: {{ .root.Values.checks.nativeTimeoutSeconds | int64 | toString | quote }}
      - name: WAP_BRANCH
        value: {{ include "data-pipelines.wapBranch" . | quote }}
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

{{- define "data-pipelines.marts-step" -}}
- name: {{ .name }}
{{- with .mutex }}
  synchronization:
    mutexes:
      - name: {{ . }}
{{- end }}
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  {{- if .ranged }}
  inputs:
    parameters:
      - name: start
      - name: end
      - name: silver-snapshots
  {{- end }}
  {{- if .decision }}
  outputs:
    parameters:
      - name: decision
        valueFrom:
          path: /tmp/marts
  {{- end }}
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.spark "global" .root.Values.global) | quote }}
    command: {{ .command | toJson }}
    env:
      - name: RUN_ID
        value: {{ "{{workflow.name}}" | quote }}
      - name: RUN_REPORT_ROOT
        value: {{ .reportRoot | quote }}
      {{- if .ranged }}
      - name: DP_START
        value: {{ "{{inputs.parameters.start}}" | quote }}
      - name: DP_END
        value: {{ "{{inputs.parameters.end}}" | quote }}
      - name: DP_LOOKBACK_DAYS
        value: {{ .root.Values.pipeline.lookbackDays | toString | quote }}
      - name: SILVER_SNAPSHOTS
        value: {{ "{{inputs.parameters.silver-snapshots}}" | quote }}
      - name: DP_CODE_VERSION
        value: {{ include "data-pipelines.codeVersion" .root | quote }}
      {{- end }}
{{- include "data-pipelines.martsEnv" .root | nindent 6 }}
    resources:
      {{- toYaml .root.Values.marts.resources | nindent 6 }}
{{- end -}}

{{- define "data-pipelines.profile" -}}
- name: profile
{{- with .mutex }}
  synchronization:
    mutexes:
      - name: {{ . }}
{{- end }}
  retryStrategy:
    limit: 2
    retryPolicy: OnError
  container:
    image: {{ include "data-pipelines.image" (dict "image" .root.Values.images.openmetadata "global" .root.Values.global) | quote }}
    command: [python, -m, openmetadata.profile]
    env:
      - name: METADATA_CONNECTORS_FILE
        value: /etc/metadata/connectors.json
      - name: NATIVE_TIMEOUT_SECONDS
        value: {{ .root.Values.checks.nativeTimeoutSeconds | int64 | toString | quote }}
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
  # The step pod's name is unique per step, loop item and retry, so a backfill's months never collide.
  name: {{ "{{pod.name}}" | quote }}
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
    {{- with $sizing.driver.maxResultSize }}
    spark.driver.maxResultSize: {{ . | quote }}
    {{- end }}
    spark.executor.instances: {{ $sizing.executors | quote }}
    spark.executor.cores: {{ $sizing.executor.cores | quote }}
    {{- with $sizing.executor.requestCores }}
    spark.kubernetes.executor.request.cores: {{ . | quote }}
    {{- end }}
    spark.executor.memory: {{ $sizing.executor.memory | quote }}
    spark.executor.memoryOverhead: {{ $sizing.executor.memoryOverhead | quote }}
    spark.sql.shuffle.partitions: {{ $sizing.shufflePartitions | quote }}
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.adaptive.advisoryPartitionSizeInBytes: {{ $sizing.advisoryPartitionSize | quote }}
    spark.io.compression.codec: zstd
    {{- /* glibc gives each thread's native allocations their own arena, which grows a container's resident memory
    past its overhead without the JVM noticing. */}}
    spark.executorEnv.MALLOC_ARENA_MAX: "2"
    spark.decommission.enabled: "true"
    {{- /* A spot executor that is shutting down rejects tasks; without exclusion a task retries on it until its
    attempts run out, as the v19 Silver labels write did. */}}
    spark.excludeOnFailure.enabled: "true"
    spark.task.maxFailures: "8"
    {{- /* Spot reclaims and failed launches lose executors steadily; by default the application exits after twice
    its executor count of failures over its whole life, which a long v19 Silver run reached. */}}
    spark.executor.maxNumFailures: "200"
    spark.executor.failuresValidityInterval: "30m"
    spark.storage.decommission.enabled: "true"
    spark.storage.decommission.shuffleBlocks.enabled: "true"
    spark.storage.decommission.rddBlocks.enabled: "true"
    spark.pyspark.python: "/opt/data-pipeline/batch/.venv/bin/python"
    {{- with .root.Values.sparkEventLogDir }}
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: {{ . | quote }}
    spark.eventLog.rolling.enabled: "true"
    {{- end }}
  applicationTolerations:
    restartConfig:
      restartPolicy: Never
    resourceRetainPolicy: OnFailure
    resourceRetainDurationMillis: 300000
    ttlAfterStopMillis: 3600000
  driverSpec:
    podTemplateSpec:
      metadata:
        annotations:
          cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
        labels:
          {{- include "data-pipelines.kafkaClientLabels" .root | nindent 10 }}
          dp-product: {{ .product }}
          dp-stage: {{ .stage }}
      spec:
        {{- include "data-pipelines.driverPlacement" .root | nindent 8 }}
        containers:
          - name: spark-kubernetes-driver
            resources:
              requests:
                ephemeral-storage: {{ $sizing.driver.ephemeralStorage | quote }}
              limits:
                ephemeral-storage: {{ $sizing.driver.ephemeralStorage | quote }}
            env:
              {{- include "data-pipelines.env" .root | nindent 14 }}
              - name: SPARK_SHUFFLE_PARTITIONS
                value: {{ $sizing.shufflePartitions | quote }}
              {{- if .wap }}
              - name: WAP_BRANCH
                value: {{ include "data-pipelines.wapBranch" . | quote }}
              {{- end }}
              {{- range .extraEnv }}
              - name: {{ .name }}
                value: {{ .value | quote }}
              {{- end }}
  executorSpec:
    podTemplateSpec:
      metadata:
        annotations:
          cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
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

{{- define "data-pipelines.checksumEnv" -}}
- name: AWS_REQUEST_CHECKSUM_CALCULATION
  value: {{ .Values.storage.checksums.requestCalculation | quote }}
- name: AWS_RESPONSE_CHECKSUM_VALIDATION
  value: {{ .Values.storage.checksums.responseValidation | quote }}
{{- end -}}

{{- define "data-pipelines.feastEnv" -}}
- name: FEAST_S3_ENDPOINT_URL
  value: {{ .Values.config.S3_ENDPOINT | quote }}
{{ include "data-pipelines.checksumEnv" . }}
- name: AWS_DEFAULT_REGION
  value: {{ .Values.config.AWS_REGION | quote }}
- name: FEAST_REGISTRY
  value: {{ .Values.features.registry | quote }}
- name: FEAST_STREAM_EPOCHS
  value: {{ .Values.features.epochs | quote }}
- name: FEAST_REDIS_ADDR
  value: {{ printf "%s:6379" .Values.features.redis.name | quote }}
- name: FEAST_REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.features.redis.password.secretName }}
      key: password
- name: FEATURE_KAFKA_BROKERS
  value: {{ .Values.bronze.kafka.bootstrapServers | quote }}
- name: KAFKA_BROKERS
  value: {{ .Values.bronze.kafka.bootstrapServers | quote }}
{{- end -}}
