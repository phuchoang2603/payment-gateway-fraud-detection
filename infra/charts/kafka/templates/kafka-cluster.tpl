---
apiVersion: kafka.strimzi.io/v1
kind: KafkaNodePool
metadata:
  name: {{ printf "%s-dual-role" .Values.kafka.clusterName }}
  namespace: {{ $.Release.Namespace }}
  labels:
    strimzi.io/cluster: {{ .Values.kafka.clusterName }}
spec:
  replicas: {{ .Values.kafka.replicas }}
  roles:
    - controller
    - broker
{{- with .Values.kafka.jvmOptions }}
  jvmOptions:
{{ toYaml . | nindent 4 }}
{{- end }}
{{- with .Values.kafka.resources }}
  resources:
{{ toYaml . | nindent 4 }}
{{- end }}
  storage:
    type: jbod
    volumes:
      - id: 0
        type: persistent-claim
        size: {{ .Values.kafka.storage.size }}
        class: {{ .Values.kafka.storage.storageClass | default "local-path" }}
        kraftMetadata: shared
---
apiVersion: kafka.strimzi.io/v1
kind: Kafka
metadata:
  name: {{ .Values.kafka.clusterName }}
  namespace: {{ $.Release.Namespace }}
  annotations:
    strimzi.io/node-pools: enabled
    strimzi.io/kraft: enabled
spec:
  kafka:
    version: {{ .Values.kafka.version }}
    metadataVersion: {{ .Values.kafka.metadataVersion }}
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
        networkPolicyPeers:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: {{ .Values.kafka.clientNamespace | default .Release.Namespace }}
          {{- if .Values.kafka.dataNamespace }}
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: {{ .Values.kafka.dataNamespace }}
            podSelector:
              matchExpressions:
                - key: app.kubernetes.io/name
                  operator: In
                  values: {{ .Values.kafka.dataClients | toJson }}
          {{- end }}
      - name: tls
        port: 9093
        type: internal
        tls: true
        networkPolicyPeers:
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: {{ .Values.kafka.clientNamespace | default .Release.Namespace }}
          {{- if .Values.kafka.dataNamespace }}
          - namespaceSelector:
              matchLabels:
                kubernetes.io/metadata.name: {{ .Values.kafka.dataNamespace }}
            podSelector:
              matchExpressions:
                - key: app.kubernetes.io/name
                  operator: In
                  values: {{ .Values.kafka.dataClients | toJson }}
          {{- end }}
    config:
      auto.create.topics.enable: "false"
      offsets.topic.replication.factor: {{ .Values.kafka.replicationFactor }}
      transaction.state.log.replication.factor: {{ .Values.kafka.replicationFactor }}
      transaction.state.log.min.isr: {{ .Values.kafka.minInsyncReplicas }}
      default.replication.factor: {{ .Values.kafka.replicationFactor }}
      min.insync.replicas: {{ .Values.kafka.minInsyncReplicas }}
  entityOperator:
    topicOperator:
      reconciliationIntervalMs: 120000
{{- with .Values.entityOperator.topicOperator.jvmOptions }}
      jvmOptions:
{{ toYaml . | nindent 8 }}
{{- end }}
{{- with .Values.entityOperator.topicOperator.resources }}
      resources:
{{ toYaml . | nindent 8 }}
{{- end }}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ printf "%s-clients" .Values.kafka.clusterName }}
  namespace: {{ $.Release.Namespace }}
spec:
  podSelector:
    matchLabels:
      strimzi.io/cluster: {{ .Values.kafka.clusterName }}
      strimzi.io/kind: Kafka
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {{ .Values.kafka.clientNamespace | default .Release.Namespace }}
        {{- if .Values.kafka.dataNamespace }}
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: {{ .Values.kafka.dataNamespace }}
          podSelector:
            matchExpressions:
              - key: app.kubernetes.io/name
                operator: In
                values: {{ .Values.kafka.dataClients | toJson }}
        {{- end }}
      ports:
        - protocol: TCP
          port: 9092
        - protocol: TCP
          port: 9093
