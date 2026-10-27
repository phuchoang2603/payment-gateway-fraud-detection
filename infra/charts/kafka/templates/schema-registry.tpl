{{- if .Values.schemaRegistry.enabled }}
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: {{ .Values.kafka.clusterName }}-schemas
  labels:
    strimzi.io/cluster: {{ .Values.kafka.clusterName }}
spec:
  topicName: _schemas
  partitions: 1
  replicas: {{ .Values.kafka.replicationFactor }}
  config:
    cleanup.policy: compact
    retention.ms: -1
    min.insync.replicas: {{ .Values.kafka.minInsyncReplicas }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-registry
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/name: karapace
      app.kubernetes.io/instance: {{ .Release.Name }}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: karapace
        app.kubernetes.io/instance: {{ .Release.Name }}
    spec:
      automountServiceAccountToken: false
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: karapace
          image: "{{ .Values.schemaRegistry.image.repository }}:{{ .Values.schemaRegistry.image.tag }}"
          command: [python3, -m, karapace]
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          ports:
            - name: http
              containerPort: 8081
          env:
            - name: KARAPACE_KARAPACE_REGISTRY
              value: "true"
            - name: KARAPACE_KARAPACE_REST
              value: "false"
            - name: KARAPACE_BOOTSTRAP_URI
              value: {{ printf "%s-kafka-bootstrap:9092" .Values.kafka.clusterName | quote }}
            - name: KARAPACE_HOST
              value: "0.0.0.0"
            - name: KARAPACE_PORT
              value: "8081"
            - name: KARAPACE_ADVERTISED_HOSTNAME
              value: {{ printf "schema-registry.%s.svc.cluster.local" .Release.Namespace | quote }}
            - name: KARAPACE_GROUP_ID
              value: schema-registry
            - name: KARAPACE_CLIENT_ID
              value: schema-registry
            - name: KARAPACE_TOPIC_NAME
              value: _schemas
            - name: KARAPACE_REPLICATION_FACTOR
              value: {{ .Values.kafka.replicationFactor | quote }}
            - name: KARAPACE_COMPATIBILITY
              value: BACKWARD_TRANSITIVE
            - name: KARAPACE_CONSUMER_REQUEST_MAX_BYTES
              value: "1048576"
            - name: KARAPACE_LOG_LEVEL
              value: INFO
          resources:
            {{- toYaml .Values.schemaRegistry.resources | nindent 12 }}
          readinessProbe:
            httpGet:
              path: /_health
              port: http
            timeoutSeconds: 3
          livenessProbe:
            tcpSocket:
              port: http
            timeoutSeconds: 3
          startupProbe:
            tcpSocket:
              port: http
            periodSeconds: 5
            failureThreshold: 60
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            - name: runtime
              mountPath: /opt/karapace/runtime
      volumes:
        - name: tmp
          emptyDir:
            sizeLimit: 64Mi
        - name: runtime
          emptyDir:
            sizeLimit: 64Mi
---
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
spec:
  selector:
    app.kubernetes.io/name: karapace
    app.kubernetes.io/instance: {{ .Release.Name }}
  ports:
    - name: http
      port: 8081
      targetPort: http
{{- end }}
