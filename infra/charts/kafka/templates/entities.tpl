{{- $secretNamespace := .Release.Namespace }}
{{- range $entityName, $entity := .Values.entities }}
{{- range $entity.topics }}
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: {{ .name }}
  namespace: {{ $.Release.Namespace }}
  labels:
    strimzi.io/cluster: {{ $.Values.kafka.clusterName }}
spec:
  partitions: {{ .partitions }}
  replicas: {{ $.Values.kafka.replicas }}
  config:
    retention.ms: {{ .retention | int64 | quote }}
{{- end }}
{{- with $entity.connector }}
{{- $secret := printf "%s/%s" $secretNamespace .sourceSecretName }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ printf "%s-debezium-connector-role" $entityName }}
  namespace: {{ $secretNamespace }}
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    resourceNames: [{{ .sourceSecretName | quote }}]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ printf "%s-debezium-connector-rolebinding" $entityName }}
  namespace: {{ $secretNamespace }}
subjects:
  - kind: ServiceAccount
    name: {{ printf "%s-connect" $.Values.connect.clusterName }}
    namespace: {{ $.Release.Namespace }}
roleRef:
  kind: Role
  name: {{ printf "%s-debezium-connector-role" $entityName }}
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: kafka.strimzi.io/v1
kind: KafkaConnector
metadata:
  name: {{ .name }}
  namespace: {{ $.Release.Namespace }}
  labels:
    strimzi.io/cluster: {{ $.Values.connect.clusterName }}
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  autoRestart:
    enabled: true
  config:
    database.hostname: {{ if .databaseHost }}{{ .databaseHost | quote }}{{ else }}${secrets:{{ $secret }}:host}{{ end }}
    database.port: {{ .databasePort }}
    database.user: ${secrets:{{ $secret }}:username}
    database.password: ${secrets:{{ $secret }}:password}
    database.dbname: {{ .databaseName }}
    topic.prefix: {{ default $entityName .topicPrefix | quote }}
    plugin.name: pgoutput
    slot.name: {{ default (printf "debezium_%s_slot" $entityName) .slotName }}
    table.include.list: {{ .table }}
    tombstones.on.delete: "false"
{{- if .publicationAutocreateMode }}
    publication.autocreate.mode: {{ .publicationAutocreateMode | quote }}
{{- end }}
    transforms: outbox
    transforms.outbox.type: io.debezium.transforms.outbox.EventRouter
    transforms.outbox.table.field.event.id: {{ .router.eventId | quote }}
    transforms.outbox.table.field.event.key: {{ .router.key | quote }}
    transforms.outbox.table.field.event.payload: {{ .router.payload | quote }}
    transforms.outbox.table.expand.json.payload: "false"
    transforms.outbox.tracing.span.context.field: tracingspancontext
    transforms.outbox.tracing.with.context.field.only: "true"
    transforms.outbox.table.fields.additional.placement: "tracingspancontext:header:tracingspancontext"
    transforms.outbox.route.by.field: {{ .router.routeByField | quote }}
    transforms.outbox.route.topic.replacement: {{ .router.topic | quote }}
    key.converter: org.apache.kafka.connect.storage.StringConverter
    value.converter: org.apache.kafka.connect.converters.ByteArrayConverter
{{- end }}
{{- end }}
