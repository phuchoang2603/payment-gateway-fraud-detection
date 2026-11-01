{{- define "ml-training.image" -}}
{{- include "ml-training.imageOf" (dict "root" . "repository" .Values.image.repository) -}}
{{- end -}}

{{- define "ml-training.imageOf" -}}
{{- printf "%s:%s" .repository (required (printf "image tag missing for %s" .repository) (index .root.Values.global.imageTags .repository)) -}}
{{- end -}}

{{- define "ml-training.workerEnv" -}}
{{- range $name := list "AWS_REGION" "S3_ENDPOINT" "S3_PATH_STYLE" "ICEBERG_REST_URI" "ICEBERG_WAREHOUSE" "ICEBERG_CLIENT_ID" "ICEBERG_OAUTH2_URI" "MODEL_REGISTRY_URL" }}
- name: {{ $name }}
  value: {{ index $.Values.config $name | quote }}
{{- end }}
{{- range $name, $secret := dict "AWS_ACCESS_KEY_ID" "ml-training-storage" "AWS_SECRET_ACCESS_KEY" "ml-training-storage" "ICEBERG_CLIENT_SECRET" "ml-training-iceberg" }}
- name: {{ $name }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret }}
      key: {{ $name }}
{{- end }}
{{- end -}}
