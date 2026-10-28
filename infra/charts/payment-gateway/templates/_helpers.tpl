{{- define "payment-gateway.selector" -}}
app.kubernetes.io/name: {{ . }}
payment-gateway/release: {{ . }}
{{- end -}}

{{- define "payment-gateway.services" -}}
{{- $services := dict -}}
{{- range $name, $settings := .Values.services -}}
{{- $service := mergeOverwrite (deepCopy $.Values.defaults) $settings -}}
{{- if $service.enabled -}}
{{- if and $service.database (not (hasKey $.Values.database.databases $service.database)) -}}
{{- fail (printf "service %s references unknown database %s" $name $service.database) -}}
{{- end -}}
{{- $_ := set $services $name $service -}}
{{- end -}}
{{- end -}}
{{- toYaml $services -}}
{{- end -}}

{{- define "payment-gateway.container-security" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "payment-gateway.database-roles" -}}
{{- $roles := list -}}
{{- range $name, $database := .Values.database.databases -}}
{{- $roles = append $roles (dict "name" $name) -}}
{{- range $role, $settings := $database.roles -}}
{{- $roles = append $roles (mergeOverwrite (dict "name" $role) $settings) -}}
{{- end -}}
{{- end -}}
{{- toYaml $roles -}}
{{- end -}}

{{- define "payment-gateway.database-env" -}}
- name: {{ .database | upper }}_{{ if .migrate }}MIGRATE_{{ end }}DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ .database }}-database
      key: {{ if .migrate }}migrate-{{ end }}database-url
{{- end -}}

{{- define "payment-gateway.image" -}}
{{- $repository := required "image.repository is required" .repository -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .digest) -}}
{{- fail "image.digest must be a sha256 digest" -}}
{{- end -}}
{{- printf "%s@%s" $repository .digest -}}
{{- end -}}
