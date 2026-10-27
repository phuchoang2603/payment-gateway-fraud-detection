{{- define "data-pipelines.image" -}}
{{- $repository := required "image.repository is required" .repository -}}
{{- if .digest -}}
{{- if not (regexMatch "^sha256:[a-f0-9]{64}$" .digest) -}}
{{- fail "image.digest must be a sha256 digest" -}}
{{- end -}}
{{- printf "%s@%s" $repository .digest -}}
{{- else -}}
{{- printf "%s:%s" $repository (.tag | default "latest") -}}
{{- end -}}
{{- end -}}
