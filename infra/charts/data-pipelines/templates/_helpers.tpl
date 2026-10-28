{{- define "data-pipelines.image" -}}
{{- $repository := required "image.repository is required" .image.repository -}}
{{- $tag := required "global.imageTag is required" .global.imageTag -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
