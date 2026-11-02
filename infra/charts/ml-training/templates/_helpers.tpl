{{- define "ml-training.image" -}}
{{- include "ml-training.imageOf" (dict "root" . "repository" .Values.image.repository) -}}
{{- end -}}

{{- define "ml-training.imageOf" -}}
{{- printf "%s:%s" .repository (required (printf "image tag missing for %s" .repository) (index .root.Values.global.imageTags .repository)) -}}
{{- end -}}
