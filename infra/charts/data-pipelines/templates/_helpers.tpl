{{- define "data-pipelines.governance-writes" -}}
{{- range $group := list (list "domain") (list "domain_assets" "source_product" "bronze_product") (list "product_assets" "events_contract" "labels_contract" "lineage") }}
{{- range $index, $name := $group }}
{{ if eq $index 0 }}-{{ else }} {{ end }} - name: {{ $name | replace "_" "-" }}
    template: openmetadata-write
    arguments:
      parameters:
        {{- range $field := list "method" "path" "body" }}
        - name: {{ $field }}
          value: {{ printf "{{=jsonpath(inputs.parameters.requests, '$.%s.%s')}}" $name $field | quote }}
        {{- end }}
{{- end }}
{{- end }}
{{- range $index, $name := list "events_contract" "labels_contract" }}
{{ if eq $index 0 }}-{{ else }} {{ end }} - name: {{ $name | replace "_" "-" }}-provenance
    template: openmetadata-write
    arguments:
      parameters:
        - {name: method, value: PATCH}
        - name: path
          value: {{ printf "{{='/v1/dataContracts/' + jsonpath(steps['%s'].outputs.result, '$.id')}}" ($name | replace "_" "-") | quote }}
        - name: body
          value: {{ printf "{{=toJson(jsonpath(inputs.parameters.requests, '$.%s.provenance'))}}" $name | quote }}
{{- end }}
{{- end }}
