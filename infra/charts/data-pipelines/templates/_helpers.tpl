{{- define "data-pipelines.governance-writes" -}}
{{- range $name := list "domain" "domain_assets" "source_product" "bronze_product" "product_assets" "events_contract" "labels_contract" "lineage" }}
- - name: {{ $name | replace "_" "-" }}
    template: openmetadata-write
    arguments:
      parameters:
        {{- range $field := list "method" "path" "body" }}
        - name: {{ $field }}
          value: {{ printf "{{=jsonpath(inputs.parameters.requests, '$.%s.%s')}}" $name $field | quote }}
        {{- end }}
{{- end }}
{{- end }}
