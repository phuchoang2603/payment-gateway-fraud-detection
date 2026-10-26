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
{{- if or (eq $name "events_contract") (eq $name "labels_contract") }}
- - name: {{ $name | replace "_" "-" }}-provenance
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
{{- end }}
