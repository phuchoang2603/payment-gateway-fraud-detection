{{- define "llm-platform.principal" -}}
{{- printf "cluster.local/ns/%s/sa/%s" .namespace .name -}}
{{- end -}}

{{- define "llm-platform.podSecurity" -}}
runAsNonRoot: true
runAsUser: {{ .uid }}
runAsGroup: {{ .uid }}
fsGroup: {{ .uid }}
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "llm-platform.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "llm-platform.humanInput" -}}
{{- if hasKey . "humanInput" }}{{ .humanInput }}{{ else }}true{{ end -}}
{{- end -}}

{{- define "llm-platform.modelConfig" -}}
{{- if eq (include "llm-platform.humanInput" .agent) "true" }}{{ .root.Values.agents.modelConfig }}{{ else }}{{ .agent.name }}-model{{ end -}}
{{- end -}}

{{- define "llm-platform.mcpServer" -}}
{{- $resolved := .name -}}
{{- range $candidate, $server := .root.Values.gateway.mcpServers -}}
{{- if and (eq ($server.serves | default "") $.name) (eq ($server.audience | default "") $.audience) -}}
{{- $resolved = $candidate -}}
{{- end -}}
{{- end -}}
{{- $resolved -}}
{{- end -}}

{{- define "llm-platform.agentServers" -}}
{{- $root := .root -}}
{{- $agent := .agent -}}
{{- $names := $agent.mcpServers | default list -}}
{{- range $specialist := $agent.agents -}}
{{- $names = concat $names (index $root.Values.agents.specialists $specialist).mcpServers -}}
{{- end -}}
{{- $resolved := list -}}
{{- range $name := uniq $names -}}
{{- $resolved = append $resolved (include "llm-platform.mcpServer" (dict "root" $root "name" $name "audience" $agent.audience)) -}}
{{- end -}}
{{- toJson (uniq $resolved) -}}
{{- end -}}

{{- define "llm-platform.agentPolicy" -}}
{{- $root := .root -}}
{{- $agent := .agent -}}
{{- $a2a := $root.Values.agents.a2a -}}
{{- $origin := printf "http://%s.%s.svc.cluster.local:%d" $root.Values.gateway.name $root.Release.Namespace (int $agent.port) -}}
{{- $skills := dict -}}
{{- range $name, $skill := ($agent.remoteSkills | default dict) -}}
{{- $_ := set $skills $name (dict "url" (printf "%s/a2a/internal/%s" $origin $skill.agent) "description" $skill.description) -}}
{{- end -}}
{{- $transfers := list -}}
{{- range $name := $agent.agents -}}
{{- $transfers = append $transfers ($name | replace "-" "_") -}}
{{- end -}}
{{- if $transfers -}}
{{- $transfers = append $transfers ($agent.name | replace "-" "_") -}}
{{- end -}}
{{- $profiles := dict -}}
{{- range $client, $profile := $agent.profiles -}}
{{- $base := dict -}}
{{- with $profile.extends -}}
{{- $base = deepCopy (required (printf "agents.profileBases.%s is not defined" .) (index $root.Values.agents.profileBases .)) -}}
{{- end -}}
{{- $_ := set $profiles $client (mergeOverwrite $base (omit $profile "extends") (dict "transfers" $transfers)) -}}
{{- end -}}
{{- $issuers := dict "gateway" $a2a.signing.issuer "analyst" $a2a.analystIssuer "analystClient" $a2a.analystClient "userIdClaim" $a2a.userIdClaim -}}
{{- $policy := dict "issuers" $issuers "profiles" $profiles "remoteSkills" $skills -}}
{{- if $skills -}}{{- $_ := set $policy "remoteOrigin" $origin -}}{{- end -}}
{{- toJson $policy -}}
{{- end -}}
