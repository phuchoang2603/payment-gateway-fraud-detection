{{- define "payment-gateway.canary" -}}
revisionHistoryLimit: 2
minReadySeconds: 5
progressDeadlineSeconds: 1800
progressDeadlineAbort: true
strategy:
  canary:
    stableService: edge
    canaryService: edge-canary
    maxSurge: 1
    maxUnavailable: 0
    scaleDownDelaySeconds: 60
    abortScaleDownDelaySeconds: 30
    trafficRouting:
      plugins:
        argoproj-labs/gatewayAPI:
          httpRoutes:
            - name: edge
          namespace: {{ .Release.Namespace }}
    steps:
      {{- range .Values.rollout.weights }}
      - setWeight: {{ . }}
      - analysis:
          templates:
            - templateName: edge-checkout
          args:
            - name: revision
              valueFrom:
                podTemplateHashValue: Latest
      {{- end }}
{{- end -}}
