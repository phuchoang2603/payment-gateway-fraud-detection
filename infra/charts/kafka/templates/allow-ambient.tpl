---
# Strimzi pods are enrolled in the Istio ambient mesh, so all in-mesh traffic reaches
# them over the ztunnel HBONE tunnel on 15008 and kubelet probes are rewritten to the
# ztunnel link-local source. The Strimzi-generated and chart NetworkPolicies are
# deny-by-default once they select these pods, so admit both explicitly.
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-ambient
  namespace: {{ .Release.Namespace }}
spec:
  podSelector:
    matchExpressions:
      - key: strimzi.io/cluster
        operator: Exists
  policyTypes:
    - Ingress
  ingress:
    - ports:
        - port: 15008
          protocol: TCP
    - from:
        - ipBlock:
            cidr: {{ .Values.monitoring.meshProbeSource }}
