---
# The Strimzi cluster operator runs outside the mesh and uses the broker control-plane
# (9090) and replication (9091) listeners.
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata:
  name: strimzi-operator-access
  namespace: {{ .Release.Namespace }}
spec:
  selector:
    matchLabels:
      strimzi.io/kind: Kafka
  mtls:
    mode: STRICT
  portLevelMtls:
    9090:
      mode: PERMISSIVE
    9091:
      mode: PERMISSIVE
