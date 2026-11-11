---
# The Strimzi cluster operator, Kafka Connect and the schema registry all run outside the
# mesh and reach the broker over the KRaft control-plane (9090), replication (9091) and the
# internal client listeners (9092 plain, 9093 TLS) that the bootstrap service fronts. The
# broker pod is ambient-enrolled, so under STRICT mTLS its ztunnel drops those unmeshed
# plaintext connections and the operator's AdminClient times out (listNodes/getBrokerConfig).
# Exempt every listener the off-mesh peers use from STRICT mTLS.
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
    9092:
      mode: PERMISSIVE
    9093:
      mode: PERMISSIVE
