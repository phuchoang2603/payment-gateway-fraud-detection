from botocore.exceptions import ClientError
from feast.infra.registry.s3 import S3RegistryStore
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto


class PrefixS3RegistryStore(S3RegistryStore):
    # Feast's S3 store calls HeadBucket first, which needs bucket-wide ListBucket. The gateway keys may list only
    # their own prefixes, so the registry object is read directly.
    def get_registry_proto(self):
        try:
            body = self.s3_client.Object(self._bucket, self._key).get()["Body"].read()
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise FileNotFoundError(f"no Feast registry at {self._uri.geturl()}") from error
            raise
        registry = RegistryProto()
        registry.ParseFromString(body)
        return registry
