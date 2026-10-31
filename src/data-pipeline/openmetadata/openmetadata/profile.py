"""Native OpenMetadata profiling of the published marts, and nothing else in the warehouse."""

from openmetadata.catalog import native, server
from openmetadata.integrations import connectors
from openmetadata.warehouse import MARTS, SERVICE

PROFILED = ("fact_payment_attempt", "fact_merchant_day")


def workflow():
    source = connectors()[SERVICE]["source"]
    return {
        "source": {
            "type": source["type"],
            "serviceName": source["serviceName"],
            "serviceConnection": source["serviceConnection"],
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "schemaFilterPattern": {"includes": [f"^{MARTS}$"]},
                    "tableFilterPattern": {"includes": [f"^{table}$" for table in PROFILED]},
                    "computeTableMetrics": True,
                    "computeColumnMetrics": True,
                    "profileSampleConfig": {
                        "sampleConfigType": "STATIC",
                        "config": {"profileSample": 100, "profileSampleType": "PERCENTAGE"},
                    },
                }
            },
        },
        "processor": {"type": "orm-profiler", "config": {}},
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }


def main():
    native("profile", workflow())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
