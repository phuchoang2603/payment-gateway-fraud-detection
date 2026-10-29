from fraud_batch.common.catalog import server

SERVICE = "starrocks"


def parameters(case):
    return [
        {"name": "sqlExpression", "value": case.sql},
        {"name": "strategy", "value": "COUNT"},
        {"name": "operator", "value": "=="},
        {"name": "threshold", "value": "0"},
    ]


def connection():
    from fraud_batch.common.integrations import connection as shared_connection

    return shared_connection(SERVICE)


def test_workflow(table, case_list, entity_name):
    return {
        "source": {
            "type": "testsuite",
            "serviceName": SERVICE,
            "sourceConfig": {
                "config": {
                    "type": "TestSuite",
                    "entityFullyQualifiedName": entity_name,
                    "testCases": [],
                }
            },
        },
        "processor": {
            "type": "orm-test-runner",
            "config": {
                "testCases": [
                    {
                        "name": c.name,
                        "description": c.description,
                        "testDefinitionName": "tableCustomSQLQuery",
                        "parameterValues": parameters(c),
                    }
                    for c in case_list
                    if c.table == table
                ],
            },
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server()},
    }
