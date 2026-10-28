from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline_common.manifest import physical_schemas


def table_name(table, namespace="bronze"):
    if not namespace.replace("_", "").isalnum():
        raise ValueError("invalid namespace")
    return f"lake.{namespace}.raw_{table}"


def event_input_schema(expected, version):
    try:
        fields = physical_schemas()["payment_events"][version]["assessment_fields"]
    except KeyError as error:
        raise ValueError(f"unsupported physical event schema: {version}") from error
    risk = expected["risk_decision_recorded"].dataType
    assessment = risk["assessment"].dataType
    available = {field.name: field for field in assessment}
    if len(fields) != len(set(fields)) or any(name not in available for name in fields):
        raise ValueError(f"unsupported assessment fields in physical schema: {version}")
    if any(not field.nullable and field.name not in fields for field in assessment):
        raise ValueError(f"physical schema removes required assessment fields: {version}")
    selected = T.StructType([available[name] for name in fields])
    risk_fields = [
        T.StructField(field.name, selected, field.nullable) if field.name == "assessment" else field for field in risk
    ]
    source_risk = T.StructType(risk_fields)
    return T.StructType(
        [
            T.StructField(field.name, source_risk, field.nullable) if field.name == "risk_decision_recorded" else field
            for field in expected
        ]
    )


def normalize_events(frame, source_schema, target_schema):
    source_assessment = source_schema["risk_decision_recorded"].dataType["assessment"].dataType
    target_assessment = target_schema["risk_decision_recorded"].dataType["assessment"].dataType
    assessment = F.col("risk_decision_recorded.assessment")
    for field in target_assessment:
        if field.name not in source_assessment.fieldNames():
            assessment = assessment.withField(field.name, F.lit(None).cast(field.dataType))
    risk = F.col("risk_decision_recorded").withField("assessment", assessment)
    return frame.withColumn("risk_decision_recorded", risk)
