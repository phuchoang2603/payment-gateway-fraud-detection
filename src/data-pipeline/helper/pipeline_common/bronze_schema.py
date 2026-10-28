from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline_common.manifest import physical_schemas


def table_name(table, namespace="bronze"):
    if not namespace.replace("_", "").isalnum():
        raise ValueError("invalid namespace")
    return f"lake.{namespace}.raw_{table}"


def event_input_schema(expected, version, definitions=None):
    definitions = definitions or physical_schemas()
    try:
        definition = definitions["payment_events"][version]
    except KeyError as error:
        raise ValueError(f"unsupported physical event schema: {version}") from error
    assessment = expected["risk_decision_recorded"].dataType["assessment"].dataType
    fields = definition["assessment_fields"]
    available = {field.name for field in assessment}
    if len(fields) != len(set(fields)) or not set(fields).issubset(available):
        raise ValueError(f"unsupported assessment fields in physical schema: {version}")
    omitted = set(definition.get("omitted_fields", [])) | {
        f"risk_decision_recorded.assessment.{name}" for name in available.difference(fields)
    }
    seen = set()

    def project_schema(schema, path=()):
        result = []
        for field in schema:
            current = (*path, field.name)
            name = ".".join(current)
            if name in omitted:
                if not field.nullable:
                    raise ValueError(f"physical schema removes required field: {name}")
                seen.add(name)
                continue
            dtype = field.dataType
            if isinstance(dtype, T.StructType):
                dtype = project_schema(dtype, current)
            elif isinstance(dtype, T.ArrayType) and isinstance(dtype.elementType, T.StructType):
                dtype = T.ArrayType(project_schema(dtype.elementType, (*path, field.name + "[]")), dtype.containsNull)
            result.append(T.StructField(field.name, dtype, field.nullable, field.metadata))
        if path == ("risk_decision_recorded", "assessment"):
            by_name = {field.name: field for field in result}
            result = [by_name[name] for name in fields]
        return T.StructType(result)

    result = project_schema(expected)
    if seen != omitted:
        raise ValueError(f"unknown omitted physical fields: {sorted(omitted - seen)}")
    return result


def normalize_events(frame, source_schema, target_schema):
    def project(column, source, target, path):
        if isinstance(target, T.StructType):
            if not isinstance(source, T.StructType):
                raise ValueError(f"incompatible physical type: {path}")
            extra = set(source.fieldNames()) - set(target.fieldNames())
            if extra:
                raise ValueError(f"undeclared physical fields at {path}: {sorted(extra)}")
            fields = []
            for field in target:
                child_path = f"{path}.{field.name}" if path else field.name
                if field.name not in source.fieldNames():
                    if not field.nullable:
                        raise ValueError(f"physical schema removes required field: {child_path}")
                    value = F.lit(None).cast(field.dataType)
                else:
                    value = project(
                        column.getField(field.name), source[field.name].dataType, field.dataType, child_path
                    )
                fields.append(value.alias(field.name))
            return F.when(column.isNull(), F.lit(None).cast(target)).otherwise(F.struct(*fields))
        if isinstance(target, T.ArrayType):
            if not isinstance(source, T.ArrayType):
                raise ValueError(f"incompatible physical array: {path}")
            return F.transform(column, lambda item: project(item, source.elementType, target.elementType, path + "[]"))
        if source != target:
            raise ValueError(f"incompatible physical type: {path}")
        return column

    source_names = set(source_schema.fieldNames())
    columns = []
    for field in target_schema:
        if field.name not in source_names:
            if not field.nullable:
                raise ValueError(f"physical schema removes required field: {field.name}")
            columns.append(F.lit(None).cast(field.dataType).alias(field.name))
        else:
            columns.append(
                project(F.col(field.name), source_schema[field.name].dataType, field.dataType, field.name).alias(
                    field.name
                )
            )
    return frame.select(*columns)
