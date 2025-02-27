from __future__ import annotations

import logging
import json
import re

import lkml

from . import models

LOOKER_DTYPE_MAP = {
    "bigquery": {
        "INT64": "number",
        "INTEGER": "number",
        "FLOAT": "number",
        "FLOAT64": "number",
        "NUMERIC": "number",
        "BIGNUMERIC": "number",
        "BOOLEAN": "yesno",
        "STRING": "string",
        "TIMESTAMP": "timestamp",
        "DATETIME": "datetime",
        "DATE": "date",
        "TIME": "string",  # Can time-only be handled better in looker?
        "BOOL": "yesno",
        "ARRAY": "string",
        "GEOGRAPHY": "string",
        "BYTES": "string",
    },
    "snowflake": {
        "NUMBER": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "INT": "number",
        "INTEGER": "number",
        "BIGINT": "number",
        "SMALLINT": "number",
        "FLOAT": "number",
        "FLOAT4": "number",
        "FLOAT8": "number",
        "DOUBLE": "number",
        "DOUBLE PRECISION": "number",
        "REAL": "number",
        "VARCHAR": "string",
        "CHAR": "string",
        "CHARACTER": "string",
        "STRING": "string",
        "TEXT": "string",
        "BINARY": "string",
        "VARBINARY": "string",
        "BOOLEAN": "yesno",
        "DATE": "date",
        "DATETIME": "datetime",
        "TIME": "string",  # can we support time?
        "TIMESTAMP": "timestamp",
        "TIMESTAMP_NTZ": "timestamp",
        # TIMESTAMP_LTZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        # TIMESTAMP_TZ not supported (see https://docs.looker.com/reference/field-params/dimension_group)
        "VARIANT": "string",
        "OBJECT": "string",
        "ARRAY": "string",
        "GEOGRAPHY": "string",
    },
    "redshift": {
        "SMALLINT": "number",
        "INT2": "number",
        "INTEGER": "number",
        "INT": "number",
        "INT4": "number",
        "BIGINT": "number",
        "INT8": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "REAL": "number",
        "FLOAT4": "number",
        "DOUBLE PRECISION": "number",
        "FLOAT8": "number",
        "FLOAT": "number",
        "BOOLEAN": "yesno",
        "BOOL": "yesno",
        "CHAR": "string",
        "CHARACTER": "string",
        "NCHAR": "string",
        "BPCHAR": "string",
        "VARCHAR": "string",
        "CHARACTER VARYING": "string",
        "NVARCHAR": "string",
        "TEXT": "string",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "TIMESTAMP WITHOUT TIME ZONE": "timestamp",
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        "GEOMETRY": "string",
        # HLLSKETCH not supported
        "TIME": "string",
        "TIME WITHOUT TIME ZONE": "string",
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    "postgres": {
        # BIT, BIT VARYING, VARBIT not supported
        # BOX not supported
        # BYTEA not supported
        # CIRCLE not supported
        # INTERVAL not supported
        # LINE not supported
        # LSEG not supported
        # PATH not supported
        # POINT not supported
        # POLYGON not supported
        # TSQUERY, TSVECTOR not supported
        "XML": "string",
        "UUID": "string",
        "PG_LSN": "string",
        "MACADDR": "string",
        "JSON": "string",
        "JSONB": "string",
        "CIDR": "string",
        "INET": "string",
        "MONEY": "number",
        "SMALLINT": "number",
        "INT2": "number",
        "SMALLSERIAL": "number",
        "SERIAL2": "number",
        "INTEGER": "number",
        "INT": "number",
        "INT4": "number",
        "SERIAL": "number",
        "SERIAL4": "number",
        "BIGINT": "number",
        "INT8": "number",
        "BIGSERIAL": "number",
        "SERIAL8": "number",
        "DECIMAL": "number",
        "NUMERIC": "number",
        "REAL": "number",
        "FLOAT4": "number",
        "DOUBLE PRECISION": "number",
        "FLOAT8": "number",
        "FLOAT": "number",
        "BOOLEAN": "yesno",
        "BOOL": "yesno",
        "CHAR": "string",
        "CHARACTER": "string",
        "NCHAR": "string",
        "BPCHAR": "string",
        "VARCHAR": "string",
        "CHARACTER VARYING": "string",
        "NVARCHAR": "string",
        "TEXT": "string",
        "DATE": "date",
        "TIMESTAMP": "timestamp",
        "TIMESTAMP WITHOUT TIME ZONE": "timestamp",
        # TIMESTAMPTZ not supported
        # TIMESTAMP WITH TIME ZONE not supported
        "GEOMETRY": "string",
        # HLLSKETCH not supported
        "TIME": "string",
        "TIME WITHOUT TIME ZONE": "string",
        "STRING": "string",
        # TIMETZ not supported
        # TIME WITH TIME ZONE not supported
    },
    "spark": {
        "BYTE": "number",
        "SHORT": "number",
        "INTEGER": "number",
        "LONG": "number",
        "FLOAT": "number",
        "DOUBLE": "number",
        "DECIMAL": "number",
        "STRING": "string",
        "VARCHAR": "string",
        "CHAR": "string",
        "BOOLEAN": "yesno",
        "TIMESTAMP": "timestamp",
        "DATE": "datetime",
    },
}

looker_date_time_types = ["datetime", "timestamp"]
looker_scalar_types = ["number", "yesno", "string", "date"]

looker_timeframes = [
    "raw",
    "time",
    "date",
    "week",
    "month",
    "quarter",
    "year",
]

looker_intervals = ["hour", "day"]


def normalise_spark_types(column_type: str) -> str:
    return re.match(r"^[^\(]*", column_type).group(0)


def map_adapter_type_to_looker(
    adapter_type: models.SupportedDbtAdapters,
    column_type: str,
):
    normalised_column_type = (
        normalise_spark_types(column_type)
        if adapter_type == models.SupportedDbtAdapters.spark.value
        else re.sub(r"\([,\d]+\)", "", column_type)
    ).upper()
    looker_type = LOOKER_DTYPE_MAP[adapter_type].get(normalised_column_type)
    if (column_type is not None) and (looker_type is None):
        logging.warning(
            "Column type %s not supported for conversion from %s to looker. " "No dimension will be created.",
            column_type,
            adapter_type,
        )
    return looker_type


def lookml_add_common_properties(
    looker_field: models.Dbt2LookerCustomDimension | models.Dbt2LookerDimension | models.Dbt2LookerMeasure,
    looker_dict: dict,
):    
    if looker_field.group_item_label:
        looker_dict["group_item_label"] = looker_field.group_item_label
    if looker_field.group_label:
        looker_dict["group_label"] = looker_field.group_label
    if looker_field.hidden:
        looker_dict["hidden"] = looker_field.hidden.value
    if looker_field.html:
        looker_dict["html"] = looker_field.html
    if looker_field.label:
        looker_dict["label"] = looker_field.label
    if looker_field.links:
        looker_dict["links"] = [{"label": link.label, "url": link.url} for link in looker_field.links]
    if looker_field.required_access_grants:
        looker_dict["required_access_grants"] = looker_field.required_access_grants
    if looker_field.required_fields:
        looker_dict["required_fields"] = looker_field.required_fields
    if looker_field.suggestable:
        looker_dict["suggestable"] = looker_field.suggestable.value
    if looker_field.tags:
        looker_dict["tags"] = looker_field.tags
    if looker_field.value_format:
        looker_dict["value_format"] = looker_field.value_format
    if looker_field.value_format_name:
        looker_dict["value_format_name"] = looker_field.value_format_name.value
    if looker_field.view_label:
        looker_dict["view_label"] = looker_field.view_label
    return looker_dict


def lookml_dimensions_from_model(
    model: models.DbtModel,
    adapter_type: models.SupportedDbtAdapters,
):
    column_dimensions = [
        lookml_dimension(
            dimension=column.meta.dimension,
            adapter_type=adapter_type,
            column=column,
        )
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type) in looker_scalar_types
    ]

    custom_dimensions = [
        lookml_dimension(
            dimension=dimension,
            adapter_type=adapter_type,
        )
        for dimension in model.meta.dimensions
        if dimension.type in looker_scalar_types
    ]
    return column_dimensions + custom_dimensions


def lookml_dimension_groups_from_model(
    model: models.DbtModel,
    adapter_type: models.SupportedDbtAdapters,
):
    return [
        lookml_dimension(
            dimension=column.meta.dimension,
            adapter_type=adapter_type,
            column=column,
        )
        for column in model.columns.values()
        if column.meta.dimension.enabled
        and map_adapter_type_to_looker(adapter_type, column.data_type) in looker_date_time_types
    ]


def lookml_dimension(
    dimension: models.Dbt2LookerDimension | models.Dbt2LookerCustomDimension,
    adapter_type: models.SupportedDbtAdapters,
    column: models.DbtModelColumn | None = None,
):
    d = {
        "name": dimension.name or column.name,
        "description": dimension.description or column.description,
    }
    d["type"] = (
        dimension.type.value
        if hasattr(dimension, "type")
        else map_adapter_type_to_looker(adapter_type, column.data_type)
    )
    if dimension.sql:
        d["sql"] = dimension.sql
    if d["type"] == "date" or d["type"] in looker_date_time_types:
        d["convert_tz"] = dimension.convert_tz or models.LookerBooleanType.no.value
    if dimension.intervals:
        d["intervals"] = dimension.intervals or looker_intervals
    if dimension.map_layer_name:
        d["map_layer_name"] = dimension.map_layer_name
    if dimension.primary_key:
        d["primary_key"] = dimension.primary_key.value
    if dimension.sql_end:
        d["sql_end"] = dimension.sql_end
    if dimension.sql_latitude:
        d["sql_latitude"] = dimension.sql_latitude
    if dimension.sql_longitude:
        d["sql_longitude"] = dimension.sql_longitude
    if dimension.sql_start:
        d["sql_start"] = dimension.sql_start
    if dimension.suggestions:
        d["suggestions"] = dimension.suggestions
    if d["type"] in looker_date_time_types: # check if is a dimension group
        d["name"] = d["name"].removesuffix("_at")
        d["timeframes"] = dimension.timeframes or looker_timeframes
    d = lookml_add_common_properties(dimension, d)
    if d["name"].startswith("pk_"):
        d["primary_key"] = models.LookerBooleanType.yes.value
        d["hidden"] = models.LookerBooleanType.yes.value
    if d["name"].startswith("fk_"):
        d["hidden"] = models.LookerBooleanType.yes.value
    return d


def lookml_measures_from_model(model: models.DbtModel, adapter_type: models.SupportedDbtAdapters):
    return [
        lookml_measure(measure, model, adapter_type, column)
        for column in model.columns.values()
        for measure in column.meta.measures
    ] + [lookml_measure(measure, model, adapter_type) for measure in model.meta.measures]


def lookml_measure(
    measure: models.Dbt2LookerMeasure,
    model: models.DbtModel,
    adapter_type: models.SupportedDbtAdapters,
    column: models.DbtModelColumn | None = None,
):
    m = {"name": measure.name, "type": measure.type.value}
    if column:
        m["sql"] = (measure.sql or f"${{TABLE}}.{column.name}",)
        m["description"] = (
            measure.description or column.description or f"{measure.type.value.capitalize()} of {column.name}",
        )
    else:
        if measure.sql:
            m["sql"] = measure.sql
        if measure.description:
            m["description"] = measure.description
    if measure.filters:
        m["filters"] = lookml_measure_filters(measure, model)
    m = lookml_add_common_properties(measure, m)
    return m


def lookml_measure_filters(measure: models.Dbt2LookerMeasure, model: models.DbtModel):
    try:
        columns = {column_name: model.columns[column_name] for f in measure.filters for column_name in f}
    except KeyError as e:
        msg = (
            f"Model {model.unique_id} contains a measure that references a non_existent column: {e}\n"
            f"Ensure that dbt model {model.unique_id} contains a column: {e}"
        )
        raise ValueError(
            msg,
        ) from e
    return [
        {(columns[column_name].meta.dimension.name or column_name): fexpr for column_name, fexpr in f.items()}
        for f in measure.filters
    ]


def lookml_parameters_from_model(model: models.DbtModel):
    return [lookml_parameter(parameter_name, parameter) for parameter_name, parameter in model.meta.parameters.items()]


def lookml_parameter(parameter_name: str, parameter: models.Dbt2LookerParameter):
    p = {
        "name": parameter_name,
        "label": parameter.label,
        "description": parameter.description,
        "type": parameter.type.value,
    }
    if parameter.group_label:
        p["group_label"] = parameter.group_label
    if parameter.default_value:
        p["default_value"] = parameter.default_value
    if parameter.allowed_values:
        p["allowed_values"] = [{"value": v.value, "label": v.label} for v in parameter.allowed_values]

    return p


def lookml_view_from_dbt_model(
    model: models.DbtModel,
    adapter_type: models.SupportedDbtAdapters,
):
    lookml = {
        "view": {
            "name": model.name,
            "sql_table_name": model.relation_name,
            "dimension_groups": lookml_dimension_groups_from_model(model, adapter_type),
            "dimensions": lookml_dimensions_from_model(model, adapter_type),
            "parameters": lookml_parameters_from_model(model),
            "measures": lookml_measures_from_model(model, adapter_type),
        },
    }
    logging.debug(
        "Created view from model %s with %d measures, %d dimensions, %d dimension groups",
        model.name,
        len(lookml["view"]["measures"]),
        len(lookml["view"]["dimensions"]),
        len(lookml["view"]["dimension_groups"]),
        len(lookml["view"]["parameters"]),
    )
    with open("lookml.json", "w") as f:
        f.write(json.dumps(lookml, indent=2))
    contents = lkml.dump(lookml)
    filename = f"{model.name}.view.lkml"
    return models.LookViewFile(filename=filename, contents=contents)


def lookml_model_from_dbt_model(model: models.DbtModel, connection_name: str):
    # Note: assumes view names = model names
    #       and models are unique across dbt packages in project
    lookml = {
        "connection": connection_name,
        "include": "/views/*",
        "explore": {
            "name": model.name,
            "description": model.description,
            "joins": [
                {
                    "name": join.join,
                    "type": join.type.value,
                    "relationship": join.relationship.value,
                    "sql_on": join.sql_on,
                }
                for join in model.meta.joins
            ],
        },
    }
    contents = lkml.dump(lookml)
    filename = f"{model.name}.model.lkml"
    return models.LookModelFile(filename=filename, contents=contents)
