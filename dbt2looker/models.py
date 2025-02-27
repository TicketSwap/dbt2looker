from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
from pydantic import BaseModel, Field, field_validator, model_validator


# dbt2looker utility types
class UnsupportedLookerDimensionTypeError(ValueError):
    def __init__(self, wrong_value: str):
        msg = f"{wrong_value} is not a supported looker dimension type"
        super().__init__(msg)


class UnsupportedDbtAdapterError(ValueError):
    def __init__(self, wrong_value: str):
        msg = f"{wrong_value} is not a supported dbt adapter"
        super().__init__(msg)


class SupportedDbtAdapters(str, Enum):
    bigquery = "bigquery"
    postgres = "postgres"
    redshift = "redshift"
    snowflake = "snowflake"
    spark = "spark"


# Lookml types
class LookerMeasureType(str, Enum):
    number = "number"
    string = "string"
    average = "average"
    average_distinct = "average_distinct"
    count = "count"
    count_distinct = "count_distinct"
    list = "list"
    max = "max"
    median = "median"
    median_distinct = "median_distinct"
    min = "min"
    # percentile = 'percentile'
    # percentile_distinct = 'percentile_distinct'
    sum = "sum"
    sum_distinct = "sum_distinct"


class LookerDimensionType(str, Enum):
    bin = "bin"
    date = "date"
    date_time = "date_time"
    distance = "distance"
    duration = "duration"
    location = "location"
    number = "number"
    string = "string"
    time = "time"
    unqoted = "unquoted"
    yesno = "yesno"
    zipcode = "zipcode"


class LookerParameterType(str, Enum):
    string = "string"
    yesno = "yesno"
    unquoted = "unquoted"
    date_time = "date_time"


class LookerJoinType(str, Enum):
    left_outer = "left_outer"
    full_outer = "full_outer"
    inner = "inner"
    cross = "cross"


class LookerJoinRelationship(str, Enum):
    many_to_one = "many_to_one"
    many_to_many = "many_to_many"
    one_to_many = "one_to_many"
    one_to_one = "one_to_one"


class LookerValueFormatName(str, Enum):
    decimal_0 = "decimal_0"
    decimal_1 = "decimal_1"
    decimal_2 = "decimal_2"
    decimal_3 = "decimal_3"
    decimal_4 = "decimal_4"
    usd_0 = "usd_0"
    usd = "usd"
    gbp_0 = "gbp_0"
    gbp = "gbp"
    eur_0 = "eur_0"
    eur = "eur"
    id = "id"
    percent_0 = "percent_0"
    percent_1 = "percent_1"
    percent_2 = "percent_2"
    percent_3 = "percent_3"
    percent_4 = "percent_4"


class LookerBooleanType(str, Enum):
    yes = "yes"
    no = "no"


class LookerParameterAllowedValue(BaseModel):
    value: str
    label: str


class LookerLinkType(BaseModel):
    label: str
    url: str
    icon_url: str | None = None


class Dbt2LookerBaseField(BaseModel):
    enabled: bool = True
    group_item_label: str | None = None
    group_label: str | None = None
    hidden: LookerBooleanType | None = None
    html: str | None = None
    label: str | None = None
    links: list[LookerLinkType] | None = None
    required_access_grants: list[str] | None = None
    required_fields: list[str] | None = None
    suggestable: LookerBooleanType | None = None
    suggestions: list[str] | None = None
    tags: list[str] | None = None
    value_format: str | None = None
    value_format_name: LookerValueFormatName | None = None
    view_label: str | None = None


class Dbt2LookerMeasure(Dbt2LookerBaseField):
    name: str
    description: str | None = None
    drill_fields: list[str] | None = None
    filters: list[dict[str, str]] | None = []
    sql: str | None = None
    sql_distinct_key: str | None = None
    type: LookerMeasureType

    @field_validator("filters")
    def filters_are_singular_dicts(cls, v: List[Dict[str, str]]):
        if v is not None:
            for f in v:
                if len(f) != 1:
                    raise ValueError(
                        "Multiple filter names provided for a single filter in measure block",
                    )
        return v


class Dbt2LookerBaseDimension(Dbt2LookerBaseField):
    case_sensitive: LookerBooleanType | None = None
    convert_tz: LookerBooleanType | None = None
    intervals: list[str] | None = None
    map_layer_name: str | None = None
    primary_key: LookerBooleanType | None = None
    sql_end: str | None = None
    sql_latitude: str | None = None
    sql_longitude: str | None = None
    sql_start: str | None = None
    timeframes: list[str] | None = None


class Dbt2LookerCustomDimension(Dbt2LookerBaseDimension):
    name: str
    sql: str
    description: str | None = None
    type: LookerDimensionType = LookerDimensionType.string


class Dbt2LookerDimension(Dbt2LookerBaseDimension):
    name: str | None = None
    sql: str | None = None
    description: str | None = None


class Dbt2LookerParameter(BaseModel):
    label: str
    description: str
    type: LookerParameterType
    group_label: str | None = None
    default_value: str | None = None
    allowed_values: list[LookerParameterAllowedValue] | None = None


class Dbt2LookerMeta(BaseModel):
    measures: list[Dbt2LookerMeasure] | None = []
    dimensions: Dbt2LookerDimension | None = Dbt2LookerDimension()


# Looker file types
class LookViewFile(BaseModel):
    filename: str
    contents: str


class LookModelFile(BaseModel):
    filename: str
    contents: str


# dbt config types
class DbtProjectConfig(BaseModel):
    name: str


class DbtModelColumnMeta(Dbt2LookerMeta):
    pass


class DbtModelColumn(BaseModel):
    name: str
    description: str | None = None
    data_type: str | None = None
    meta: DbtModelColumnMeta


class DbtNode(BaseModel):
    unique_id: str
    resource_type: str
    config: dict[str, Any]


class Dbt2LookerExploreJoin(BaseModel):
    join: str
    type: LookerJoinType | None = LookerJoinType.left_outer
    relationship: LookerJoinRelationship | None = LookerJoinRelationship.many_to_one
    sql_on: str


class Dbt2LookerModelMeta(BaseModel):
    add_explore: bool = False
    dimensions: list[Dbt2LookerCustomDimension] | None = {}
    measures: list[Dbt2LookerMeasure] | None = []
    parameters: dict[str, Dbt2LookerParameter] | None = {}
    joins: list[Dbt2LookerExploreJoin] | None = []


class DbtModelMeta(Dbt2LookerModelMeta):
    pass


class DbtModel(DbtNode):
    resource_type: Literal["model"]
    relation_name: str
    db_schema: str = Field(..., alias="schema")
    name: str
    description: str
    columns: dict[str, DbtModelColumn]
    tags: list[str]
    meta: DbtModelMeta

    @field_validator("columns")
    def case_insensitive_column_names(cls, v: Dict[str, DbtModelColumn]):
        return {name.lower(): column.model_copy(update={"name": column.name.lower()}) for name, column in v.items()}


class DbtManifestMetadata(BaseModel):
    adapter_type: str

    @field_validator("adapter_type")
    def adapter_must_be_supported(cls, v):
        try:
            SupportedDbtAdapters(v)
        except ValueError:
            raise UnsupportedDbtAdapterError(wrong_value=v)
        return v


class DbtManifest(BaseModel):
    nodes: Dict[str, Union[DbtModel, DbtNode]]
    metadata: DbtManifestMetadata


class DbtCatalogNodeMetadata(BaseModel):
    type: str
    db_schema: str = Field(..., alias="schema")
    name: str
    comment: Optional[str] = None
    owner: Optional[str] = None


class DbtCatalogNodeColumn(BaseModel):
    type: str
    comment: Optional[str] = None
    index: int
    name: str


class DbtCatalogNode(BaseModel):
    metadata: DbtCatalogNodeMetadata
    columns: Dict[str, DbtCatalogNodeColumn]

    @field_validator("columns")
    def case_insensitive_column_names(cls, v: Dict[str, DbtCatalogNodeColumn]):
        return {name.lower(): column.copy(update={"name": column.name.lower()}) for name, column in v.items()}


class DbtCatalog(BaseModel):
    nodes: Dict[str, DbtCatalogNode]
