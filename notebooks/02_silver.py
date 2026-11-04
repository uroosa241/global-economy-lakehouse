# Databricks notebook source
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone


dbutils.widgets.text("catalog", "dbt_global_economy")
dbutils.widgets.text("silver_schema", "silver")
dbutils.widgets.text("bronze_root", "abfss://bronze@stglobaleconomic.dfs.core.windows.net/worldbank/")
dbutils.widgets.text("silver_location", "abfss://silver@stglobaleconomic.dfs.core.windows.net/worldbank")

dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("gold_location", "abfss://gold@stglobaleconomic.dfs.core.windows.net/worldbank")


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    silver_schema: str
    bronze_root: str
    silver_location: str
    run_id: str
    run_ts_utc: datetime

    @property
    def schema_fqn(self) -> str:
        return f"`{self.catalog}`.`{self.silver_schema}`"

    def table(self, name: str) -> str:
        """Fully qualified three-level UC name for a Silver table."""
        return f"{self.schema_fqn}.`{name}`"


cfg = PipelineConfig(
    catalog=dbutils.widgets.get("catalog").strip(),
    silver_schema=dbutils.widgets.get("silver_schema").strip(),
    bronze_root=dbutils.widgets.get("bronze_root").strip().rstrip("/") + "/",
    silver_location=dbutils.widgets.get("silver_location").strip().rstrip("/"),
    run_id=str(uuid.uuid4()),
    run_ts_utc=datetime.now(timezone.utc),
)

# Planned Silver objects (created in later steps, listed here as the single source of truth)
TABLES = {
    "observations": cfg.table("wb_indicator_observations"),   # business records, full load history
    "load_metadata": cfg.table("wb_load_metadata"),            # API pagination / lastupdated per file
    "quarantine": cfg.table("wb_indicator_quarantine"),        # rows failing DQ rules (never silently dropped)
    "run_audit": cfg.table("wb_pipeline_run_audit"),           # one row per pipeline run
}

# ---------------------------------------------------------------------------
# Structured logger (idempotent: re-running the cell does not duplicate handlers)
# ---------------------------------------------------------------------------
logger = logging.getLogger("worldbank_silver")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        f"%(asctime)s | %(levelname)s | run_id={cfg.run_id} | %(message)s", "%Y-%m-%dT%H:%M:%S"))
    logger.addHandler(_h)
else:
    for _h in logger.handlers:
        _h.setFormatter(logging.Formatter(
            f"%(asctime)s | %(levelname)s | run_id={cfg.run_id} | %(message)s", "%Y-%m-%dT%H:%M:%S"))


class PipelineError(Exception):
    """Raised for any condition that must stop the pipeline (fail fast, never write partial Silver data)."""


logger.info(f"Config loaded: catalog={cfg.catalog}, schema={cfg.silver_schema}, "
            f"bronze_root={cfg.bronze_root}, silver_location={cfg.silver_location}")
logger.info(f"Spark version: {spark.version}")



# DBTITLE 1,Create Silver schema and validate access
# ---------------------------------------------------------------------------
# 1. Verify Bronze is reachable (read-only listing; no writes to Bronze anywhere in this notebook)
# ---------------------------------------------------------------------------
try:
    bronze_entries = dbutils.fs.ls(cfg.bronze_root)
    logger.info(f"Bronze reachable: {len(bronze_entries)} top-level indicator folder(s): "
                f"{[e.name.rstrip('/') for e in bronze_entries]}")
    if not bronze_entries:
        raise PipelineError(f"Bronze root is empty: {cfg.bronze_root}")
except PipelineError:
    raise
except Exception as e:
    logger.error(f"Cannot access Bronze root {cfg.bronze_root}: {e}")
    raise PipelineError("Bronze access check failed") from e

# ---------------------------------------------------------------------------
# 2. Create the Silver schema idempotently (never drops or replaces an existing schema)
# ---------------------------------------------------------------------------
existing = {r[0] for r in spark.sql(f"SHOW SCHEMAS IN `{cfg.catalog}`").collect()}
if cfg.silver_schema in existing:
    logger.info(f"Schema {cfg.schema_fqn} already exists - leaving it unchanged")
else:
    try:
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {cfg.schema_fqn}
            MANAGED LOCATION '{cfg.silver_location}'
            COMMENT 'Silver layer: cleansed, conformed World Bank indicator data. History-preserving (business key + load date).'
        """)
        logger.info(f"Created schema {cfg.schema_fqn} with managed location {cfg.silver_location}")
    except Exception as e:
        logger.error(f"Schema creation failed: {e}")
        raise PipelineError("Could not create Silver schema") from e

# ---------------------------------------------------------------------------
# 3. Validate the schema exists and points to the expected storage
# ---------------------------------------------------------------------------
schema_info = {r.database_description_item: r.database_description_value
               for r in spark.sql(f"DESCRIBE SCHEMA EXTENDED {cfg.schema_fqn}").collect()}
display(spark.createDataFrame(list(schema_info.items()), "property STRING, value STRING"))

location = schema_info.get("Location") or ""
if not location.rstrip("/").startswith(cfg.silver_location):
    raise PipelineError(f"Unexpected Silver schema location: {location!r}")
logger.info(f"Step 1 validation passed: schema {cfg.schema_fqn} located at {location}")


# DBTITLE 1,2a - Bronze file inventory
import re
from collections import Counter
from pyspark.sql import functions as F, types as T

# Layout observed in Bronze: <indicator>/<ISO3>/<ISO3>_<indicator>_<YYYY-MM-DD>.json
# Indicator codes may contain dots/underscores, so the indicator group is anchored by the trailing _<date>.json
FILE_PATH_RE = re.compile(
    r"^(?P<folder_indicator>[^/]+)/(?P<folder_iso3>[^/]+)/"
    r"(?P<file_iso3>[A-Za-z0-9]+)_(?P<file_indicator>.+)_(?P<load_date>\d{4}-\d{2}-\d{2})\.json$"
)


def list_files_recursive(path: str) -> list:
    """Read-only recursive listing of a storage path."""
    files = []
    for item in dbutils.fs.ls(path):
        if item.isDir():
            files.extend(list_files_recursive(item.path))
        else:
            files.append(item)
    return files


def classify_file(f) -> dict:
    rel = f.path[len(cfg.bronze_root):] if f.path.startswith(cfg.bronze_root) else f.path
    rec = dict(file_path=f.path, relative_path=rel, size_bytes=int(f.size),
               modified_ts=datetime.fromtimestamp(f.modificationTime / 1000, tz=timezone.utc),
               indicator_code=None, country_iso3=None, load_date=None, status="VALID", issue=None)
    m = FILE_PATH_RE.match(rel)
    if f.name.startswith(("_", ".")):
        rec.update(status="IGNORED_SYSTEM_FILE", issue="hidden or marker file")
        return rec
    if not m:
        rec.update(status="INVALID_PATH_PATTERN",
                   issue="expected <indicator>/<ISO3>/<ISO3>_<indicator>_<YYYY-MM-DD>.json")
        return rec
    g = m.groupdict()
    rec.update(indicator_code=g["file_indicator"], country_iso3=g["file_iso3"].upper())
    try:
        rec["load_date"] = datetime.strptime(g["load_date"], "%Y-%m-%d").date()
    except ValueError:
        rec.update(status="INVALID_LOAD_DATE", issue=f"unparseable date {g['load_date']}")
        return rec
    if g["folder_indicator"] != g["file_indicator"] or g["folder_iso3"].upper() != g["file_iso3"].upper():
        rec.update(status="FOLDER_FILE_MISMATCH",
                   issue=f"folder={g['folder_indicator']}/{g['folder_iso3']} file={g['file_indicator']}/{g['file_iso3']}")
    elif rec["size_bytes"] == 0:
        rec.update(status="EMPTY_FILE", issue="0 bytes")
    return rec


try:
    file_records = [classify_file(f) for f in list_files_recursive(cfg.bronze_root)]
except Exception as e:
    logger.error(f"Bronze discovery failed: {e}")
    raise PipelineError("Bronze discovery failed") from e

# Same indicator + country + load date landed more than once -> ambiguous, flag both
load_keys = Counter((r["indicator_code"], r["country_iso3"], r["load_date"]) for r in file_records if r["status"] == "VALID")
for r in file_records:
    if r["status"] == "VALID" and load_keys[(r["indicator_code"], r["country_iso3"], r["load_date"])] > 1:
        r.update(status="DUPLICATE_LOAD", issue="same indicator/country/load_date found in more than one file")

INVENTORY_SCHEMA = T.StructType([
    T.StructField("file_path", T.StringType(), False),
    T.StructField("relative_path", T.StringType(), False),
    T.StructField("size_bytes", T.LongType(), False),
    T.StructField("modified_ts", T.TimestampType(), False),
    T.StructField("indicator_code", T.StringType(), True),
    T.StructField("country_iso3", T.StringType(), True),
    T.StructField("load_date", T.DateType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("issue", T.StringType(), True),
])
inventory_df = spark.createDataFrame(file_records, INVENTORY_SCHEMA)

status_counts = Counter(r["status"] for r in file_records)
logger.info(f"Discovered {len(file_records)} file(s); status breakdown: {dict(status_counts)}")
if status_counts.get("VALID", 0) == 0:
    raise PipelineError("No valid Bronze files found")

display(inventory_df.orderBy("indicator_code", "country_iso3", "load_date"))

# COMMAND ----------

# DBTITLE 1,2b - Line-level read and record classification
valid_inventory_df = (inventory_df.filter(F.col("status") == "VALID")
                      .select(F.col("file_path").alias("source_file_path"),
                              F.col("indicator_code").alias("path_indicator_code"),
                              F.col("country_iso3").alias("path_country_iso3"),
                              "load_date"))
valid_paths = [r["file_path"] for r in file_records if r["status"] == "VALID"]

# Plain-text read: one row per physical line, nothing inferred, nothing dropped.
raw_lines_df = (spark.read.format("text").load(valid_paths)
                .select(F.col("_metadata.file_path").alias("source_file_path"),
                        F.col("_metadata.file_modification_time").alias("source_file_modified_ts"),
                        F.col("value").alias("raw_line")))

lines_df = (raw_lines_df
    .withColumn("has_bom", F.col("raw_line").startswith("\ufeff"))
    .withColumn("clean_line", F.regexp_replace(F.regexp_replace("raw_line", "^\ufeff", ""), "\r$", ""))
    .withColumn("parsed", F.expr("try_parse_json(clean_line)"))
    .withColumn("top_keys", F.expr("json_object_keys(clean_line)"))
    .withColumn("record_type",
        F.when(F.trim("clean_line") == "", "BLANK")
         .when(F.col("parsed").isNull(), "MALFORMED")
         .when(F.col("top_keys").isNull(), "NON_OBJECT")
         .when(F.array_contains("top_keys", "page") & F.array_contains("top_keys", "total"), "PAGINATION_METADATA")
         .when(F.array_contains("top_keys", "indicator") & F.array_contains("top_keys", "date"), "OBSERVATION")
         .otherwise("UNKNOWN"))
    .withColumn("key_signature", F.array_join(F.array_sort("top_keys"), ","))
    .join(F.broadcast(valid_inventory_df), "source_file_path", "left"))

# Line classification per file and key signature
display(lines_df.groupBy("source_file_path", "record_type", "key_signature")
        .agg(F.count("*").alias("line_count"),
             F.sum(F.col("has_bom").cast("int")).alias("lines_with_bom"),
             F.sum(F.col("path_indicator_code").isNull().cast("int")).alias("unmatched_to_inventory"))
        .orderBy("source_file_path", "record_type"))

# Any line that is not metadata or an observation is shown in full for investigation
anomalies_df = lines_df.filter(~F.col("record_type").isin("PAGINATION_METADATA", "OBSERVATION"))
logger.info(f"Non-business / anomalous lines: {anomalies_df.count()}")
display(anomalies_df.select("source_file_path", "record_type", "raw_line"))

# COMMAND ----------

# DBTITLE 1,2c - Data-driven structure inference
business_lines_df = lines_df.filter(F.col("record_type").isin("PAGINATION_METADATA", "OBSERVATION"))

# Schema inferred from the actual content, merged across every record of each type
display(business_lines_df.groupBy("record_type")
        .agg(F.count("*").alias("records"),
             F.expr("schema_of_variant_agg(parsed)").alias("inferred_schema")))

obs_lines_df = business_lines_df.filter(F.col("record_type") == "OBSERVATION")

# Field-level profile of observations: physical JSON type of every leaf, and date formats
field_profile_df = obs_lines_df.select(
    F.expr("schema_of_variant(parsed:value)").alias("value_json_type"),
    F.expr("schema_of_variant(parsed:decimal)").alias("decimal_json_type"),
    F.expr("schema_of_variant(parsed:unit)").alias("unit_json_type"),
    F.expr("schema_of_variant(parsed:obs_status)").alias("obs_status_json_type"),
    F.expr("array_join(array_sort(json_object_keys(get_json_object(clean_line, '$.indicator'))), ',')").alias("indicator_keys"),
    F.expr("array_join(array_sort(json_object_keys(get_json_object(clean_line, '$.country'))), ',')").alias("country_keys"),
    F.when(F.expr("parsed:date::string").rlike(r"^\d{4}$"), "YYYY")
     .when(F.expr("parsed:date::string").rlike(r"^\d{4}Q[1-4]$"), "YYYYQn")
     .when(F.expr("parsed:date::string").rlike(r"^\d{4}M\d{2}$"), "YYYYMmm")
     .otherwise("OTHER").alias("date_format"),
)
display(field_profile_df.groupBy(*field_profile_df.columns).count())

# COMMAND ----------

# DBTITLE 1,2d - Schema drift detection and lossless field extraction
# Expected key sets derived from the 2c inference (not assumed). Any deviation is reported as drift.
EXPECTED_TOP_KEYS = {
    "PAGINATION_METADATA": ["lastupdated", "page", "pages", "per_page", "sourceid", "total"],
    "OBSERVATION": ["country", "countryiso3code", "date", "decimal", "indicator", "obs_status", "unit", "value"],
}
EXPECTED_NESTED_KEYS = ["id", "value"]   # for both indicator{} and country{}

expected_keys_col = (F.when(F.col("record_type") == "OBSERVATION", F.array(*map(F.lit, EXPECTED_TOP_KEYS["OBSERVATION"])))
                      .otherwise(F.array(*map(F.lit, EXPECTED_TOP_KEYS["PAGINATION_METADATA"]))))
nested_expected = F.array(*map(F.lit, EXPECTED_NESTED_KEYS))

drift_df = (business_lines_df
    .withColumn("extra_keys", F.array_except("top_keys", expected_keys_col))
    .withColumn("missing_keys", F.array_except(expected_keys_col, "top_keys"))
    .withColumn("indicator_keys", F.expr("json_object_keys(get_json_object(clean_line, '$.indicator'))"))
    .withColumn("country_keys", F.expr("json_object_keys(get_json_object(clean_line, '$.country'))"))
    .withColumn("nested_drift", F.when(F.col("record_type") == "OBSERVATION",
        (F.size(F.array_except(F.coalesce("indicator_keys", F.array()), nested_expected)) > 0) |
        (F.size(F.array_except(nested_expected, F.coalesce("indicator_keys", F.array()))) > 0) |
        (F.size(F.array_except(F.coalesce("country_keys", F.array()), nested_expected)) > 0) |
        (F.size(F.array_except(nested_expected, F.coalesce("country_keys", F.array()))) > 0)).otherwise(F.lit(False)))
    .filter((F.size("extra_keys") > 0) | (F.size("missing_keys") > 0) | F.col("nested_drift")))

drift_count = drift_df.count()
logger.info(f"Records with schema drift: {drift_count}")
if drift_count:
    display(drift_df.select("source_file_path", "record_type", "extra_keys", "missing_keys", "nested_drift", "clean_line"))

# Lossless extraction: every field as STRING from VARIANT (exact numeric text preserved; typing in Step 6)
def vget(path: str):
    return F.expr(f"variant_get(parsed, '{path}', 'string')")

LINEAGE_COLS = ["source_file_path", "source_file_modified_ts", "path_indicator_code", "path_country_iso3", "load_date"]

bronze_obs_df = obs_lines_df.select(
    *LINEAGE_COLS,
    vget("$.indicator.id").alias("indicator_id"),
    vget("$.indicator.value").alias("indicator_name"),
    vget("$.country.id").alias("country_id"),
    vget("$.country.value").alias("country_name"),
    vget("$.countryiso3code").alias("countryiso3code"),
    vget("$.date").alias("date"),
    vget("$.value").alias("value"),
    vget("$.unit").alias("unit"),
    vget("$.obs_status").alias("obs_status"),
    vget("$.decimal").alias("decimal"),
    F.col("clean_line").alias("raw_record"),
)

bronze_meta_df = business_lines_df.filter(F.col("record_type") == "PAGINATION_METADATA").select(
    *LINEAGE_COLS,
    vget("$.page").alias("page"),
    vget("$.pages").alias("pages"),
    vget("$.per_page").alias("per_page"),
    vget("$.total").alias("total"),
    vget("$.sourceid").alias("sourceid"),
    vget("$.lastupdated").alias("lastupdated"),
    F.col("clean_line").alias("raw_record"),
)

display(bronze_meta_df)
display(bronze_obs_df.orderBy(F.col("date").desc()).limit(10))

# COMMAND ----------

# DBTITLE 1,2e - Step 2 validation
checks = []

def check(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

# 1. Every valid file was read, and every line maps back to the inventory
read_files = {r[0] for r in lines_df.select("source_file_path").distinct().collect()}
check("all valid files read", sorted(valid_paths), sorted(read_files))
check("lines not matched to inventory", 0, lines_df.filter(F.col("path_indicator_code").isNull()).count())

# 2. No unparseable / unclassified lines
for rt in ["MALFORMED", "NON_OBJECT", "UNKNOWN"]:
    check(f"{rt} lines", 0, lines_df.filter(F.col("record_type") == rt).count())
check("BLANK lines", 0, lines_df.filter(F.col("record_type") == "BLANK").count(), critical=False)

# 3. No schema drift
check("records with schema drift", 0, drift_count)

# 4. Per-file reconciliation against API pagination metadata
recon_df = (bronze_obs_df.groupBy("source_file_path").agg(F.count("*").alias("observation_rows"))
    .join(bronze_meta_df.groupBy("source_file_path").agg(
            F.count("*").alias("metadata_rows"),
            F.max(F.col("total").cast("long")).alias("api_total"),
            F.max(F.col("pages").cast("long")).alias("api_pages"),
            F.max(F.col("per_page").cast("long")).alias("api_per_page")),
          "source_file_path", "full")
    .withColumn("reconciled", (F.col("observation_rows") == F.col("api_total")) & (F.col("metadata_rows") == F.col("api_pages"))))
display(recon_df)
check("files failing total/pages reconciliation", 0, recon_df.filter(~F.coalesce("reconciled", F.lit(False))).count())

# 5. Path-derived identifiers agree with record content
check("indicator_id != path indicator", 0, bronze_obs_df.filter(~F.col("indicator_id").eqNullSafe(F.col("path_indicator_code"))).count())
check("countryiso3code != path ISO3", 0, bronze_obs_df.filter(~F.col("countryiso3code").eqNullSafe(F.col("path_country_iso3"))).count())

# 6. Independent method: Spark's JSON reader (different parser) must find the same record counts
indep = (spark.read.json(valid_paths)
         .select(F.when(F.col("indicator").isNotNull(), "OBSERVATION")
                  .when(F.col("page").isNotNull(), "PAGINATION_METADATA").otherwise("OTHER").alias("t"))
         .groupBy("t").count().collect())
indep = {r.t: r["count"] for r in indep}
check("observations: text+variant vs spark.read.json", indep.get("OBSERVATION", 0), bronze_obs_df.count())
check("metadata: text+variant vs spark.read.json", indep.get("PAGINATION_METADATA", 0), bronze_meta_df.count())

# 7. Lossless value extraction: string value must round-trip to the same double the JSON reader produces
value_mismatch = (bronze_obs_df.select("source_file_path", "date", F.col("value").cast("double").alias("v_str"))
    .join(spark.read.json(valid_paths).filter("indicator is not null")
            .select(F.col("_metadata.file_path").alias("source_file_path"), "date", F.col("value").alias("v_json")),
          ["source_file_path", "date"], "full")
    .filter(~F.col("v_str").eqNullSafe(F.col("v_json"))).count())
check("value mismatches vs JSON reader", 0, value_mismatch)

checks_df = spark.createDataFrame(checks, "check STRING, expected STRING, actual STRING, status STRING")
display(checks_df)

failed = [c for c in checks if c[3] == "FAIL"]
if failed:
    logger.error(f"Step 2 validation failed: {failed}")
    raise PipelineError(f"Step 2 validation failed: {[c[0] for c in failed]}")
logger.info(f"Step 2 validation passed: {len(checks)} checks, "
            f"{sum(c[3] == 'WARN' for c in checks)} warning(s)")


# DBTITLE 1,3a - Explicit source schemas
# Explicit schemas derived from the Step 2 inference (schema_of_variant_agg) - not assumed.
VALUE_DECIMAL = T.DecimalType(38, 10)
CORRUPT_COL = "_corrupt_record"

_REF_SCHEMA = T.StructType([            # shape of both indicator{} and country{}
    T.StructField("id", T.StringType(), True),
    T.StructField("value", T.StringType(), True),
])

OBSERVATION_SCHEMA = T.StructType([
    T.StructField("indicator", _REF_SCHEMA, True),
    T.StructField("country", _REF_SCHEMA, True),
    T.StructField("countryiso3code", T.StringType(), True),
    T.StructField("date", T.StringType(), True),        # kept as text: World Bank periods can be YYYY, YYYYQn, YYYYMmm
    T.StructField("value", VALUE_DECIMAL, True),
    T.StructField("unit", T.StringType(), True),
    T.StructField("obs_status", T.StringType(), True),
    T.StructField("decimal", T.IntegerType(), True),
    T.StructField(CORRUPT_COL, T.StringType(), True),
])

METADATA_SCHEMA = T.StructType([
    T.StructField("page", T.IntegerType(), True),
    T.StructField("pages", T.IntegerType(), True),
    T.StructField("per_page", T.IntegerType(), True),
    T.StructField("total", T.LongType(), True),
    T.StructField("sourceid", T.StringType(), True),
    T.StructField("lastupdated", T.DateType(), True),
    T.StructField(CORRUPT_COL, T.StringType(), True),
])

JSON_OPTIONS = {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COL, "dateFormat": "yyyy-MM-dd"}

# Non-string fields: a non-null source value that parses to NULL is a type mismatch
OBS_TYPED_FIELDS = {"indicator": "$.indicator", "country": "$.country", "value": "$.value", "decimal": "$.decimal"}
META_TYPED_FIELDS = {"page": "$.page", "pages": "$.pages", "per_page": "$.per_page",
                     "total": "$.total", "lastupdated": "$.lastupdated"}
NESTED_STRUCTS = ["indicator", "country"]


def schema_keys(schema: T.StructType) -> list:
    return [f.name for f in schema.fields if f.name != CORRUPT_COL]


print("OBSERVATION_SCHEMA:\n" + OBSERVATION_SCHEMA.treeString())
print("METADATA_SCHEMA:\n" + METADATA_SCHEMA.treeString())

# COMMAND ----------

# DBTITLE 1,3b - Parse, flatten, capture errors and drift
EMPTY_STR_ARRAY = F.array().cast("array<string>")


def src_present(json_path: str):
    """True when the source JSON has a non-null value at json_path."""
    return F.expr(f"variant_get(parsed, '{json_path}', 'string')").isNotNull()


def type_mismatch_flags(typed_fields: dict) -> list:
    return [F.when(src_present(p) & F.col(f"j.{f}").isNull(), F.lit(f"TYPE_MISMATCH:{f}"))
            for f, p in typed_fields.items()]


def with_drift(df, schema: T.StructType, nested: list):
    expected = F.array(*map(F.lit, schema_keys(schema)))
    top = F.coalesce(F.col("top_keys"), EMPTY_STR_ARRAY)
    extra = [F.array_except(top, expected)]
    missing = [F.array_except(expected, top)]
    nested_expected = F.array(*map(F.lit, schema_keys(_REF_SCHEMA)))
    for n in nested:
        nk = F.coalesce(F.expr(f"json_object_keys(get_json_object(clean_line, '$.{n}'))"), EMPTY_STR_ARRAY)
        extra.append(F.transform(F.array_except(nk, nested_expected), lambda k, n=n: F.concat(F.lit(f"{n}."), k)))
        # only report nested missing keys when the parent object itself exists
        missing.append(F.when(src_present(f"$.{n}"),
                              F.transform(F.array_except(nested_expected, nk), lambda k, n=n: F.concat(F.lit(f"{n}."), k)))
                       .otherwise(EMPTY_STR_ARRAY))
    return (df.withColumn("drift_extra_keys", F.flatten(F.array(*extra)))
              .withColumn("drift_missing_keys", F.flatten(F.array(*missing))))


def strip_trailing_zeros(c):
    return F.regexp_replace(F.regexp_replace(c, r"(\.\d*?)0+$", "$1"), r"\.$", "")


# Lineage / ingestion columns carried on every output
LINEAGE = [
    F.col("source_file_path"),
    F.col("load_date").alias("ingestion_date"),                       # ADF load date (from file name)
    F.col("source_file_modified_ts").alias("ingestion_timestamp"),    # when ADF landed the file in Bronze
    F.lit(cfg.run_id).alias("pipeline_run_id"),
    F.lit(cfg.run_ts_utc).cast("timestamp").alias("processed_timestamp"),
]

# ---------------- Observations ----------------
value_text = F.expr("variant_get(parsed, '$.value', 'string')")      # exact source digits
precision_loss = (value_text.isNotNull() & F.col("j.value").isNotNull() &
    F.when(value_text.rlike(r"^-?\d+(\.\d+)?$"),
           strip_trailing_zeros(value_text) != strip_trailing_zeros(F.col("j.value").cast("string")))
     .otherwise(value_text.cast("double") != F.col("j.value").cast("double")))   # exponent notation

obs_parsed_df = with_drift(
    lines_df.filter(F.col("record_type") == "OBSERVATION")
            .withColumn("j", F.from_json("clean_line", OBSERVATION_SCHEMA, JSON_OPTIONS))
            .withColumn("parse_errors", F.array_compact(F.array(
                F.when(F.col(f"j.{CORRUPT_COL}").isNotNull(), F.lit("PARSE_FAILURE")),
                *type_mismatch_flags(OBS_TYPED_FIELDS),
                F.when(precision_loss, F.lit("PRECISION_LOSS"))))),
    OBSERVATION_SCHEMA, NESTED_STRUCTS)

observations_df = (obs_parsed_df.filter(F.size("parse_errors") == 0).select(
    F.col("j.indicator.id").alias("indicator_id"),
    F.col("j.indicator.value").alias("indicator_name"),
    F.col("j.country.id").alias("country_id"),              # World Bank 2-char code (ISO2 for countries)
    F.col("j.country.value").alias("country_name"),
    F.col("j.countryiso3code").alias("country_iso3_code"),
    F.col("j.date").alias("observation_period"),
    F.col("j.value").alias("observation_value"),
    F.col("j.unit").alias("unit"),
    F.col("j.obs_status").alias("obs_status"),
    F.col("j.decimal").alias("decimal_places"),
    "drift_extra_keys", "drift_missing_keys",
    *LINEAGE,
    F.col("clean_line").alias("raw_record")))

# ---------------- Pagination metadata ----------------
meta_parsed_df = with_drift(
    lines_df.filter(F.col("record_type") == "PAGINATION_METADATA")
            .withColumn("j", F.from_json("clean_line", METADATA_SCHEMA, JSON_OPTIONS))
            .withColumn("parse_errors", F.array_compact(F.array(
                F.when(F.col(f"j.{CORRUPT_COL}").isNotNull(), F.lit("PARSE_FAILURE")),
                *type_mismatch_flags(META_TYPED_FIELDS)))),
    METADATA_SCHEMA, [])

metadata_df = (meta_parsed_df.filter(F.size("parse_errors") == 0).select(
    F.col("j.page").alias("page"),
    F.col("j.pages").alias("total_pages"),
    F.col("j.per_page").alias("per_page"),
    F.col("j.total").alias("total_records"),
    F.col("j.sourceid").alias("source_id"),
    F.col("j.lastupdated").alias("source_last_updated_date"),
    "drift_extra_keys", "drift_missing_keys",
    *LINEAGE,
    F.col("clean_line").alias("raw_record")))

# ---------------- Parse errors (nothing is dropped silently) ----------------
ERROR_COLS = ["source_file_path", "ingestion_date", "ingestion_timestamp", "pipeline_run_id",
              "processed_timestamp", "record_type", "error_types", "raw_record"]


def to_error_rows(df, errors_col):
    return df.select(*LINEAGE, "record_type", errors_col.alias("error_types"),
                     F.col("raw_line").alias("raw_record")).select(*ERROR_COLS)


parse_errors_df = (
    to_error_rows(lines_df.filter(~F.col("record_type").isin("OBSERVATION", "PAGINATION_METADATA")),
                  F.array(F.col("record_type")))
    .unionByName(to_error_rows(obs_parsed_df.filter(F.size("parse_errors") > 0), F.col("parse_errors")))
    .unionByName(to_error_rows(meta_parsed_df.filter(F.size("parse_errors") > 0), F.col("parse_errors"))))

# ---------------- Drift warnings ----------------
for name, df in [("observations", observations_df), ("metadata", metadata_df)]:
    n = df.filter((F.size("drift_extra_keys") > 0) | (F.size("drift_missing_keys") > 0)).count()
    (logger.warning if n else logger.info)(f"Schema drift in {name}: {n} record(s)")

logger.info("Parsing plan built (lazy); nothing written.")

# COMMAND ----------

# DBTITLE 1,3c - Parsed schema, samples and counts
print("observations_df schema:")
observations_df.printSchema()
print("metadata_df schema:")
metadata_df.printSchema()

# Counts per file and output
counts_df = (lines_df.groupBy("source_file_path").agg(F.count("*").alias("bronze_lines"))
    .join(observations_df.groupBy("source_file_path").agg(F.count("*").alias("observations")), "source_file_path", "left")
    .join(metadata_df.groupBy("source_file_path").agg(F.count("*").alias("metadata_records"),
                                                       F.max("total_records").alias("api_total_records")),
          "source_file_path", "left")
    .join(parse_errors_df.groupBy("source_file_path").agg(F.count("*").alias("parse_errors")), "source_file_path", "left")
    .fillna(0, ["observations", "metadata_records", "parse_errors"]))   # counts only - absence of rows means 0
display(counts_df)

display(metadata_df)
display(observations_df.drop("raw_record").orderBy(F.col("observation_period").desc()).limit(10))

print(f"Parse errors: {parse_errors_df.count()}")
display(parse_errors_df)

# COMMAND ----------

# DBTITLE 1,3d - Step 3 validation
checks3 = []

def check3(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks3.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

n_lines = lines_df.count()
n_obs, n_meta, n_err = observations_df.count(), metadata_df.count(), parse_errors_df.count()

# 1. Conservation: every Bronze line ends up in exactly one output
check3("bronze lines == observations + metadata + errors", n_lines, n_obs + n_meta + n_err)
check3("parse errors", 0, n_err)

# 2. Reconciliation with the API's own totals, per file
recon3 = (observations_df.groupBy("source_file_path").agg(F.count("*").alias("obs"))
          .join(metadata_df.groupBy("source_file_path").agg(F.sum("total_records").alias("api_total"),
                                                             F.count("*").alias("meta_rows"),
                                                             F.max("total_pages").alias("pages")),
                "source_file_path", "full"))
check3("files where observations != api total or pages != metadata rows", 0,
       recon3.filter(~((F.col("obs") == F.col("api_total")) & (F.col("meta_rows") == F.col("pages")))).count())

# 3. Types are the declared ones
obs_types = {f.name: f.dataType for f in observations_df.schema.fields}
check3("observation_value type", VALUE_DECIMAL, obs_types["observation_value"])
check3("decimal_places type", T.IntegerType(), obs_types["decimal_places"])
check3("source_last_updated_date type", T.DateType(),
       {f.name: f.dataType for f in metadata_df.schema.fields}["source_last_updated_date"])

# 4. Independent precision check: Spark JSON reader with prefersDecimal infers exact decimals (different parser)
indep_df = (spark.read.option("prefersDecimal", "true").json(valid_paths)
            .filter("indicator is not null")
            .select(F.col("_metadata.file_path").alias("source_file_path"),
                    F.col("date").alias("observation_period"),
                    F.col("value").cast(VALUE_DECIMAL).alias("indep_value")))
print("Independent reader inferred value type:",
      spark.read.option("prefersDecimal", "true").json(valid_paths).schema["value"].dataType)
cmp_df = observations_df.select("source_file_path", "observation_period", "observation_value").join(
    indep_df, ["source_file_path", "observation_period"], "full")
check3("value mismatches vs independent decimal reader", 0,
       cmp_df.filter(~F.col("observation_value").eqNullSafe(F.col("indep_value"))).count())

# 5. Source NULLs preserved as NULL (no zero-filling), no NULLs introduced
src_null_values = lines_df.filter((F.col("record_type") == "OBSERVATION") &
                                  F.expr("variant_get(parsed, '$.value', 'string')").isNull()).count()
check3("NULL observation_value == NULL in source", src_null_values,
       observations_df.filter(F.col("observation_value").isNull()).count())

# 6. Lineage completeness
for c in ["source_file_path", "ingestion_date", "ingestion_timestamp", "pipeline_run_id", "processed_timestamp"]:
    check3(f"NULL {c} in observations", 0, observations_df.filter(F.col(c).isNull()).count())

# 7. Grain check (informational at this stage; enforced in the data quality step)
grain = ["indicator_id", "country_iso3_code", "observation_period", "ingestion_date"]
check3("duplicate business key + ingestion_date", 0,
       n_obs - observations_df.select(*grain).distinct().count(), critical=False)

# 8. Drift (warning only - records are retained)
check3("observations with schema drift", 0,
       observations_df.filter((F.size("drift_extra_keys") > 0) | (F.size("drift_missing_keys") > 0)).count(), critical=False)

# 9. Bronze untouched and nothing written to Silver
current_mod = {f.path: f.modificationTime for f in list_files_recursive(cfg.bronze_root)}
check3("Bronze files modified since inventory", 0,
       sum(current_mod.get(r["file_path"]) != int(r["modified_ts"].timestamp() * 1000) for r in file_records))
# Silver tables now exist (Step 8); Step 3 must not have written to any of them during this run
silver_delta_tables = [r.table_name for r in spark.sql(f"""
    SELECT table_name FROM `{cfg.catalog}`.information_schema.tables
    WHERE table_schema = '{cfg.silver_schema}' AND table_type <> 'VIEW'""").collect()]
check3("Silver tables written during this run", 0, sum(
    spark.sql(f"DESCRIBE HISTORY {cfg.schema_fqn}.`{t}`")
         .filter(F.col("timestamp") >= F.lit(cfg.run_ts_utc).cast("timestamp")).count() > 0
    for t in silver_delta_tables))

checks3_df = spark.createDataFrame(checks3, "check STRING, expected STRING, actual STRING, status STRING")
display(checks3_df)

failed3 = [c for c in checks3 if c[3] == "FAIL"]
if failed3:
    logger.error(f"Step 3 validation failed: {failed3}")
    raise PipelineError(f"Step 3 validation failed: {[c[0] for c in failed3]}")
logger.info(f"Step 3 validation passed: {len(checks3)} checks, {sum(c[3] == 'WARN' for c in checks3)} warning(s)")



# DBTITLE 1,4a - Build metadata and GDP observation DataFrames
GDP_INDICATOR_ID = "NY.GDP.MKTP.CD"            # observed indicator.id in Bronze: "GDP (current US$)"
EMPTY_AS_NULL_COLS = ["unit", "obs_status"]     # empty string -> NULL (approved by data owner)
ANNUAL_PERIOD_RE = r"^\d{4}$"
LINEAGE_OUT = ["source_file_path", "ingestion_date", "ingestion_timestamp", "pipeline_run_id", "processed_timestamp"]


def empty_to_null(col_name: str):
    """Empty / whitespace-only -> NULL; any other value is kept exactly as received."""
    return F.when(F.trim(F.col(col_name)) == "", F.lit(None).cast("string")).otherwise(F.col(col_name))


# ---------------- Standardised observations (all indicators) ----------------
standardised_obs_df = observations_df.select(
    "indicator_id", "indicator_name",
    "country_iso3_code", "country_name", "country_id",
    F.when(F.col("observation_period").rlike(ANNUAL_PERIOD_RE),
           F.col("observation_period").cast("int")).alias("year"),
    "observation_period",
    "observation_value",
    *[empty_to_null(c).alias(c) for c in EMPTY_AS_NULL_COLS],
    "decimal_places",
    "drift_extra_keys", "drift_missing_keys",
    *LINEAGE_OUT,
)

# ---------------- GDP observations vs. any other indicator (nothing dropped) ----------------
gdp_observations_df = (standardised_obs_df
    .filter(F.col("indicator_id") == GDP_INDICATOR_ID)
    .withColumnRenamed("observation_value", "gdp_value"))

non_gdp_observations_df = standardised_obs_df.filter(~F.col("indicator_id").eqNullSafe(GDP_INDICATOR_ID))

# ---------------- Per-file reconciliation ----------------
file_reconciliation_df = (
    valid_inventory_df.select("source_file_path", "path_indicator_code", "path_country_iso3",
                              F.col("load_date").alias("ingestion_date"))
    .join(metadata_df.groupBy("source_file_path").agg(
            F.count("*").alias("metadata_pages_received"),
            F.max("total_pages").alias("api_total_pages"),
            F.max("total_records").alias("api_total_records"),
            F.countDistinct("total_records").alias("distinct_api_totals")), "source_file_path", "left")
    .join(observations_df.groupBy("source_file_path").agg(F.count("*").alias("valid_observations")),
          "source_file_path", "left")
    .join(gdp_observations_df.groupBy("source_file_path").agg(F.count("*").alias("gdp_observations")),
          "source_file_path", "left")
    .join(parse_errors_df.filter(F.col("record_type") == "OBSERVATION")
            .groupBy("source_file_path").agg(F.count("*").alias("observation_parse_errors")),
          "source_file_path", "left")
    # absent group = zero rows of that kind (count semantics, not missing business data)
    .fillna(0, ["metadata_pages_received", "valid_observations", "gdp_observations", "observation_parse_errors"])
    .withColumn("reconciliation_status",
        F.when(F.col("api_total_records").isNull(), "NO_METADATA")
         .when(F.col("distinct_api_totals") > 1, "INCONSISTENT_METADATA")
         .when(F.col("metadata_pages_received") != F.col("api_total_pages"), "PAGES_INCOMPLETE")
         .when(F.col("valid_observations") + F.col("observation_parse_errors") != F.col("api_total_records"), "COUNT_MISMATCH")
         .when(F.col("observation_parse_errors") > 0, "MATCHED_WITH_ERRORS")
         .otherwise("MATCHED")))

# ---------------- Metadata for audit (page level + file reconciliation) ----------------
load_metadata_df = (metadata_df
    .select("page", "total_pages", "per_page", "total_records", "source_id", "source_last_updated_date",
            "drift_extra_keys", "drift_missing_keys", *LINEAGE_OUT, "raw_record")
    .join(file_reconciliation_df.select("source_file_path", "path_indicator_code", "path_country_iso3",
                                        "valid_observations", "observation_parse_errors", "reconciliation_status"),
          "source_file_path", "left"))

logger.info("Step 4 DataFrames built (lazy): load_metadata_df, gdp_observations_df, "
            "non_gdp_observations_df, file_reconciliation_df; nothing written.")

# COMMAND ----------

# DBTITLE 1,4b - Schemas, samples and reconciliation
print("load_metadata_df schema:")
load_metadata_df.printSchema()
print("gdp_observations_df schema:")
gdp_observations_df.printSchema()

display(load_metadata_df)
display(gdp_observations_df.orderBy(F.col("year").desc_nulls_first()).limit(10))
display(gdp_observations_df.orderBy(F.col("year").asc_nulls_first()).limit(5))

print(f"non_gdp_observations_df rows: {non_gdp_observations_df.count()}")
display(file_reconciliation_df)

# COMMAND ----------

# DBTITLE 1,4c - Step 4 validation
checks4 = []

def check4(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks4.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

n_lines = lines_df.count()
n_gdp, n_non_gdp = gdp_observations_df.count(), non_gdp_observations_df.count()
n_meta, n_err, n_obs = load_metadata_df.count(), parse_errors_df.count(), observations_df.count()

# 1. Nothing discarded
check4("bronze lines == gdp + non-gdp + metadata + parse errors", n_lines, n_gdp + n_non_gdp + n_meta + n_err)
check4("gdp + non-gdp == parsed observations", n_obs, n_gdp + n_non_gdp)
check4("metadata rows == PAGINATION_METADATA lines",
       lines_df.filter(F.col("record_type") == "PAGINATION_METADATA").count(), n_meta)
check4("non-GDP observations", 0, n_non_gdp, critical=False)

# 2. Per-file reconciliation with the API total (every valid file must be MATCHED)
check4("files in reconciliation == valid Bronze files", len(valid_paths), file_reconciliation_df.count())
check4("files not MATCHED", 0, file_reconciliation_df.filter(F.col("reconciliation_status") != "MATCHED").count())
check4("files where gdp_observations != api_total_records", 0,
       file_reconciliation_df.filter(~F.col("gdp_observations").eqNullSafe(F.col("api_total_records"))).count())

# 3. Empty string -> NULL, verified independently against the raw VARIANT parse
src = lines_df.filter(F.col("record_type") == "OBSERVATION")
for c in EMPTY_AS_NULL_COLS:
    raw_c = F.expr(f"variant_get(parsed, '$.{c}', 'string')")
    expected_nulls = src.filter(raw_c.isNull() | (F.trim(raw_c) == "")).count()
    expected_non_null = sorted(r[0] for r in src.filter(F.trim(raw_c) != "").select(raw_c).distinct().collect())
    out = gdp_observations_df.unionByName(non_gdp_observations_df.withColumnRenamed("observation_value", "gdp_value"))
    check4(f"{c}: remaining empty strings", 0, out.filter(F.trim(c) == "").count())
    check4(f"{c}: NULLs == empty-or-null in source", expected_nulls, out.filter(F.col(c).isNull()).count())
    check4(f"{c}: non-empty values unchanged", expected_non_null,
           sorted(r[0] for r in out.filter(F.col(c).isNotNull()).select(c).distinct().collect()))

# 4. Year derivation
check4("annual periods with NULL year", 0,
       gdp_observations_df.filter(F.col("observation_period").rlike(ANNUAL_PERIOD_RE) & F.col("year").isNull()).count())
check4("year != observation_period", 0,
       gdp_observations_df.filter(F.col("year").isNotNull() &
                                  (F.col("year").cast("string") != F.col("observation_period"))).count())
check4("non-annual periods (year NULL, period kept)", 0,
       gdp_observations_df.filter(F.col("year").isNull()).count(), critical=False)

# 5. GDP values carried unchanged from the parsed observations
sum_parsed = observations_df.filter(F.col("indicator_id") == GDP_INDICATOR_ID).agg(F.sum("observation_value")).first()[0]
check4("sum(gdp_value) == sum(parsed observation_value)", sum_parsed, gdp_observations_df.agg(F.sum("gdp_value")).first()[0])
check4("gdp_value type", VALUE_DECIMAL, gdp_observations_df.schema["gdp_value"].dataType)

# 6. Grain: business key + ingestion_date unique
grain4 = ["indicator_id", "country_iso3_code", "observation_period", "ingestion_date"]
check4("duplicate business key + ingestion_date", 0, n_gdp - gdp_observations_df.select(*grain4).distinct().count())

# 7. Lineage present on both outputs
for name, df in [("gdp_observations_df", gdp_observations_df), ("load_metadata_df", load_metadata_df)]:
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in LINEAGE_OUT]).first().asDict()
    check4(f"{name}: NULL lineage values", 0, sum(nulls.values()))

# 8. Nothing written to Silver during this run (Silver tables exist since Step 8)
silver_delta_tables = [r.table_name for r in spark.sql(f"""
    SELECT table_name FROM `{cfg.catalog}`.information_schema.tables
    WHERE table_schema = '{cfg.silver_schema}' AND table_type <> 'VIEW'""").collect()]
check4("Silver tables written during this run", 0, sum(
    spark.sql(f"DESCRIBE HISTORY {cfg.schema_fqn}.`{t}`")
         .filter(F.col("timestamp") >= F.lit(cfg.run_ts_utc).cast("timestamp")).count() > 0
    for t in silver_delta_tables))

display(gdp_observations_df.agg(F.min("year").alias("min_year"), F.max("year").alias("max_year"),
                                F.countDistinct("year").alias("distinct_years"),
                                F.sum(F.col("gdp_value").isNull().cast("int")).alias("null_gdp_values")))

checks4_df = spark.createDataFrame(checks4, "check STRING, expected STRING, actual STRING, status STRING")
display(checks4_df)

failed4 = [c for c in checks4 if c[3] == "FAIL"]
if failed4:
    logger.error(f"Step 4 validation failed: {failed4}")
    raise PipelineError(f"Step 4 validation failed: {[c[0] for c in failed4]}")
logger.info(f"Step 4 validation passed: {len(checks4)} checks, {sum(c[3] == 'WARN' for c in checks4)} warning(s)")



# DBTITLE 1,5a - Per-file profiling (observations, pagination, errors)
BUSINESS_KEY = ["indicator_id", "country_iso3_code", "observation_period", "ingestion_date"]
CURRENT_YEAR = cfg.run_ts_utc.year
EMPTY_INT_ARRAY = F.array().cast("array<int>")

# Path identifiers attached to every GDP observation (from the Step 2 inventory)
gdp_with_path_df = gdp_observations_df.join(
    F.broadcast(valid_inventory_df.select("source_file_path", "path_indicator_code", "path_country_iso3")),
    "source_file_path", "left")

# ---------------- Observation profile per file ----------------
obs_profile_df = (gdp_with_path_df.groupBy("source_file_path").agg(
        F.count("*").alias("actual_count"),
        F.count_distinct(F.struct(*BUSINESS_KEY)).alias("distinct_business_keys"),
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
        F.collect_set("year").alias("years_present"),
        F.sum(F.col("gdp_value").isNull().cast("int")).alias("null_gdp_values"),
        F.sum((F.col("gdp_value") < 0).cast("int")).alias("negative_gdp_values"),
        F.sum((F.col("gdp_value") == 0).cast("int")).alias("zero_gdp_values"),
        F.sum(F.col("year").isNull().cast("int")).alias("non_annual_periods"),
        F.sum((F.col("year") > CURRENT_YEAR).cast("int")).alias("future_years"),
        F.sum((~F.col("indicator_id").eqNullSafe(F.col("path_indicator_code"))).cast("int")).alias("indicator_path_mismatches"),
        F.sum((~F.col("country_iso3_code").eqNullSafe(F.col("path_country_iso3"))).cast("int")).alias("iso3_path_mismatches"),
        F.count_distinct("indicator_name").alias("distinct_indicator_names"),
        F.count_distinct("country_name").alias("distinct_country_names"),
        F.sum(((F.size("drift_extra_keys") > 0) | (F.size("drift_missing_keys") > 0)).cast("int")).alias("drift_records"))
    .withColumn("duplicate_records", F.col("actual_count") - F.col("distinct_business_keys"))
    .withColumn("missing_years", F.when(F.col("min_year").isNotNull(),
        F.array_sort(F.array_except(F.sequence("min_year", "max_year"), "years_present"))).otherwise(EMPTY_INT_ARRAY))
    .drop("years_present"))

# ---------------- Pagination profile per file ----------------
expected_pages = F.when(F.col("total_records") == 0, F.col("total_pages")).otherwise(
    F.ceil(F.col("total_records") / F.col("per_page")))
page_row_valid = F.coalesce(
    (F.col("page") >= 1) & (F.col("total_pages") >= 0) & (F.col("page") <= F.greatest(F.col("total_pages"), F.lit(1))) &
    (F.col("per_page") > 0) & (F.col("total_records") >= 0) & (F.col("total_pages") == expected_pages) &
    F.col("source_id").isNotNull(),
    F.lit(False))

pagination_df = (load_metadata_df.withColumn("page_row_valid", page_row_valid)
    .groupBy("source_file_path").agg(
        F.count("*").alias("metadata_rows"),
        F.max("total_records").alias("expected_count"),
        F.max("total_pages").alias("api_pages"),
        F.max("per_page").alias("api_per_page"),
        F.array_sort(F.collect_set("page")).alias("pages_received"),
        F.count_distinct("page").alias("distinct_pages"),
        F.sum((~F.col("page_row_valid")).cast("int")).alias("invalid_page_rows"),
        (F.count_distinct("total_records") + F.count_distinct("total_pages") + F.count_distinct("per_page")
         + F.count_distinct("source_last_updated_date")).alias("_distinct_attr_sum"),
        F.max("source_last_updated_date").alias("source_last_updated_date"),
        F.max("ingestion_date").alias("_meta_ingestion_date"))
    .withColumn("pagination_inconsistent", F.col("_distinct_attr_sum") != 4)
    .withColumn("pages_complete", F.col("pages_received") ==
                F.when(F.col("api_pages") >= 1, F.sequence(F.lit(1), F.col("api_pages"))).otherwise(F.array(F.lit(1))))
    .withColumn("duplicate_pages", F.col("metadata_rows") - F.col("distinct_pages"))
    .withColumn("capacity_exceeded", F.col("expected_count") > F.col("api_pages") * F.col("api_per_page"))
    .withColumn("lastupdated_after_load", F.col("source_last_updated_date") > F.col("_meta_ingestion_date"))
    .drop("_distinct_attr_sum", "_meta_ingestion_date"))

# ---------------- Malformed / unexpected records per file ----------------
errors_profile_df = parse_errors_df.groupBy("source_file_path").agg(
    F.count("*").alias("parse_errors"),
    F.array_sort(F.array_distinct(F.flatten(F.collect_list("error_types")))).alias("parse_error_types"))
non_gdp_profile_df = non_gdp_observations_df.groupBy("source_file_path").agg(F.count("*").alias("non_gdp_records"))

# Duplicate business keys, kept for investigation (nothing removed)
duplicate_keys_df = (gdp_with_path_df.groupBy("source_file_path", *BUSINESS_KEY).count()
                     .filter(F.col("count") > 1).withColumnRenamed("count", "occurrences"))

logger.info("Step 5 profiles built (lazy).")

# COMMAND ----------

# DBTITLE 1,5b - Reconciliation report
def issue(ok_cond, code):
    """Emit `code` when the passing condition `ok_cond` is not TRUE (NULL = cannot verify = issue)."""
    return F.when(~F.coalesce(ok_cond, F.lit(False)), F.lit(code))


report_base_df = (valid_inventory_df
    .select("source_file_path", F.col("path_indicator_code").alias("indicator_code"),
            F.col("path_country_iso3").alias("country_iso3"), F.col("load_date").alias("ingestion_date"))
    .join(pagination_df, "source_file_path", "left")
    .join(obs_profile_df, "source_file_path", "left")
    .join(errors_profile_df, "source_file_path", "left")
    .join(non_gdp_profile_df, "source_file_path", "left")
    # count semantics: no group = zero rows of that kind
    .fillna(0, ["metadata_rows", "actual_count", "duplicate_records", "parse_errors", "non_gdp_records",
                "null_gdp_values", "negative_gdp_values", "zero_gdp_values", "non_annual_periods", "future_years",
                "indicator_path_mismatches", "iso3_path_mismatches", "drift_records"])
    .withColumn("missing_records", F.greatest(F.col("expected_count") - F.col("actual_count"), F.lit(0)))
    .withColumn("excess_records", F.greatest(F.col("actual_count") - F.col("expected_count"), F.lit(0))))

critical_issues = F.array_compact(F.array(
    F.when(F.col("metadata_rows") == 0, F.lit("NO_METADATA")),
    issue(F.col("actual_count") == F.col("expected_count"), "COUNT_MISMATCH"),
    issue(F.col("invalid_page_rows") == 0, "INVALID_PAGINATION_VALUES"),
    issue(~F.col("pagination_inconsistent"), "INCONSISTENT_PAGINATION"),
    issue(F.col("pages_complete"), "INCOMPLETE_PAGES"),
    F.when(F.col("duplicate_pages") > 0, F.lit("DUPLICATE_PAGES")),
    F.when(F.col("capacity_exceeded"), F.lit("TOTAL_EXCEEDS_PAGE_CAPACITY")),
    F.when(F.col("duplicate_records") > 0, F.lit("DUPLICATE_BUSINESS_KEYS")),
    F.when(F.col("indicator_path_mismatches") > 0, F.lit("INDICATOR_PATH_MISMATCH")),
    F.when(F.col("iso3_path_mismatches") > 0, F.lit("ISO3_PATH_MISMATCH")),
    F.when(F.col("parse_errors") > 0, F.lit("PARSE_ERRORS")),
    F.when(F.col("non_gdp_records") > 0, F.lit("UNEXPECTED_INDICATOR_RECORDS"))))

warning_issues = F.array_compact(F.array(
    F.when(F.size("missing_years") > 0, F.lit("MISSING_YEARS")),
    F.when(F.col("null_gdp_values") > 0, F.lit("NULL_GDP_VALUES")),
    F.when(F.col("negative_gdp_values") > 0, F.lit("NEGATIVE_GDP_VALUES")),
    F.when(F.col("zero_gdp_values") > 0, F.lit("ZERO_GDP_VALUES")),
    F.when(F.col("non_annual_periods") > 0, F.lit("NON_ANNUAL_PERIODS")),
    F.when(F.col("future_years") > 0, F.lit("FUTURE_YEARS")),
    F.when(F.col("drift_records") > 0, F.lit("SCHEMA_DRIFT")),
    F.when((F.col("distinct_indicator_names") > 1) | (F.col("distinct_country_names") > 1), F.lit("MULTIPLE_NAMES")),
    F.when(F.col("lastupdated_after_load"), F.lit("LASTUPDATED_AFTER_LOAD"))))

reconciliation_report_df = (report_base_df
    .withColumn("critical_issues", critical_issues)
    .withColumn("warnings", warning_issues)
    .withColumn("validation_status", F.when(F.size("critical_issues") > 0, "FAIL")
                                      .when(F.size("warnings") > 0, "WARN").otherwise("PASS"))
    .select("source_file_path", "indicator_code", "country_iso3", "ingestion_date",
            "expected_count", "actual_count", "missing_records", "excess_records", "duplicate_records",
            "parse_errors", "non_gdp_records",
            "metadata_rows", "pages_received", "api_pages", "api_per_page", "invalid_page_rows", "source_last_updated_date",
            "min_year", "max_year", "missing_years",
            "null_gdp_values", "negative_gdp_values", "zero_gdp_values",
            "indicator_path_mismatches", "iso3_path_mismatches",
            "validation_status", "critical_issues", "warnings"))

# Files the inventory excluded must not disappear silently
excluded_files_df = inventory_df.filter(~F.col("status").isin("VALID", "IGNORED_SYSTEM_FILE"))
ignored_system_files = inventory_df.filter(F.col("status") == "IGNORED_SYSTEM_FILE").count()

display(reconciliation_report_df)
print(f"Excluded Bronze files (critical if > 0): {excluded_files_df.count()} | ignored system/marker files: {ignored_system_files}")
if excluded_files_df.count():
    display(excluded_files_df)
print(f"Duplicate business keys: {duplicate_keys_df.count()}")
if duplicate_keys_df.count():
    display(duplicate_keys_df)

# COMMAND ----------

# DBTITLE 1,5c - Step 5 validation gate
checks5 = []

def check5(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks5.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

report_rows = reconciliation_report_df.collect()

# 1. Report coverage
check5("files in report == valid Bronze files", sorted(valid_paths), sorted(r.source_file_path for r in report_rows))
check5("Bronze files excluded by inventory", 0, excluded_files_df.count())

# 2. Per-file outcome
check5("files with status FAIL", 0, sum(r.validation_status == "FAIL" for r in report_rows))
check5("files with status WARN", 0, sum(r.validation_status == "WARN" for r in report_rows), critical=False)

# 3. Independent recount straight from the Bronze line classification (different code path)
raw_obs_counts = {r[0]: r[1] for r in lines_df.filter(F.col("record_type") == "OBSERVATION")
                  .groupBy("source_file_path").count().collect()}
raw_totals = {r[0]: r[1] for r in lines_df.filter(F.col("record_type") == "PAGINATION_METADATA")
              .groupBy("source_file_path").agg(F.max(F.expr("variant_get(parsed, '$.total', 'bigint')"))).collect()}
check5("report actual_count == raw OBSERVATION lines", raw_obs_counts, {r.source_file_path: r.actual_count for r in report_rows})
check5("report expected_count == raw meta.total", raw_totals, {r.source_file_path: r.expected_count for r in report_rows})

# 4. Independent missing-years computation in plain Python
for r in report_rows:
    yrs = {x[0] for x in gdp_observations_df.filter(F.col("source_file_path") == r.source_file_path)
           .select("year").where(F.col("year").isNotNull()).collect()}
    py_missing = sorted(set(range(min(yrs), max(yrs) + 1)) - yrs) if yrs else []
    check5(f"missing years match Python ({r.country_iso3}, {r.ingestion_date})", py_missing, list(r.missing_years or []))

# 5. Global totals
check5("global duplicate business keys", 0, duplicate_keys_df.count())
check5("global parse errors", 0, parse_errors_df.count())
# Nothing written to Silver during this run (Silver tables exist since Step 8)
silver_delta_tables = [r.table_name for r in spark.sql(f"""
    SELECT table_name FROM `{cfg.catalog}`.information_schema.tables
    WHERE table_schema = '{cfg.silver_schema}' AND table_type <> 'VIEW'""").collect()]
check5("Silver tables written during this run", 0, sum(
    spark.sql(f"DESCRIBE HISTORY {cfg.schema_fqn}.`{t}`")
         .filter(F.col("timestamp") >= F.lit(cfg.run_ts_utc).cast("timestamp")).count() > 0
    for t in silver_delta_tables))

checks5_df = spark.createDataFrame(checks5, "check STRING, expected STRING, actual STRING, status STRING")
display(checks5_df)

failed5 = [c for c in checks5 if c[3] == "FAIL"]
for r in report_rows:
    if r.validation_status == "FAIL":
        logger.error(f"CRITICAL {r.source_file_path}: {list(r.critical_issues)}")
    if r.warnings:
        logger.warning(f"WARN {r.source_file_path}: {list(r.warnings)}")
if failed5:
    logger.error(f"Step 5 reconciliation failed - pipeline stopped: {[c[0] for c in failed5]}")
    raise PipelineError(f"Step 5 reconciliation failed: {[c[0] for c in failed5]}")
logger.info(f"Step 5 reconciliation passed: {len(checks5)} checks, {sum(c[3] == 'WARN' for c in checks5)} warning(s)")



# DBTITLE 1,6a - Silver contract and standardisation
GDP_DECIMAL = VALUE_DECIMAL     # DECIMAL(38,10) - validated lossless in Steps 3-5

SILVER_GDP_SCHEMA = T.StructType([
    T.StructField("record_key", T.StringType(), False),
    T.StructField("indicator_id", T.StringType(), False),
    T.StructField("indicator_name", T.StringType(), True),
    T.StructField("country_iso3_code", T.StringType(), False),
    T.StructField("country_id", T.StringType(), True),
    T.StructField("country_name", T.StringType(), True),
    T.StructField("year", T.IntegerType(), True),
    T.StructField("observation_period", T.StringType(), False),
    T.StructField("gdp_value", GDP_DECIMAL, True),
    T.StructField("unit", T.StringType(), True),
    T.StructField("obs_status", T.StringType(), True),
    T.StructField("decimal_places", T.IntegerType(), True),
    T.StructField("source_last_updated_date", T.DateType(), True),
    T.StructField("dq_flags", T.ArrayType(T.StringType()), True),
    T.StructField("drift_extra_keys", T.ArrayType(T.StringType()), True),
    T.StructField("drift_missing_keys", T.ArrayType(T.StringType()), True),
    T.StructField("record_hash", T.StringType(), False),
    T.StructField("source_file_path", T.StringType(), False),
    T.StructField("ingestion_date", T.DateType(), False),
    T.StructField("ingestion_timestamp", T.TimestampType(), False),
    T.StructField("pipeline_run_id", T.StringType(), False),
    T.StructField("processed_timestamp", T.TimestampType(), False),
])
KEY_COLS = ["indicator_id", "country_iso3_code", "observation_period", "ingestion_date"]
HASH_COLS = ["indicator_name", "country_id", "country_name", "year", "gdp_value", "unit", "obs_status",
             "decimal_places", "source_last_updated_date"]
DERIVED_COLS = {"record_key", "record_hash", "dq_flags", "source_last_updated_date"}


# ---------------- Schema-variation guard on the input ----------------
def conform_input(df):
    """Add missing contract columns as typed NULL (warn); fail on type conflicts (no silent casts)."""
    in_types = {f.name: f.dataType for f in df.schema.fields}
    out = df
    for f in SILVER_GDP_SCHEMA.fields:
        if f.name in DERIVED_COLS:
            continue
        if f.name not in in_types:
            logger.warning(f"Input column '{f.name}' missing - added as NULL {f.dataType.simpleString()}")
            out = out.withColumn(f.name, F.lit(None).cast(f.dataType))
        elif in_types[f.name] != f.dataType:
            raise PipelineError(f"Type conflict on '{f.name}': input {in_types[f.name].simpleString()} "
                                f"vs contract {f.dataType.simpleString()}")
    return out


def std_code(c: str):
    """Identifier standardisation: trim + upper-case; empty -> NULL."""
    v = F.upper(F.trim(F.col(c)))
    return F.when(v == "", F.lit(None).cast("string")).otherwise(v)


def std_text(c: str):
    """Business text: trim only (case preserved); empty -> NULL."""
    v = F.trim(F.col(c))
    return F.when(v == "", F.lit(None).cast("string")).otherwise(v)


file_release_df = load_metadata_df.groupBy("source_file_path").agg(
    F.max("source_last_updated_date").alias("source_last_updated_date"))

standardised_df = (conform_input(gdp_observations_df)
    .withColumn("indicator_id", std_code("indicator_id"))
    .withColumn("country_iso3_code", std_code("country_iso3_code"))
    .withColumn("country_id", std_code("country_id"))
    .withColumn("indicator_name", std_text("indicator_name"))
    .withColumn("country_name", std_text("country_name"))
    .withColumn("observation_period", std_text("observation_period"))
    .withColumn("unit", empty_to_null("unit"))                  # idempotent re-application (Step 4 rule)
    .withColumn("obs_status", empty_to_null("obs_status"))
    .withColumn("year", F.when(F.col("observation_period").rlike(ANNUAL_PERIOD_RE),
                               F.col("observation_period").cast("int")))
    .join(F.broadcast(file_release_df), "source_file_path", "left")
    .withColumn("reject_reasons", F.array_compact(F.array(
        *[F.when(F.col(c).isNull(), F.lit(f"NULL_{c.upper()}")) for c in KEY_COLS])))
    .withColumn("dq_flags", F.array_compact(F.array(
        F.when(F.col("gdp_value").isNull(), F.lit("NULL_GDP_VALUE")),
        F.when(F.col("gdp_value") < 0, F.lit("NEGATIVE_GDP_VALUE")),
        F.when(F.col("gdp_value") == 0, F.lit("ZERO_GDP_VALUE")),
        F.when(F.col("year").isNull(), F.lit("NON_ANNUAL_PERIOD")),
        F.when(~F.coalesce(F.col("country_iso3_code").rlike(r"^[A-Z]{3}$"), F.lit(False)), F.lit("ISO3_FORMAT")),
        F.when((F.size("drift_extra_keys") > 0) | (F.size("drift_missing_keys") > 0), F.lit("SCHEMA_DRIFT"))))
        .cast(SILVER_GDP_SCHEMA["dq_flags"].dataType))      # array_compact yields containsNull=false; align to contract
    .withColumn("record_key", F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in KEY_COLS]), 256))
    .withColumn("record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in HASH_COLS])), 256)))

silver_gdp_df = (standardised_df.filter(F.size("reject_reasons") == 0)
                 .select(*[F.col(f.name) for f in SILVER_GDP_SCHEMA.fields]))

silver_gdp_rejects_df = (standardised_df.filter(F.size("reject_reasons") > 0)
                         .select("reject_reasons", *[F.col(f.name) for f in SILVER_GDP_SCHEMA.fields]))

# Contract enforcement (names, order, types)
actual_contract = [(f.name, f.dataType) for f in silver_gdp_df.schema.fields]
expected_contract = [(f.name, f.dataType) for f in SILVER_GDP_SCHEMA.fields]
if actual_contract != expected_contract:
    raise PipelineError(f"silver_gdp_df does not match SILVER_GDP_SCHEMA: {actual_contract}")
logger.info("silver_gdp_df built and matches SILVER_GDP_SCHEMA (lazy; nothing written).")

# COMMAND ----------

# DBTITLE 1,6b - Final schema, samples and DQ flag summary
print("silver_gdp_df schema:")
silver_gdp_df.printSchema()

display(silver_gdp_df.orderBy(F.col("year").desc()).limit(10))
display(silver_gdp_df.orderBy(F.col("year").asc()).limit(5))

# DQ flags (records kept, only flagged) and snapshot overview
display(silver_gdp_df.select(F.explode_outer("dq_flags").alias("dq_flag")).groupBy("dq_flag").count())
display(silver_gdp_df.groupBy("indicator_id", "country_iso3_code", "ingestion_date", "source_last_updated_date")
        .agg(F.count("*").alias("observations"), F.min("year").alias("min_year"), F.max("year").alias("max_year")))

print(f"silver_gdp_rejects_df rows: {silver_gdp_rejects_df.count()}")
display(silver_gdp_rejects_df)

# COMMAND ----------

# DBTITLE 1,6c - Before/after validation
checks6 = []

def check6(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks6.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

before_df = gdp_observations_df
after_all_df = silver_gdp_df.unionByName(silver_gdp_rejects_df.drop("reject_reasons"))
n_before, n_silver, n_rej = before_df.count(), silver_gdp_df.count(), silver_gdp_rejects_df.count()

# 1. No observation lost or duplicated
check6("before == silver + rejects", n_before, n_silver + n_rej)
check6("rejected records (kept in rejects_df)", 0, n_rej, critical=False)
row_id = ["source_file_path", "ingestion_date", "observation_period"]     # unchanged by standardisation
check6("before rows missing after", 0, before_df.join(after_all_df, row_id, "left_anti").count())
check6("after rows not in before", 0, after_all_df.join(before_df, row_id, "left_anti").count())

# 2. GDP precision - row level vs previous step, and vs raw Bronze text (independent path)
cmp6 = before_df.select(*row_id, F.col("gdp_value").alias("v_before")).join(
    silver_gdp_df.select(*row_id, F.col("gdp_value").alias("v_after")), row_id, "inner")
check6("gdp_value changed vs before", 0, cmp6.filter(~F.col("v_before").eqNullSafe(F.col("v_after"))).count())
raw_text_df = (lines_df.filter(F.col("record_type") == "OBSERVATION")
               .select("source_file_path", F.col("load_date").alias("ingestion_date"),
                       F.expr("variant_get(parsed, '$.date', 'string')").alias("observation_period"),
                       F.expr("variant_get(parsed, '$.value', 'string')").alias("raw_value_text")))
raw_cmp = raw_text_df.join(silver_gdp_df.select(*row_id, "gdp_value"), row_id, "inner")
check6("rows compared with raw Bronze text", n_silver, raw_cmp.count())
check6("gdp_value differs from raw Bronze digits", 0, raw_cmp.filter(
    ~strip_trailing_zeros(F.col("raw_value_text")).eqNullSafe(strip_trailing_zeros(F.col("gdp_value").cast("string")))).count())
sum_before = before_df.agg(F.sum("gdp_value")).first()[0]
check6("sum(gdp_value) before == after", sum_before, after_all_df.agg(F.sum("gdp_value")).first()[0])
check6("NULL gdp_value before == after", before_df.filter(F.col("gdp_value").isNull()).count(),
       after_all_df.filter(F.col("gdp_value").isNull()).count())

# 3. Types and contract
check6("schema == SILVER_GDP_SCHEMA", expected_contract, actual_contract)
check6("year type", T.IntegerType(), silver_gdp_df.schema["year"].dataType)
check6("gdp_value type", GDP_DECIMAL, silver_gdp_df.schema["gdp_value"].dataType)
check6("annual periods where year != period", 0, silver_gdp_df.filter(
    F.col("observation_period").rlike(ANNUAL_PERIOD_RE) &
    ~F.col("year").cast("string").eqNullSafe(F.col("observation_period"))).count())

# 4. Empty strings and standardisation impact (values actually changed are reported, not hidden)
for c in EMPTY_AS_NULL_COLS:
    check6(f"{c}: empty strings remaining", 0, silver_gdp_df.filter(F.trim(c) == "").count())
std_join = before_df.select(*row_id, *[F.col(c).alias(f"{c}__b") for c in
                                        ["indicator_id", "country_iso3_code", "country_id", "indicator_name", "country_name"]]
                            ).join(silver_gdp_df, row_id, "inner")
for c in ["indicator_id", "country_iso3_code", "country_id", "indicator_name", "country_name"]:
    check6(f"{c}: values changed by standardisation", 0,
           std_join.filter(~F.col(f"{c}__b").eqNullSafe(F.col(c))).count(), critical=False)

# 5. Keys, history and lineage
check6("record_key unique", n_silver, silver_gdp_df.select("record_key").distinct().count())
check6("business key + ingestion_date unique", n_silver, silver_gdp_df.select(*KEY_COLS).distinct().count())
check6("snapshots (ingestion_date) preserved",
       sorted(r[0] for r in before_df.select("ingestion_date").distinct().collect()),
       sorted(r[0] for r in after_all_df.select("ingestion_date").distinct().collect()))
check6("source files preserved",
       sorted(r[0] for r in before_df.select("source_file_path").distinct().collect()),
       sorted(r[0] for r in after_all_df.select("source_file_path").distinct().collect()))
non_nullable = [f.name for f in SILVER_GDP_SCHEMA.fields if not f.nullable]
nn = silver_gdp_df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in non_nullable]).first().asDict()
check6("NULLs in non-nullable contract columns", 0, sum(nn.values()))
check6("NULL source_last_updated_date", 0, silver_gdp_df.filter(F.col("source_last_updated_date").isNull()).count(),
       critical=False)

# 6. Nothing written to Silver during this run (Silver tables exist since Step 8)
silver_delta_tables = [r.table_name for r in spark.sql(f"""
    SELECT table_name FROM `{cfg.catalog}`.information_schema.tables
    WHERE table_schema = '{cfg.silver_schema}' AND table_type <> 'VIEW'""").collect()]
check6("Silver tables written during this run", 0, sum(
    spark.sql(f"DESCRIBE HISTORY {cfg.schema_fqn}.`{t}`")
         .filter(F.col("timestamp") >= F.lit(cfg.run_ts_utc).cast("timestamp")).count() > 0
    for t in silver_delta_tables))

checks6_df = spark.createDataFrame(checks6, "check STRING, expected STRING, actual STRING, status STRING")
display(checks6_df)

failed6 = [c for c in checks6 if c[3] == "FAIL"]
if failed6:
    logger.error(f"Step 6 validation failed: {failed6}")
    raise PipelineError(f"Step 6 validation failed: {[c[0] for c in failed6]}")
logger.info(f"Step 6 validation passed: {len(checks6)} checks, {sum(c[3] == 'WARN' for c in checks6)} warning(s)")


# DBTITLE 1,8a - Silver table specifications
# (name, type, nullable, comment) - single source of truth for DDL and post-creation validation
OBSERVATION_COLUMNS = [
    ("record_key", "string", False, "SHA-256 of indicator_id, country_iso3_code, observation_period, ingestion_date"),
    ("indicator_id", "string", False, "World Bank indicator code, trimmed and upper-cased"),
    ("indicator_name", "string", True, "World Bank indicator name as received (trimmed)"),
    ("country_iso3_code", "string", False, "Country / aggregate 3-letter code, trimmed and upper-cased"),
    ("country_id", "string", True, "World Bank 2-character country code (ISO2 for countries)"),
    ("country_name", "string", True, "Country name as received (trimmed)"),
    ("year", "int", True, "Year, derived only from 4-digit periods; NULL otherwise"),
    ("observation_period", "string", False, "Original World Bank period text (YYYY, YYYYQn, YYYYMmm)"),
    ("observation_value", "decimal(38,10)", True, "Exact observation value; NULL when not published by the World Bank"),
    ("unit", "string", True, "Unit as received; empty string stored as NULL"),
    ("obs_status", "string", True, "Observation status as received; empty string stored as NULL"),
    ("decimal_places", "int", True, "World Bank decimal field"),
    ("source_last_updated_date", "date", True, "World Bank lastupdated date of the snapshot"),
    ("dq_flags", "array<string>", True, "Data quality flags (records are flagged, never altered)"),
    ("drift_extra_keys", "array<string>", True, "Unexpected source keys found in the record"),
    ("drift_missing_keys", "array<string>", True, "Expected source keys missing from the record"),
    ("record_hash", "string", False, "SHA-256 of business attributes, used to detect snapshot conflicts"),
    ("source_file_path", "string", False, "Bronze file the record was read from"),
    ("ingestion_date", "date", False, "ADF load date taken from the Bronze file name"),
    ("ingestion_timestamp", "timestamp", False, "Time ADF landed the Bronze file"),
    ("pipeline_run_id", "string", False, "Silver pipeline run that inserted the record"),
    ("processed_timestamp", "timestamp", False, "Silver pipeline run timestamp (UTC)"),
]

METADATA_COLUMNS = [
    ("metadata_key", "string", False, "SHA-256 of source_file_path and page"),
    ("source_file_path", "string", False, "Bronze file the metadata was read from"),
    ("indicator_code", "string", True, "Indicator code from the Bronze path"),
    ("country_iso3", "string", True, "Country ISO3 code from the Bronze path"),
    ("ingestion_date", "date", False, "ADF load date taken from the Bronze file name"),
    ("ingestion_timestamp", "timestamp", False, "Time ADF landed the Bronze file"),
    ("page", "int", True, "World Bank API page"),
    ("total_pages", "int", True, "World Bank API pages"),
    ("per_page", "int", True, "World Bank API per_page"),
    ("total_records", "bigint", True, "World Bank API total"),
    ("source_id", "string", True, "World Bank API sourceid"),
    ("source_last_updated_date", "date", True, "World Bank API lastupdated"),
    ("valid_observations", "bigint", True, "Observations parsed successfully from the file"),
    ("observation_parse_errors", "bigint", True, "Observations quarantined at parse stage"),
    ("reconciliation_status", "string", True, "MATCHED, COUNT_MISMATCH, PAGES_INCOMPLETE, NO_METADATA, ..."),
    ("validation_status", "string", True, "Step 5 file status: PASS, WARN or FAIL"),
    ("critical_issues", "array<string>", True, "Step 5 critical issue codes"),
    ("warnings", "array<string>", True, "Step 5 warning codes"),
    ("drift_extra_keys", "array<string>", True, "Unexpected metadata keys"),
    ("drift_missing_keys", "array<string>", True, "Expected metadata keys missing"),
    ("raw_record", "string", True, "Original metadata line"),
    ("pipeline_run_id", "string", False, "Silver pipeline run that inserted the record"),
    ("processed_timestamp", "timestamp", False, "Silver pipeline run timestamp (UTC)"),
]

QUARANTINE_COLUMNS = [
    ("quarantine_key", "string", False, "SHA-256 of source_file_path, ingestion_date, quarantine_stage and raw_record"),
    ("quarantine_stage", "string", False, "FILE, PARSE, KEY_VALIDATION or SNAPSHOT_CONFLICT"),
    ("record_type", "string", True, "Line classification (OBSERVATION, PAGINATION_METADATA, MALFORMED, ...)"),
    ("error_types", "array<string>", True, "All reasons the record was quarantined"),
    ("indicator_id", "string", True, "Indicator code if available"),
    ("country_iso3_code", "string", True, "Country ISO3 code if available"),
    ("observation_period", "string", True, "Period if available"),
    ("raw_record", "string", True, "Original Bronze line, or JSON of the rejected row"),
    ("source_file_path", "string", False, "Bronze file the record came from"),
    ("ingestion_date", "date", True, "ADF load date (NULL if the file name had no valid date)"),
    ("ingestion_timestamp", "timestamp", True, "Time ADF landed the Bronze file"),
    ("pipeline_run_id", "string", False, "Silver pipeline run that quarantined the record"),
    ("processed_timestamp", "timestamp", False, "Silver pipeline run timestamp (UTC)"),
]

RUN_AUDIT_COLUMNS = [
    ("pipeline_run_id", "string", False, "Silver pipeline run id"),
    ("run_start_ts", "timestamp", False, "Run start (UTC)"),
    ("run_end_ts", "timestamp", True, "Run end (UTC)"),
    ("run_status", "string", False, "SUCCEEDED or FAILED"),
    ("files_discovered", "bigint", True, "Bronze files discovered"),
    ("files_valid", "bigint", True, "Bronze files accepted by the inventory"),
    ("files_excluded", "bigint", True, "Bronze files excluded by the inventory"),
    ("bronze_lines", "bigint", True, "Bronze lines read"),
    ("observations_inserted", "bigint", True, "New observation rows inserted"),
    ("observations_already_present", "bigint", True, "Rows skipped because the same snapshot was already loaded"),
    ("snapshot_conflicts", "bigint", True, "Rows whose key existed with different content (quarantined, run failed)"),
    ("metadata_inserted", "bigint", True, "New metadata rows inserted"),
    ("quarantine_inserted", "bigint", True, "New quarantine rows inserted"),
    ("warnings", "array<string>", True, "Warnings raised during the run"),
    ("error_message", "string", True, "Failure reason when run_status = FAILED"),
]

COMMON_TBLPROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.columnMapping.mode": "name",
    "delta.logRetentionDuration": "interval 90 days",
    "delta.deletedFileRetentionDuration": "interval 7 days",
}
COMMON_TAGS = {"layer": "silver", "source": "worldbank", "domain": "economics"}

TABLE_SPECS = {
    "observations": dict(columns=OBSERVATION_COLUMNS, pk="record_key", cluster_by=["indicator_id", "country_iso3_code"],
                         extra_props={"delta.appendOnly": "true"},
                         comment="Silver: trusted World Bank indicator observations. One row per indicator, country, period and ADF load (ingestion_date). Insert-only; history is never overwritten."),
    "load_metadata": dict(columns=METADATA_COLUMNS, pk="metadata_key", cluster_by=["indicator_code", "country_iso3"],
                          extra_props={},
                          comment="Silver: World Bank API pagination metadata per Bronze file and page, with reconciliation and validation results."),
    "quarantine": dict(columns=QUARANTINE_COLUMNS, pk="quarantine_key", cluster_by=["quarantine_stage", "ingestion_date"],
                       extra_props={},
                       comment="Silver: records rejected by the pipeline (file, parse, key validation, snapshot conflict) kept for investigation."),
    "run_audit": dict(columns=RUN_AUDIT_COLUMNS, pk="pipeline_run_id", cluster_by=[],
                      extra_props={},
                      comment="Silver: one row per Silver pipeline run with counts and outcome."),
}
GDP_VIEW = cfg.table("v_gdp_observations")


def _q(s: str) -> str:
    return s.replace("'", "\\'")


def build_create_ddl(key: str) -> str:
    spec = TABLE_SPECS[key]
    fqn = TABLES[key]
    cols = ",\n  ".join(f"`{n}` {t}{'' if nullable else ' NOT NULL'} COMMENT '{_q(c)}'"
                          for n, t, nullable, c in spec["columns"])
    table_short = fqn.split(".")[-1].strip("`")
    pk = f",\n  CONSTRAINT `{table_short}_pk` PRIMARY KEY (`{spec['pk']}`)"
    cluster = f"\nCLUSTER BY ({', '.join(f'`{c}`' for c in spec['cluster_by'])})" if spec["cluster_by"] else ""
    props = {**COMMON_TBLPROPERTIES, **spec["extra_props"]}
    props_sql = ",\n  ".join(f"'{k}' = '{v}'" for k, v in props.items())
    return (f"CREATE TABLE IF NOT EXISTS {fqn} (\n  {cols}{pk}\n)\nUSING DELTA{cluster}\n"
            f"COMMENT '{_q(spec['comment'])}'\nTBLPROPERTIES (\n  {props_sql}\n)")


GDP_VIEW_DDL = f"""CREATE VIEW IF NOT EXISTS {GDP_VIEW}
COMMENT 'Silver: GDP (current US$) observations, all ingestion snapshots. Filter of wb_indicator_observations.'
AS SELECT record_key, indicator_id, indicator_name, country_iso3_code, country_id, country_name,
          year, observation_period, observation_value AS gdp_value, unit, obs_status, decimal_places,
          source_last_updated_date, dq_flags, record_hash, source_file_path, ingestion_date,
          ingestion_timestamp, pipeline_run_id, processed_timestamp
   FROM {TABLES['observations']}
   WHERE indicator_id = '{GDP_INDICATOR_ID}'"""

for k in TABLE_SPECS:
    print(f"----- {TABLES[k]} -----\n{build_create_ddl(k)}\n")
print(f"----- {GDP_VIEW} -----\n{GDP_VIEW_DDL}")

# COMMAND ----------

# DBTITLE 1,8b - Create tables and view (idempotent)
def table_exists(fqn: str) -> bool:
    return spark.catalog.tableExists(fqn.replace("`", ""))


creation_log = []
for key in TABLE_SPECS:
    fqn = TABLES[key]
    if table_exists(fqn):
        logger.info(f"{fqn} already exists - not replaced or altered; will be validated against the spec")
        creation_log.append((fqn, "EXISTING"))
        continue
    try:
        spark.sql(build_create_ddl(key))
        logger.info(f"Created {fqn}")
        creation_log.append((fqn, "CREATED"))
    except Exception as e:
        logger.error(f"Failed to create {fqn}: {e}")
        raise PipelineError(f"Table creation failed for {fqn}") from e

    # Governance tags: non-critical (a tag policy must not block table creation)
    try:
        tags_sql = ", ".join(f"'{k}' = '{v}'" for k, v in COMMON_TAGS.items())
        spark.sql(f"ALTER TABLE {fqn} SET TAGS ({tags_sql})")
    except Exception as e:
        logger.warning(f"Could not set tags on {fqn}: {e}")

if table_exists(GDP_VIEW):
    creation_log.append((GDP_VIEW, "EXISTING"))
else:
    spark.sql(GDP_VIEW_DDL)
    creation_log.append((GDP_VIEW, "CREATED"))
    logger.info(f"Created view {GDP_VIEW}")

display(spark.createDataFrame(creation_log, "object STRING, action STRING"))

# COMMAND ----------

# DBTITLE 1,8c - Validate tables against the spec
checks8 = []

def check8(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks8.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

schema_location = {r.database_description_item: r.database_description_value for r in
                   spark.sql(f"DESCRIBE SCHEMA EXTENDED {cfg.schema_fqn}").collect()}["Location"].rstrip("/")
details = []

for key, spec in TABLE_SPECS.items():
    fqn = TABLES[key]
    short = fqn.split(".")[-1].strip("`")
    d = spark.sql(f"DESCRIBE DETAIL {fqn}").first().asDict()
    details.append((short, d["format"], d["location"], d["clusteringColumns"], d["numFiles"], d["sizeInBytes"]))

    # Storage: Delta, managed, inside the Silver schema's managed location
    check8(f"{short}: format", "delta", d["format"])
    check8(f"{short}: under Silver managed location", True, d["location"].startswith(schema_location + "/"))
    check8(f"{short}: managed table", "MANAGED",
           spark.sql(f"DESCRIBE TABLE EXTENDED {fqn}").filter("col_name = 'Type'").first()["data_type"])

    # Layout and properties
    check8(f"{short}: clustering columns", spec["cluster_by"], list(d["clusteringColumns"]))
    expected_props = {**COMMON_TBLPROPERTIES, **spec["extra_props"]}
    actual_props = {k: v for k, v in d["properties"].items() if k in expected_props}
    check8(f"{short}: table properties", expected_props, actual_props)
    check8(f"{short}: partition columns", [], list(d["partitionColumns"]))

    # Schema: names, types, nullability, order
    actual_cols = [(f.name, f.dataType.simpleString(), f.nullable) for f in spark.table(fqn.replace("`", "")).schema.fields]
    check8(f"{short}: schema matches spec", [(n, t, nl) for n, t, nl, _ in spec["columns"]], actual_cols)

    # Empty when created in this run (existing tables legitimately hold data from earlier loads)
    if (fqn, "CREATED") in creation_log:
        check8(f"{short}: row count (new table)", 0, spark.table(fqn.replace("`", "")).count())

# Primary keys (informational constraints)
pk_rows = spark.sql(f"""
    SELECT tc.table_name, kcu.column_name
    FROM `{cfg.catalog}`.information_schema.table_constraints tc
    JOIN `{cfg.catalog}`.information_schema.key_column_usage kcu
      ON tc.constraint_catalog = kcu.constraint_catalog AND tc.constraint_schema = kcu.constraint_schema
     AND tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema = '{cfg.silver_schema}' AND tc.constraint_type = 'PRIMARY KEY'""").collect()
check8("primary keys",
       sorted((TABLES[k].split(".")[-1].strip("`"), s["pk"]) for k, s in TABLE_SPECS.items()),
       sorted((r.table_name, r.column_name) for r in pk_rows))

# Tags (warning only)
tag_rows = spark.sql(f"""SELECT table_name, tag_name, tag_value FROM `{cfg.catalog}`.information_schema.table_tags
                         WHERE schema_name = '{cfg.silver_schema}'""").collect()
for key in TABLE_SPECS:
    short = TABLES[key].split(".")[-1].strip("`")
    check8(f"{short}: tags", COMMON_TAGS,
           {r.tag_name: r.tag_value for r in tag_rows if r.table_name == short}, critical=False)

# GDP view
view_cols = spark.table(GDP_VIEW.replace("`", "")).columns
check8("v_gdp_observations: exposes gdp_value", True, "gdp_value" in view_cols)
check8("v_gdp_observations: gdp_value type", "decimal(38,10)",
       spark.table(GDP_VIEW.replace("`", "")).schema["gdp_value"].dataType.simpleString())
if (GDP_VIEW, "CREATED") in creation_log:
    check8("v_gdp_observations: row count (new view)", 0, spark.table(GDP_VIEW.replace("`", "")).count())

# Bronze untouched
current_mod = {f.path: f.modificationTime for f in list_files_recursive(cfg.bronze_root)}
check8("Bronze files modified", 0,
       sum(current_mod.get(r["file_path"]) != int(r["modified_ts"].timestamp() * 1000) for r in file_records))

display(spark.createDataFrame(details, "table STRING, format STRING, location STRING, clustering ARRAY<STRING>, num_files BIGINT, size_bytes BIGINT"))
checks8_df = spark.createDataFrame(checks8, "check STRING, expected STRING, actual STRING, status STRING")
display(checks8_df)

failed8 = [c for c in checks8 if c[3] == "FAIL"]
if failed8:
    logger.error(f"Step 8 validation failed: {failed8}")
    raise PipelineError(f"Step 8 validation failed: {[c[0] for c in failed8]}")
logger.info(f"Step 8 validation passed: {len(checks8)} checks, {sum(c[3] == 'WARN' for c in checks8)} warning(s)")



# DBTITLE 1,9a - Build source DataFrames for the four tables
OBS_COLS = [c[0] for c in OBSERVATION_COLUMNS]
META_COLS = [c[0] for c in METADATA_COLUMNS]
Q_COLS = [c[0] for c in QUARANTINE_COLUMNS]
RUN_COLS_DDL = ", ".join(f"`{n}` {t}" for n, t, _, _ in RUN_AUDIT_COLUMNS)
RUN_SCOPED = ["pipeline_run_id", "processed_timestamp"]     # excluded from keys / raw JSON so reruns dedupe

# Scope guard: generic table, but Step 6 currently standardises GDP only
n_non_gdp = non_gdp_observations_df.count()
if n_non_gdp:
    raise PipelineError(f"{n_non_gdp} non-GDP observations found - extend Step 6 to all indicators before loading")

# ---------------- Observations ----------------
obs_src_df = silver_gdp_df.withColumnRenamed("gdp_value", "observation_value").select(*OBS_COLS)

# ---------------- Load metadata (+ Step 5 file validation result) ----------------
meta_src_df = (load_metadata_df
    .join(reconciliation_report_df.select("source_file_path", "validation_status", "critical_issues", "warnings"),
          "source_file_path", "left")
    .withColumnRenamed("path_indicator_code", "indicator_code")
    .withColumnRenamed("path_country_iso3", "country_iso3")
    .withColumn("metadata_key", F.sha2(F.concat_ws("||", F.col("source_file_path"), F.col("page").cast("string")), 256))
    .select(*META_COLS))

# ---------------- Quarantine sources (FILE, PARSE, KEY_VALIDATION) ----------------
RUN_LIT = [F.lit(cfg.run_id).alias("pipeline_run_id"), F.lit(cfg.run_ts_utc).cast("timestamp").alias("processed_timestamp")]
NULL_STR = F.lit(None).cast("string")

q_file_df = excluded_files_df.select(
    F.lit("FILE").alias("quarantine_stage"), F.lit("FILE").alias("record_type"),
    F.array(F.col("status")).alias("error_types"),
    F.col("indicator_code").alias("indicator_id"), F.col("country_iso3").alias("country_iso3_code"),
    NULL_STR.alias("observation_period"),
    F.to_json(F.struct(*inventory_df.columns)).alias("raw_record"),
    F.col("file_path").alias("source_file_path"), F.col("load_date").alias("ingestion_date"),
    F.col("modified_ts").alias("ingestion_timestamp"), *RUN_LIT)

q_parse_df = parse_errors_df.select(
    F.lit("PARSE").alias("quarantine_stage"), "record_type", "error_types",
    F.get_json_object("raw_record", "$.indicator.id").alias("indicator_id"),
    F.get_json_object("raw_record", "$.countryiso3code").alias("country_iso3_code"),
    F.get_json_object("raw_record", "$.date").alias("observation_period"),
    "raw_record", "source_file_path", "ingestion_date", "ingestion_timestamp", *RUN_LIT)

REJECT_JSON_COLS = [f.name for f in SILVER_GDP_SCHEMA.fields if f.name not in RUN_SCOPED]
q_key_df = silver_gdp_rejects_df.select(
    F.lit("KEY_VALIDATION").alias("quarantine_stage"), F.lit("OBSERVATION").alias("record_type"),
    F.col("reject_reasons").alias("error_types"),
    "indicator_id", "country_iso3_code", "observation_period",
    F.to_json(F.struct(*REJECT_JSON_COLS)).alias("raw_record"),
    "source_file_path", "ingestion_date", "ingestion_timestamp", *RUN_LIT)

quarantine_key_expr = F.sha2(F.concat_ws("||",
    F.col("source_file_path"), F.coalesce(F.col("ingestion_date").cast("string"), F.lit("")),
    F.col("quarantine_stage"), F.coalesce(F.col("raw_record"), F.lit(""))), 256)

quarantine_src_df = (q_file_df.unionByName(q_parse_df).unionByName(q_key_df)
                     .withColumn("quarantine_key", quarantine_key_expr).select(*Q_COLS))

# Source counts, logged for the record
src_counts = {"observations": obs_src_df.count(), "load_metadata": meta_src_df.count(), "quarantine": quarantine_src_df.count()}
logger.info(f"Load sources prepared: {src_counts}")
for name, df, cols in [("observations", obs_src_df, OBS_COLS), ("load_metadata", meta_src_df, META_COLS),
                       ("quarantine", quarantine_src_df, Q_COLS)]:
    if df.columns != cols:
        raise PipelineError(f"{name} source columns do not match the table spec: {df.columns}")

# COMMAND ----------

# DBTITLE 1,9b - Insert-only load function
def merge_insert_only(src_df, target_fqn: str, key_col: str) -> int:
    """Insert rows whose key is not in the target; never update or delete. Returns rows inserted."""
    view = f"_src_{target_fqn.split('.')[-1].strip('`')}"
    src_df.createOrReplaceTempView(view)
    try:
        res = spark.sql(f"MERGE INTO {target_fqn} AS t USING {view} AS s "
                        f"ON t.`{key_col}` = s.`{key_col}` WHEN NOT MATCHED THEN INSERT *").first()
        return int(res["num_inserted_rows"])
    except Exception as e:
        if "append" not in str(e).lower():
            raise
        # Append-only table refused MERGE: append only the new keys (identical semantics)
        logger.warning(f"MERGE refused on {target_fqn} ({str(e)[:200]}); falling back to anti-join append")
        new_rows = src_df.join(spark.table(target_fqn.replace("`", "")).select(key_col), key_col, "left_anti")
        n_new = new_rows.count()
        if n_new:
            new_rows.select(*src_df.columns).write.mode("append").saveAsTable(target_fqn.replace("`", ""))
        return n_new


def with_run(df, run_id: str, run_ts):
    return (df.withColumn("pipeline_run_id", F.lit(run_id))
              .withColumn("processed_timestamp", F.lit(run_ts).cast("timestamp")))


def load_silver(run_id: str, run_ts) -> dict:
    """One idempotent Silver load. Always writes a run-audit row; re-raises on failure."""
    stats = dict(observations_inserted=None, observations_already_present=None, snapshot_conflicts=None,
                 metadata_inserted=None, quarantine_inserted=None)
    status, error, failure = "SUCCEEDED", None, None
    try:
        # Gate: Step 5 file validation
        failed_files = reconciliation_report_df.filter(F.col("validation_status") == "FAIL").count()
        if failed_files:
            raise PipelineError(f"{failed_files} file(s) failed Step 5 reconciliation - load blocked")

        obs = with_run(obs_src_df, run_id, run_ts)
        n_obs = obs.count()
        if n_obs != obs.select("record_key").distinct().count():
            raise PipelineError("Duplicate record_key in the load batch")

        # Snapshot conflicts: key already stored with different business content
        target = spark.table(TABLES["observations"].replace("`", "")).select("record_key", F.col("record_hash").alias("stored_hash"))
        conflicts = obs.join(target, "record_key", "inner").filter(F.col("record_hash") != F.col("stored_hash"))
        n_conf = conflicts.count()
        stats["snapshot_conflicts"] = n_conf
        q_conflicts = conflicts.select(
            F.lit("SNAPSHOT_CONFLICT").alias("quarantine_stage"), F.lit("OBSERVATION").alias("record_type"),
            F.array(F.lit("RECORD_HASH_MISMATCH")).alias("error_types"),
            "indicator_id", "country_iso3_code", "observation_period",
            F.to_json(F.struct(*[c for c in OBS_COLS if c not in RUN_SCOPED])).alias("raw_record"),
            "source_file_path", "ingestion_date", "ingestion_timestamp", "pipeline_run_id", "processed_timestamp"
        ).withColumn("quarantine_key", quarantine_key_expr).select(*Q_COLS)

        # 1. Quarantine  2. Metadata  3. Observations (blocked by conflicts)
        stats["quarantine_inserted"] = merge_insert_only(
            with_run(quarantine_src_df, run_id, run_ts).unionByName(q_conflicts), TABLES["quarantine"], "quarantine_key")
        stats["metadata_inserted"] = merge_insert_only(
            with_run(meta_src_df, run_id, run_ts), TABLES["load_metadata"], "metadata_key")
        if n_conf:
            raise PipelineError(f"{n_conf} snapshot conflict(s) quarantined - stored history left unchanged")
        ins = merge_insert_only(obs, TABLES["observations"], "record_key")
        stats["observations_inserted"] = ins
        stats["observations_already_present"] = n_obs - ins
        logger.info(f"Silver load {run_id} succeeded: {stats}")
    except Exception as e:
        status, error, failure = "FAILED", str(e)[:4000], e
        logger.error(f"Silver load {run_id} failed: {error}")
    finally:
        warns = sorted({w for r in reconciliation_report_df.select("warnings").collect() for w in (r.warnings or [])})
        audit_row = [(run_id, run_ts, datetime.now(timezone.utc), status,
                      len(file_records), len(valid_paths), excluded_files_df.count(), lines_df.count(),
                      stats["observations_inserted"], stats["observations_already_present"], stats["snapshot_conflicts"],
                      stats["metadata_inserted"], stats["quarantine_inserted"], warns, error)]
        merge_insert_only(spark.createDataFrame(audit_row, RUN_COLS_DDL), TABLES["run_audit"], "pipeline_run_id")
    if failure:
        raise PipelineError(f"Silver load failed: {error}") from failure
    return {"pipeline_run_id": run_id, "run_status": status, **stats}

logger.info("load_silver() defined.")

# COMMAND ----------

# DBTITLE 1,9c - Run the Silver load
pre_counts = {k: spark.table(TABLES[k].replace("`", "")).count() for k in TABLE_SPECS}
load_result = load_silver(cfg.run_id, cfg.run_ts_utc)
post_counts = {k: spark.table(TABLES[k].replace("`", "")).count() for k in TABLE_SPECS}

display(spark.createDataFrame([(k, pre_counts[k], post_counts[k]) for k in TABLE_SPECS],
                              "table STRING, rows_before BIGINT, rows_after BIGINT"))
display(spark.createDataFrame([load_result]))

# COMMAND ----------

# DBTITLE 1,9d - Idempotency test (re-run the same snapshot)
# Off by default so scheduled runs don't add test audit rows; set the widget to "true" to run the test.
dbutils.widgets.dropdown("run_idempotency_test", "false", ["false", "true"])

if dbutils.widgets.get("run_idempotency_test") == "true":
    test_run_id = str(uuid.uuid4())
    before = {k: spark.table(TABLES[k].replace("`", "")).count() for k in TABLE_SPECS}
    rerun = load_silver(test_run_id, datetime.now(timezone.utc))
    after = {k: spark.table(TABLES[k].replace("`", "")).count() for k in TABLE_SPECS}

    idem = [
        ("observations inserted on rerun", "0", str(rerun["observations_inserted"])),
        ("observations already present", str(obs_src_df.count()), str(rerun["observations_already_present"])),
        ("snapshot conflicts", "0", str(rerun["snapshot_conflicts"])),
        ("metadata inserted on rerun", "0", str(rerun["metadata_inserted"])),
        ("quarantine inserted on rerun", "0", str(rerun["quarantine_inserted"])),
        ("observations rows unchanged", str(before["observations"]), str(after["observations"])),
        ("metadata rows unchanged", str(before["load_metadata"]), str(after["load_metadata"])),
        ("quarantine rows unchanged", str(before["quarantine"]), str(after["quarantine"])),
        ("one audit row added", str(before["run_audit"] + 1), str(after["run_audit"])),
    ]
    idem_df = spark.createDataFrame([(n, e, a, "PASS" if e == a else "FAIL") for n, e, a in idem],
                                    "check STRING, expected STRING, actual STRING, status STRING")
    display(idem_df)
    if idem_df.filter("status = 'FAIL'").count():
        raise PipelineError("Idempotency test failed")
    logger.info(f"Idempotency test passed (test run {test_run_id})")
else:
    logger.info("Idempotency test skipped (run_idempotency_test = false)")

# COMMAND ----------

# DBTITLE 1,9e - Post-load validation
checks9 = []

def check9(name: str, expected, actual, critical: bool = True):
    ok = expected == actual
    checks9.append((name, str(expected), str(actual), "PASS" if ok else ("FAIL" if critical else "WARN")))

obs_t = spark.table(TABLES["observations"].replace("`", ""))
meta_t = spark.table(TABLES["load_metadata"].replace("`", ""))
q_t = spark.table(TABLES["quarantine"].replace("`", ""))
audit_t = spark.table(TABLES["run_audit"].replace("`", ""))
gdp_v = spark.table(GDP_VIEW.replace("`", ""))

# 1. Every current-batch record is stored exactly once
batch_keys = obs_src_df.select("record_key")
check9("batch records missing from table", 0, batch_keys.join(obs_t, "record_key", "left_anti").count())
check9("record_key unique in table", obs_t.count(), obs_t.select("record_key").distinct().count())
check9("business key + ingestion_date unique", obs_t.count(), obs_t.select(*KEY_COLS).distinct().count())

# 2. Stored business content == batch content (lineage run columns excluded)
biz = [c for c in OBS_COLS if c not in RUN_SCOPED]
stored_batch = obs_t.join(batch_keys, "record_key", "inner").select(*biz)
check9("batch rows differing from stored rows", 0, obs_src_df.select(*biz).exceptAll(stored_batch).count())

# 3. Precision vs raw Bronze digits (independent path)
raw_cmp9 = raw_text_df.join(obs_t.select("source_file_path", "ingestion_date", "observation_period", "observation_value"),
                            ["source_file_path", "ingestion_date", "observation_period"], "inner")
check9("rows compared with raw Bronze text", raw_text_df.count(), raw_cmp9.count())
check9("stored value differs from raw Bronze digits", 0, raw_cmp9.filter(
    ~strip_trailing_zeros(F.col("raw_value_text")).eqNullSafe(strip_trailing_zeros(F.col("observation_value").cast("string")))).count())
check9("sum(observation_value) for batch", obs_src_df.agg(F.sum("observation_value")).first()[0],
       obs_t.join(batch_keys, "record_key").agg(F.sum("observation_value")).first()[0])

# 4. Metadata, quarantine, audit, view
check9("metadata rows for batch", meta_src_df.count(),
       meta_t.join(meta_src_df.select("metadata_key"), "metadata_key").count())
check9("quarantine rows for batch", quarantine_src_df.count(),
       q_t.join(quarantine_src_df.select("quarantine_key"), "quarantine_key").count())
this_run = audit_t.filter(F.col("pipeline_run_id") == cfg.run_id).collect()
check9("audit row for this run", 1, len(this_run))
check9("audit run_status", "SUCCEEDED", this_run[0].run_status if this_run else None)
check9("failed runs in audit", 0, audit_t.filter(F.col("run_status") == "FAILED").count(), critical=False)
check9("v_gdp_observations rows == GDP rows in table",
       obs_t.filter(F.col("indicator_id") == GDP_INDICATOR_ID).count(), gdp_v.count())

# 5. Table history: only CREATE and inserts (no UPDATE / DELETE on history)a
ops = {r.operation for r in spark.sql(f"DESCRIBE HISTORY {TABLES['observations']}").collect()}
check9("observations history has no UPDATE/DELETE", set(), ops & {"UPDATE", "DELETE", "TRUNCATE", "CREATE OR REPLACE TABLE"})

# 6. Bronze untouched
current_mod9 = {f.path: f.modificationTime for f in list_files_recursive(cfg.bronze_root)}
check9("Bronze files modified", 0,
       sum(current_mod9.get(r["file_path"]) != int(r["modified_ts"].timestamp() * 1000) for r in file_records))

display(audit_t.orderBy("run_start_ts"))
display(spark.sql(f"DESCRIBE HISTORY {TABLES['observations']}").select("version", "timestamp", "operation", "operationMetrics"))
checks9_df = spark.createDataFrame(checks9, "check STRING, expected STRING, actual STRING, status STRING")
display(checks9_df)

failed9 = [c for c in checks9 if c[3] == "FAIL"]
if failed9:
    logger.error(f"Step 9 validation failed: {failed9}")
    raise PipelineError(f"Step 9 validation failed: {[c[0] for c in failed9]}")
logger.info(f"Step 9 validation passed: {len(checks9)} checks, {sum(c[3] == 'WARN' for c in checks9)} warning(s)")
