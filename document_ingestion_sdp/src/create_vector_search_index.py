# Databricks notebook source
# MAGIC %md
# MAGIC # Create / Sync Vector Search Index
# MAGIC
# MAGIC Runs as a standalone job task AFTER the gold PII masking step.
# MAGIC Logic preserved from 05_create_vector_search_index.py.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet
# MAGIC %restart_python

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient

# ── Configuration ───────────────────────────────────────────────────────────
CATALOG = "mason_demo_catalog"
SCHEMA = "amex_enc_demo"
VS_ENDPOINT_NAME = "amex-enc-vs-endpoint"
VS_PRIMARY_KEY = "chunk_id"
VS_EMBEDDING_SOURCE_COLUMN = "page_content_masked"
VS_EMBEDDING_MODEL_ENDPOINT = "databricks-gte-large-en"
VS_PIPELINE_TYPE = "TRIGGERED"

# Per-category split: one Delta Sync index per docs_gold_<cat> on the shared
# endpoint. Keep in sync with CATEGORIES in config.py (duplicated inline).
CATEGORIES = ["hr", "finance", "research", "engineering", "support"]

print(f"Endpoint: {VS_ENDPOINT_NAME}")
print(f"Categories: {CATEGORIES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def endpoint_exists(client, endpoint_name):
    """Check if a Vector Search endpoint exists."""
    try:
        endpoints = client.list_endpoints()
        endpoint_names = [ep.get("name") for ep in endpoints.get("endpoints", [])]
        return endpoint_name in endpoint_names
    except Exception as e:
        print(f"Error checking endpoints: {e}")
        return False


def wait_for_endpoint_ready(client, endpoint_name, timeout=3600):
    """Wait for endpoint to be ready."""
    print(f"Waiting for endpoint '{endpoint_name}' to be ready...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            endpoint = client.get_endpoint(endpoint_name)
            status = endpoint.get("endpoint_status", {}).get("state", "UNKNOWN")
            if status == "ONLINE":
                print(f"Endpoint '{endpoint_name}' is ready.")
                return True
            print(f"  Endpoint status: {status}")
        except Exception as e:
            print(f"  Error checking endpoint: {e}")
        time.sleep(30)
    raise TimeoutError(
        f"Endpoint '{endpoint_name}' did not become ready within {timeout} seconds."
    )


def index_exists(client, endpoint_name, index_name):
    """Check if a Vector Search index exists."""
    try:
        index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
        return index is not None
    except Exception as e:
        if "NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
            return False
        print(f"Error checking index: {e}")
        return False


def wait_for_index_ready(client, endpoint_name, index_name, timeout=1800):
    """Wait for index to be ready (up to 30 minutes by default)."""
    print(f"Waiting for index '{index_name}' to be ready...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            index = client.get_index(
                endpoint_name=endpoint_name, index_name=index_name
            )
            status = index.describe().get("status", {}).get("ready", False)
            detailed_state = (
                index.describe().get("status", {}).get("detailed_state", "UNKNOWN")
            )
            if status:
                print(f"Index '{index_name}' is ready!")
                return True
            print(f"  Index status: ready={status}, state={detailed_state}")
        except Exception as e:
            print(f"  Error checking index: {e}")
        time.sleep(60)

    print(
        f"Warning: Index did not become ready within {timeout} seconds. "
        "It may still be processing."
    )
    return False

def gold_table_ready(table_name):
    """True if the gold table exists and has at least one row."""
    try:
        return spark.table(table_name).count() > 0
    except Exception as e:
        print(f"  {table_name} not available: {e}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create / Verify Endpoint (shared across all category indexes)

# COMMAND ----------

client = VectorSearchClient()
print("Vector Search client initialized.")

if endpoint_exists(client, VS_ENDPOINT_NAME):
    print(f"Endpoint '{VS_ENDPOINT_NAME}' already exists.")
else:
    print(f"Creating endpoint '{VS_ENDPOINT_NAME}'...")
    client.create_endpoint(name=VS_ENDPOINT_NAME, endpoint_type="STANDARD")
    print(f"Endpoint '{VS_ENDPOINT_NAME}' creation initiated.")

wait_for_endpoint_ready(client, VS_ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create / Sync One Index Per Category
# MAGIC
# MAGIC For each non-empty `docs_gold_<cat>`: enable CDF, then create or sync
# MAGIC `docs_gold_<cat>_index` on the shared endpoint. Empty/absent categories
# MAGIC are skipped (Delta Sync requires rows + CDF). Indexes provision
# MAGIC sequentially, so expect a few extra minutes per category.

# COMMAND ----------

created_indexes = []

for category in CATEGORIES:
    source_table_name = f"{CATALOG}.{SCHEMA}.docs_gold_{category}"
    full_index_name = f"{CATALOG}.{SCHEMA}.docs_gold_{category}_index"
    print("-" * 60)
    print(f"[{category}] table={source_table_name} index={full_index_name}")

    if not gold_table_ready(source_table_name):
        print(f"[skip] {category}: gold table missing or empty.")
        continue

    spark.sql(
        f"ALTER TABLE {source_table_name} "
        "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    print(f"  Change Data Feed enabled on {source_table_name}")

    if index_exists(client, VS_ENDPOINT_NAME, full_index_name):
        print(f"  Index exists. Triggering sync...")
        index = client.get_index(
            endpoint_name=VS_ENDPOINT_NAME, index_name=full_index_name
        )
        print(f"  Sync triggered. Result: {index.sync()}")
    else:
        print(f"  Creating new Delta Sync index...")
        index = client.create_delta_sync_index(
            endpoint_name=VS_ENDPOINT_NAME,
            source_table_name=source_table_name,
            index_name=full_index_name,
            pipeline_type=VS_PIPELINE_TYPE,
            primary_key=VS_PRIMARY_KEY,
            embedding_source_column=VS_EMBEDDING_SOURCE_COLUMN,
            embedding_model_endpoint_name=VS_EMBEDDING_MODEL_ENDPOINT,
        )
        print(f"  Index creation initiated.")

    wait_for_index_ready(client, VS_ENDPOINT_NAME, full_index_name)
    created_indexes.append(full_index_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("VECTOR SEARCH INDEX SUMMARY")
print("=" * 60)
print(f"Endpoint: {VS_ENDPOINT_NAME}")
print(f"Indexes created/synced: {len(created_indexes)}")
for full_index_name in created_indexes:
    try:
        index = client.get_index(
            endpoint_name=VS_ENDPOINT_NAME, index_name=full_index_name
        )
        status = index.describe().get("status", {})
        num_rows = status.get("num_rows", "N/A")
        print(f"  {full_index_name}: rows={num_rows}")
    except Exception as e:
        print(f"  {full_index_name}: error getting info ({e})")

print("\nVector Search index setup complete!")
