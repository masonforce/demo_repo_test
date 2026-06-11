# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: PII Masking → Delta Tables (docs_gold_<category>)
# MAGIC
# MAGIC Runs as a standalone job task AFTER the SDP pipeline refresh.
# MAGIC Reads from each `docs_silver_<category>` materialized view and writes one
# MAGIC regular Delta table per category (`docs_gold_<category>`) – not streaming
# MAGIC tables, not materialized views. Empty categories are skipped.

# COMMAND ----------

from pyspark.sql import functions as F

# ── Configuration ───────────────────────────────────────────────────────────
CATALOG = "mason_demo_catalog"
SCHEMA = "amex_enc_demo"
GOLD_TABLE = "docs_gold"
PII_MASKING_LLM = "databricks-claude-opus-4-6"
PII_MASKING_PROMPT = (
    "You are a text processing AI. Your task is to detect and mask all personally identifiable "
    "information (PII) in the text. \\n\\n"
    "Instructions:\\n\\n"
    "1. Only mask the PII types specified below. Other text and text format should remain unchanged.\\n"
    "2. Replace each detected PII with the mask format specified for that type.\\n"
    "3. Maintain the original text structure and punctuation.\\n\\n"
    "PII types to mask and their mask format:\\n"
    "- Names: [[NAME]]\\n"
    "- Emails: <<mask all except domain.com>>\\n"
    "- Phone numbers: [[mask all except last 4 digits.Preserve the format]]\\n"
    "- Social Security Numbers (SSN): <<SSN>>\\n"
    "- Credit Card Numbers: [all all digits.Preserve the format]\\n"
    "- Addresses: [ADDRESS]\\n\\n"
    "Output only the text with PII masked. Do not add any explanations.\\n\\n"
    "Text to process:\\n\\n"
)

key_b64 = "0EiSK5fLsVLlQZNl1+8obChaZYjADvyJutadxb2yb40="

# Per-category split: read docs_silver_<cat>, mask, write docs_gold_<cat>.
# Keep in sync with CATEGORIES in config.py (duplicated inline — imports are
# not reliable in this job context).
CATEGORIES = ["hr", "finance", "research", "engineering", "support"]

escaped_pii_prompt = PII_MASKING_PROMPT.replace("'", "''")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mask + Write One Gold Table Per Category
# MAGIC
# MAGIC For each category: read `docs_silver_<cat>`, decrypt + PII-mask, write
# MAGIC `docs_gold_<cat>` (overwrite). Categories with no rows are skipped so we
# MAGIC never create an empty gold table (Vector Search needs rows + CDF).

# COMMAND ----------

def _mask_and_write(category):
    source_table = f"{CATALOG}.{SCHEMA}.docs_silver_{category}"
    target_table = f"{CATALOG}.{SCHEMA}.docs_gold_{category}"

    try:
        base_df = spark.table(source_table)
    except Exception as e:
        print(f"[skip] {category}: source table {source_table} not found ({e}).")
        return None

    row_count = base_df.count()
    if row_count == 0:
        print(f"[skip] {category}: {source_table} has 0 rows — no gold table written.")
        return None

    print(f"[{category}] Source: {source_table} ({row_count} rows) -> Target: {target_table}")

    base_df = base_df.withColumn("chunk_id", F.expr("uuid()"))
    base_df = base_df.withColumn(
        "decrypted_page_content",
        F.expr(f"CAST(aes_decrypt(unbase64(encrypted_page_content), unbase64('{key_b64}')) AS STRING)"),
    )
    masked_df = base_df.withColumn(
        "page_content_masked",
        F.expr(
            f"ai_query('{PII_MASKING_LLM}', "
            f"concat('{escaped_pii_prompt}', decrypted_page_content))"
        ),
    )

    result_df = masked_df.select(
        "chunk_id",
        "file_name",
        "file_path",
        "document_type",
        "page_id",
        # "page_content",
        "page_content_masked",
        "element_count",
        "element_types",
        "image_uri",
    )

    result_df.write \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target_table)

    print(f"[{category}] Gold table written to {target_table}")
    return target_table


written = [t for t in (_mask_and_write(cat) for cat in CATEGORIES) if t]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print(f"Gold tables written: {len(written)}")
for target_table in written:
    gold_df = spark.table(target_table)
    print(
        f"  {target_table}: {gold_df.count()} rows, "
        f"{gold_df.select('file_name').distinct().count()} unique documents"
    )
