# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Format Elements and Aggregate by Page
# MAGIC
# MAGIC Formats element content with markdown-style prefixes and aggregates
# MAGIC all elements per page into a single `page_content` column.

# COMMAND ----------

import pyspark.pipelines as dp
from pyspark.sql import functions as F
key_b64 = "0EiSK5fLsVLlQZNl1+8obChaZYjADvyJutadxb2yb40="

# COMMAND ----------

# MAGIC %md
# MAGIC ## Page aggregation helper
# MAGIC
# MAGIC `_build_pages` holds the decrypt → format → filter → aggregate logic,
# MAGIC operating on a `docs_bronze_elements` DataFrame. The category split now
# MAGIC happens at the gold layer, so silver materializes a single pooled view.

# COMMAND ----------

def _build_pages(elements_df):
    # Decrypt encrypted_content → content
    elements_df = elements_df.withColumn(
        "decrypted_content",
        F.expr(f"CAST(aes_decrypt(unbase64(encrypted_content), unbase64('{key_b64}')) AS STRING)")
    )

    # Format content based on element type:
    #   title  → "# <content>"
    #   header → "## <content>"  (matches header, sectionheader, pageheader, etc.)
    #   table/figure → "[TYPE]\n<content>\n[Description: <desc>]"
    #   other  → content as-is
    formatted_df = elements_df.withColumn(
        "formatted_content",
        F.when(
            F.lower(F.trim(F.col("element_type"))) == "title",
            F.concat(F.lit("# "), F.coalesce(F.col("decrypted_content"), F.lit(""))),
        )
        .when(
            F.lower(F.trim(F.col("element_type"))).contains("header"),
            F.concat(F.lit("## "), F.coalesce(F.col("decrypted_content"), F.lit(""))),
        )
        .when(
            F.lower(F.trim(F.col("element_type"))).isin("table", "figure"),
            F.when(
                F.col("description").isNotNull()
                & (F.trim(F.col("description")) != ""),
                F.concat(
                    F.lit("["),
                    F.upper(F.trim(F.col("element_type"))),
                    F.lit("]\n"),
                    F.coalesce(F.col("decrypted_content"), F.lit("")),
                    F.lit("\n[Description: "),
                    F.col("description"),
                    F.lit("]"),
                ),
            ).otherwise(
                F.concat(
                    F.lit("["),
                    F.upper(F.trim(F.col("element_type"))),
                    F.lit("]\n"),
                    F.coalesce(F.col("decrypted_content"), F.lit("")),
                )
            ),
        )
        .otherwise(F.coalesce(F.col("decrypted_content"), F.lit(""))),
    )

    # Filter out empty content
    formatted_df = formatted_df.filter(
        F.col("formatted_content").isNotNull()
        & (F.trim(F.col("formatted_content")) != "")
    )

    # Aggregate elements by page, ordered by element_id
    aggregated_df = (
        formatted_df.groupBy("file_name", "file_path", "document_type", "page_id")
        .agg(
            F.concat_ws(
                "\n\n",
                F.array_sort(
                    F.collect_list(
                        F.struct(F.col("element_id"), F.col("formatted_content"))
                    )
                ).getField("formatted_content"),
            ).alias("page_content"),
            F.count("*").alias("element_count"),
            F.collect_set("element_type").alias("element_types"),
            F.first("image_uri").alias("image_uri"),
            F.max("extracted_at").alias("extracted_at"),
        )
        .withColumn("aggregated_at", F.current_timestamp())
        .withColumn(
            "page_content",
            F.expr(f"base64(aes_encrypt(page_content, unbase64('{key_b64}')))")
        )
        .withColumnRenamed("page_content", "encrypted_page_content")
    )

    # Convert element_types array to comma-separated string
    aggregated_df = aggregated_df.withColumn(
        "element_types", F.array_join(F.col("element_types"), ", ")
    )

    return aggregated_df.orderBy("file_name", "page_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View: docs_silver_pages
# MAGIC
# MAGIC Single pooled view across all document types. `document_type` is carried
# MAGIC through from bronze so the gold layer can split into docs_gold_<category>.

# COMMAND ----------

@dp.materialized_view(
    name="docs_silver_pages",
    comment="Silver layer: elements formatted and aggregated per page (all document types).",
)
def docs_silver_pages():
    return _build_pages(spark.table("docs_bronze_elements"))
