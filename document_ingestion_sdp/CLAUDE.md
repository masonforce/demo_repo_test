# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Databricks-native document ingestion and RAG pipeline that processes PDFs through a medallion architecture (Bronze → Silver → Gold) with PII masking and vector search indexing. Deployed as a Databricks Asset Bundle (DAB) to Azure Databricks.

## Deployment Commands

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to dev (default target)
databricks bundle deploy --target dev

# Deploy to prod
databricks bundle deploy --target prod

# Run the ingestion job
databricks bundle run document_ingestion_job
```

There are no local build, lint, or test commands. All code runs as Databricks notebooks on remote compute.

## Architecture

### Pipeline Flow

```
PDF files (UC Volume: /Volumes/bircatalog/pdf2/vol1/source/)
    │
    ▼
[DLT Streaming Pipeline - serverless, preview channel]
    │
    ├─ bronze_ingestion.py
    │   AutoLoader (cloudFiles) → ai_classify() → ai_parse_document v2.0
    │   Outputs: docs_bronze_parsed_docs_raw, docs_bronze_elements
    │
    ├─ silver_aggregated_pages.py
    │   Format elements as markdown → aggregate by page (all document types)
    │   Output: docs_silver_pages (single pooled materialized view)
    │
    ▼
[Standalone Job Tasks - sequential]
    │
    ├─ gold_summarized.py
    │   Read pooled silver → filter by document_type → PII masking (ai_query, Claude Opus 4.6)
    │   Output: docs_gold_<category> (one Delta table per category, overwrite; empties skipped)
    │
    └─ create_vector_search_index.py
        Delta Sync index with GTE Large embeddings on the finance gold table
        Output: docs_gold_finance_index (single example index for the KA pattern)
```

The category split happens at the **gold** layer. Categories are defined by
`CATEGORIES` in `src/config.py` (`hr`, `finance`, `research`, `engineering`,
`support`) and duplicated inline in `gold_summarized.py` (imports are not
reliable in the job context). Silver is a single pooled view; only one gold
table (`finance`) is vectorized, as an example of the retrieval/KA consumption
pattern — the other gold tables stay available for other consumption patterns.

### Job Orchestration

The `document_ingestion_job` runs three tasks in sequence:
1. `refresh_pipeline` — triggers the DLT streaming pipeline (bronze + pooled silver MV)
2. `gold_pii_masking` — reads pooled silver, filters by document_type, writes one gold Delta table per category
3. `vector_search` — enables CDF on `docs_gold_finance`, creates/syncs the `docs_gold_finance_index`

### Key Distinction

Bronze and silver tables are managed by DLT (`pyspark.pipelines` decorators: `@dp.table`, `@dp.materialized_view`). Gold and vector search run as regular notebook tasks outside DLT.

## Configuration

All pipeline constants are centralized in `src/config.py` (catalog, schema, volume paths, LLM endpoints, prompts, vector search settings). However, `gold_summarized.py` and `create_vector_search_index.py` currently duplicate these constants inline rather than importing from config.

## Additional Utilities

- **`src/mask_pdf.py`** — Standalone PDF-level PII masking. Extracts text with bounding boxes (pdfplumber), detects PII via `ai_query()`, and produces a redacted PDF with clipped overlays (reportlab). Requires: `pdfplumber`, `PyPDF2`, `reportlab`, `PyMuPDF`.

- **`evaluation_KA/evaluate_KA.py`** — MLflow GenAI evaluation notebook. Takes a CSV of Q&A pairs and a knowledge assistant endpoint, runs scorers (Correctness, RetrievalSufficiency, RetrievalGroundedness, RelevanceToQuery, Guidelines, conciseness). Parameterized via Databricks widgets: `eval_csv_volume_path` and `ka-endpoint`.

## Key Technical Details

- Unity Catalog location: `bircatalog.pdf2`
- LLM for PII masking: `databricks-claude-opus-4-6` via `ai_query()`
- Document classification: `ai_classify()` with labels: hr, finance, research, engineering, support
- Document parsing: `ai_parse_document` v2.0 with image extraction to volume
- Embeddings: `databricks-gte-large-en` via Delta Sync managed embeddings
- Vector Search endpoint: `ka-f3925e58-vs-endpoint`, pipeline type: TRIGGERED
- The silver layer preserves element ordering via `array_sort` on `element_id` before `concat_ws`
