# demo_repo_test

Synthetic test-data generator for the **encrypted document-ingestion pipeline**
(AMEX Unstructured Data Platform demo).

## `generate_synthetic_pdfs.py`

A zero-dependency Databricks notebook that builds 12 realistic synthetic
tax/audit/compliance PDFs (raw PDF bytes — no libraries) and writes them to a
Unity Catalog volume. All content is synthetic (fabricated names, EINs, SSNs,
figures) and safe for the sandbox.

**Pre-set targets** (edit the constants at the top if yours differ):
```python
CATALOG = "unstructured_poc_v2"
SCHEMA  = "mason_demo_schema"
VOLUME  = "source_volume"        # writes to source_volume/raw_pdfs
```

**Use:**
1. Import into the workspace (Git folder, or Workspace → Import) — the
   `# Databricks notebook source` header lands it as a notebook.
2. Confirm the catalog/schema/volume above match `config.py`.
3. **Run all.** Writes 12 PDFs to `/Volumes/<CATALOG>/<SCHEMA>/source_volume/raw_pdfs/`
   and KA eval Q/A pairs to `source_volume/ka_eval/eval_questions.csv`.

> **UC Volumes note:** on serverless, direct `os.makedirs` / `open(...,"wb")` on a
> volume fails (errno 95) or yields empty files. This notebook writes to local
> `/tmp` first, then copies onto the volume with `dbutils.fs.cp`. Use the same
> pattern in the encryption notebook.

## `raw_pdfs/` — pre-generated PDFs (upload directly)

The 12 synthetic PDFs are also committed under `raw_pdfs/` so you can skip the
generator notebook entirely. In the workspace:

**Catalog Explorer → `unstructured_poc_v2` → `mason_demo_schema` → `source_volume`
→ Upload to this volume** → drop the 12 files into a `raw_pdfs/` folder.

This UI upload writes through the supported path (no serverless `dbutils.fs.cp`
dance needed). Use either this OR the generator notebook — not both.

**Run order in the pipeline:**
upload (or run `generate_synthetic_pdfs`) → encryption notebook
(`raw_pdfs` → `encrypted_pdfs`) → Lakeflow pipeline (bronze + silver)
→ gold → vector search.
