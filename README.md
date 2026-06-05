# demo_repo_test

Synthetic test-data generator for the **encrypted document-ingestion pipeline**
(AMEX Unstructured Data Platform demo).

## `generate_synthetic_pdfs.py`

A zero-dependency Databricks notebook that builds 12 realistic synthetic
tax/audit/compliance PDFs (raw PDF bytes — no libraries) and writes them to a
Unity Catalog volume. All content is synthetic (fabricated names, EINs, SSNs,
figures) and safe for the sandbox.

**Use:**
1. Import into the workspace (Git folder, or Workspace → Import) — the
   `# Databricks notebook source` header lands it as a notebook.
2. Edit the two constants at the top to match `config.py`:
   ```python
   CATALOG = "<AMEX_CATALOG>"
   SCHEMA  = "<AMEX_SCHEMA>"
   ```
3. **Run all.** Writes 12 PDFs to `/Volumes/<CATALOG>/<SCHEMA>/vol1/raw_pdfs/`
   and KA eval Q/A pairs to `vol1/ka_eval/eval_questions.csv`.

**Run order in the pipeline:**
`generate_synthetic_pdfs` → encryption notebook (`raw_pdfs` → `encrypted_pdfs`)
→ Lakeflow pipeline (bronze + silver) → gold → vector search.
