# Retail Data Warehouse — Architecture Notes


## 1. Purpose

This document provides a short overview of the retail data warehouse architecture, the quality gates that control data movement and the agreed retention and reload policy.

---

## 2. What the Architecture Does

The warehouse uses a medallion-style flow to turn daily retail source files into trusted reporting data.

| Layer      | Description                                                                                                                                                                                                          |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Source** | Receives five daily CSV files: item, POS sales, store, inventory, and price.                                                                                                                                         |
| **Bronze** | Loads the source data into five staging tables (`STG_`), retaining the raw ingested records.                                                                                                                         |
| **Silver** | Checks and deduplicates the Bronze data. Valid records go to the corresponding `TMP_` tables with the invalid records going to the shared `REJ_TABLE`.                                                                          |
| **Gold**   | Loads the dimensional model: calendar, item, location, and customer dimensions, together with sales, price, inventory and weekly sales facts.                                                                       |

### Gold model

The Gold model uses **dimension tables** to describe business entities and **fact tables** to store business measures.

Main hierarchies:

| Dimension | Hierarchy                                  |
| --------- | ------------------------------------------ |
| Calendar  | Day → Week → Month → Quarter → Year        |
| Item      | Item → Class → Department → Company        |
| Location  | Location → District → State                |

---

## 3. Data Quality Gates

Data must pass the relevant checks before it moves to the next stage.

### 3.1 Source to Bronze

| Gate             | Pass rule                              | If it fails                        |
| ---------------- | -------------------------------------- | ---------------------------------- |
| File arrival     | All five expected files have arrived.  | Wait until timeout, then stop the process. |
| File row count   | Each file contains more than zero rows. | Fail the load for the empty file.  |

### 3.2 Bronze to Silver

Records are checked and separated into accepted and rejected sets.

| Check               | Rule / outcome                                          |
| ------------------- | ------------------------------------------------------- |
| Business key        | A business key must be present.                         |
| ID null check       | The `_ID` business key must not be `NULL`.              |
| Type casting        | Values must be convertible to the expected data types.  |
| POS sales amount    | Sales amount must not be negative.                      |
| POS sales quantity  | Sales quantity must not be negative.                    |
| Deduplication       | Deduplication tests are applied as part of Silver processing. |

**Outcome**

- Records that pass are written to the relevant Silver `TMP_` table.
- Records that fail are sent to `REJ_TABLE`.

### 3.3 Gold checks and reconciliations

| Check                                | Expected rule                                                        |
| ------------------------------------ | -------------------------------------------------------------------- |
| Surrogate keys                       | Surrogate keys must be unique.                                       |
| Mandatory foreign keys               | Mandatory foreign keys must not be `NULL` or orphaned.               |
| Bronze-to-Silver reconciliation      | Bronze row count = accepted Silver `TMP_` rows + rejected rows.      |
| Silver-to-Gold sales reconciliation  | Silver `TMP_` sales amount = Gold `FCT_SALES` sales amount.          |
| Current dimension version            | Each business key has exactly one current active dimension version.  |

Gold validation failures and reconciliation mismatches are recorded in the [Batch Log](#7-batch-log). Only successfully validated data should proceed to the next layer.

---

## 4. Retention Policy

| Data area    | Retention rule                                          |
| ------------ | ------------------------------------------------------- |
| Source files | Daily files are archived for recovery and reprocessing. |
| Bronze       | Retained for 30 days.                                   |
| Silver       | Retained for 30 days.                                   |
| Gold         | Historical business data is maintained.                 |

### Source archive rules

- Keep original source files; do not overwrite them.
- Store corrected files as separate versions in the archive.

### Bronze and Silver rules

- Bronze is append-oriented. Corrected files are ingested as new batches; original Bronze records are not overwritten.
- Silver is rebuilt for the affected scope when its corresponding Bronze batch is reprocessed.

---

## 5. Reload Policy

Reloading is primarily **file-level**: reprocess the affected source file and its dependent data instead of than rebuilding unrelated data.

### 5.1 Reload procedures

| Situation | Reload procedure |
| --------- | ---------------- |
| **Affected data is still within the 30-day Bronze/Silver retention window** | 1. Identify the affected file, batch, and business date.<br>2. Reprocess the Bronze batch.<br>3. Rebuild the corresponding Silver data.<br>4. Validate it.<br>5. Reprocess the affected Gold objects. |
| **Bronze/Silver data has passed the 30-day retention window** | 1. Retrieve the source file from the archive.<br>2. Ingest it into Bronze as a new batch.<br>3. Rebuild Silver.<br>4. Validate.<br>5. Reprocess the affected Gold objects. |
| **Only Gold processing failed** | Use the previously validated Silver data to reprocess the affected Gold objects. Do not repeat successful upstream processing unnecessarily. |
| **The archived source file is incorrect** | Obtain and archive a corrected file as a separate version, then process it as a new batch. |

### 5.2 Reload rules

- Every ingestion attempt receives a unique `BATCH_ID`.
- `BUSINESS_DT` identifies the business date represented by the data; `BATCH_ID` identifies the processing attempt.
- A corrected or reprocessed batch references its original batch using `PARENT_BATCH_ID`.
- Original Bronze batches are not overwritten.
- Downstream processing must distinguish approved batches from incorrect batches.
- Reprocessing must be idempotent and limited to the affected scope wherever possible.

---

## 6. Reconcilation

After a reload, perform the following checks:

- Bronze rows reconcile to accepted Silver `TMP_` rows plus rejected rows.
- Silver sales amount reconciles to the Gold sales fact amount for the same scope.
- Each dimension business key has exactly one current active version.
- Surrogate keys remain unique and mandatory foreign keys are valid.
- Reprocessing does not create duplicate business records.

Only successfully validated data should proceed to the next layer.

---

## 7. Batch Log

The Batch Log records ingestion, processing, failures, and reprocessing The retention/reload policy identifies these fields:

| Field                  | Purpose                                                                                     |
| ---------------------- | ------------------------------------------------------------------------------------------- |
| `BATCH_ID`             | Unique processing batch identifier.                                                         |
| `PARENT_BATCH_ID`      | Links a corrected/reprocessed batch to its original batch.                                  |
| `BUSINESS_DT`          | Business date represented by the data.                                                      |
| `SOURCE_FILE_NAME`     | Source file being processed.                                                                |
| `OBJECT_NAME`          | Table or object being processed.                                                            |
| `BATCH_STATUS`         | Processing status, such as `SUCCESS`, `FAILED`, or `REPROCESSING`.                          |
| `START_TS`, `END_TS`   | Processing start and completion timestamps.                                                 |

---
