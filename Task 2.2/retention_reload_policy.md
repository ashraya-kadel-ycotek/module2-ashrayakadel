# Data Warehouse Retention Policy

## 1. Purpose

This document defines the data retention and recovery policies for the designed architecture.

The policy establishes:
- How long data is retained in each warehouse layer.
- How source files are archived.
- How historical data is maintained.


## 2. Retention Policy Overview

The following table defines the retention period for each layer.

| Layer / Component | Retention Policy | Description |
|---|---|---|
| Source Files | Archived | Daily source files are kept in an archive for recovery and reprocessing purposes. |
| Bronze | 30 days | Raw ingested data is retained for 30 days. |
| Silver | 30 days | Cleaned and validated data is retained for 30 days. |
| Gold | Historical maintenance | Historical business data is maintained in the Gold layer. |


## 3. Layer-Specific Retention Policies

### 3.1 Source Layer

- Daily source files are stored in an archive.
- Archived files act as the original source for recovery and reprocessing.
- Original source files should not be overwritten when corrected files are received.
- Corrected files should be archived as separate versions so that the original and corrected files can be distinguished.

### 3.2 Bronze Layer

- Raw data is retained for 30 days.
- Bronze data is stored in an append-oriented manner.
- If a source file contains incorrect data, the corrected file is ingested as a new batch.
- Previously ingested Bronze records are not overwritten during correction.
- Original batches are retained until they reach the 30-day retention limit.

### 3.3 Silver Layer

- Cleaned and validated data is retained for 30 days.
- Silver data is processed from the corresponding Bronze batches.
- If a Bronze batch requires reprocessing, the corresponding Silver data is rebuilt for the affected scope.

### 3.4 Gold Layer

- Gold data is historically maintained.
- Corrected or reprocessed data must update the affected Gold records without creating duplicate business records.
- Historical dimension versions must be preserved according to the applicable historical tracking rules.
- Fact data must remain consistent with the corrected source information.

### 3.5 Batch Log

The Batch Log records information about ingestion, processing, failures, and reprocessing activities.

Recommended information to maintain includes:

| Field | Description |
|---|---|
| BATCH_ID | Unique identifier for each processing batch. |
| PARENT_BATCH_ID | Identifies the original batch associated with a corrected or reprocessed batch. |
| BUSINESS_DT | Business date represented by the data. |
| SOURCE_FILE_NAME | Name of the source file being processed. |
| OBJECT_NAME | Table or object being processed. |
| BATCH_STATUS | Processing status, such as SUCCESS, FAILED, or REPROCESSING. |
| START_TS | Processing start timestamp. |
| END_TS | Processing completion timestamp. |


## 4. Reload and Recovery Policy

The warehouse is mostly focused on **File-level reprocessing**. It means to reprocess only the affected source file and its dependent objects.

### 4.1 Bronze and Silver Retention Window (Within 30 Days)

When the affected Bronze and Silver data is still available:

- Identify the affected source file, batch, and business date.
- Reprocess the affected Bronze batch or batches.
- Rebuild the corresponding Silver data.
- Validate the reprocessed data before proceeding to Gold.

Here, original Bronze batches are retained with the corrected source files are ingested as new batches.

### 4.2 After the 30-Day Retention Period

When the required Bronze and Silver data has expired:

- Retrieve the original source file from the source archive.
- Re-ingest the file into Bronze as a new batch.
- Rebuild the corresponding Silver data.
- Validate the reprocessed data.
- Reprocess the affected Gold objects.

If the archived source file itself is incorrect, a corrected source file must be obtained and processed as a separate version.

### 4.3 Gold-Only Failure

If Bronze and Silver processing has completed successfully and the failure occurs only in Gold:

- Use the previously validated Silver data as the input for Gold recovery.
- Reprocess the affected Gold objects.
- Perform the required validations and reconciliations.

This avoids repeating successful upstream processing unnecessarily.

## 5. Batch and Reprocessing Rules

The following rules apply during normal ingestion and reprocessing:

1. Each ingestion attempt must have a unique `BATCH_ID`.
2. `BUSINESS_DT` identifies the business date represented by the data whereas `BATCH_ID` identifies a particular processing attempt.
3. A corrected batch should reference its original batch through `PARENT_BATCH_ID`.
4. Original Bronze batches must not be overwritten by corrected batches.
5. Downstream processing must distinguish the approved batch from incorrect batches.
6. Reprocessing must be idempotent.
7. Reprocessing must be limited to the affected scope wherever possible.

## 6. Data Validation After Reprocessing

After reprocessing, the following checks should be performed:

| Validation | Expected Result |
|---|---|
| Bronze-to-Silver reconciliation | Bronze rows = Silver TMP rows + rejected rows. |
| Silver-to-Gold sales reconciliation | Silver sales amount = Gold sales fact amount for the corresponding scope. |
| Dimension current-record validation | Exactly one current active dimension version per business key. |
| Surrogate key validation | Surrogate keys remain unique. |
| Foreign key validation | Mandatory foreign key relationships are valid. |
| Duplicate validation | Reprocessing does not create duplicate business records. |

Only successfully validated data should proceed to the next layer.
