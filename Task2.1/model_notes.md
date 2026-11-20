# Retail Analytics Schema — Design Notes

## 1. Fact table grains

| Fact table | Grain — what one row represents |
|---|---|
| `FCT_SALES` | One row per one item line per day, per location |
| `FCT_PRICE` | One row per day, item, and location. |
| `FCT_INVENTORY` | One row per day, item, and location. |
| `FCT_SALES_PER_WEEK` | One row per week, item, location. |

## 2. SCD Type 2 Attributes and Rationale

| Dimension | SCD2 attribute(s) | Reason |
|---|---|---|
| `DIM_COMPANY` | `COMPANY_NAME` | Preserve historical company names for reporting. |
| `DIM_DEPARTMENT` | `DEPARTMENT_NAME` | Track department-name changes over time. |
| `DIM_CLASS` | `CLASS_NAME` | Track class-name changes over time. |
| `DIM_ITEM` | `ITEM_NAME`, `COLOR`, `SIZE` | Preserve item description and characteristic changes for historical analysis. |
| `DIM_STATE` | `STATE_CODE` | Retain history when a state code changes. |
| `DIM_DISTRICT` | `DISTRICT_CODE` | Retain history when a district code changes. |
| `DIM_LOCATION` | `LOCATION_NAME` | Preserve historical location names. |
| `DIM_CUSTOMER` | `CUSTOMER_PHONE` | Retain previous phone values for historical reference. |

## 3. Assumptions

| Area | Assumption |
|---|---|
| Date coverage | The date hierarchy is seeded for `2026-01-01` through `2035-12-31`. |
| Week definition | Weeks follow ISO rules: Monday start; a week is assigned to the month containing its Thursday. |
| Fiscal year | `FISCAL_YEAR_LABEL` is generated from the calendar year (for example, `FY2026`) |
| Date hierarchy | The hierarchy is `DIM_YEAR` → `DIM_QUARTER` → `DIM_MONTH` → `DIM_WEEK` → `DIM_DAY`. |
| Product hierarchy | The hierarchy is `DIM_COMPANY` → `DIM_DEPARTMENT` → `DIM_CLASS` → `DIM_ITEM`. |
| Location hierarchy | The hierarchy is `DIM_STATE` → `DIM_DISTRICT` → `DIM_LOCATION`. |
| SCD2 handling | Effective dates and `CURR_FLG` are intended to support Type 2 history; load logic must create new versions and expire old ones. |
| Sales grain | The grain is assumed to be one row per item line in a transaction (bill), identified by the Transaction ID and Transaction Line Number. |
| Price and inventory | Price and inventory are recorded as daily snapshots by date, item, and location, allowing historical values to be checked for a given day and effective dates to determine when a price or inventory status applies.|
| Key integrity | Declared keys document intended relationships; the load process must validate relationships and duplicates. |


