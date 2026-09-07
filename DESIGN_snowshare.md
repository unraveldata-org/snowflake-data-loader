# Snow Share Design: Unified CDC Script (`snow_share_procedure_upgrade_to_cortex.sql`)

## 1. Background: what this replaces

Three scripts previously existed, each solving a different half of the pipeline:

| Script | Runs in | Purpose |
|---|---|---|
| [script/snow_share/snow_share_procedure.sql](script/snow_share/snow_share_procedure.sql) | Customer's Snowflake account | Reads `SNOWFLAKE.ACCOUNT_USAGE` / `INFORMATION_SCHEMA`, writes into `UNRAVEL_SHARE.SCHEMA_4827_T`, shares that schema out to Unravel's account via `S_SECURE_SHARE`. |
| [script/snow_share/cortex_tables_snow_share.sql](script/snow_share/cortex_tables_snow_share.sql) | Customer's Snowflake account | Addendum bolted onto the above: adds 10 Cortex/AI usage tables via its own `REPLICATE_CORTEX_USAGE` procedure/task. |
| [script/snow_share/backup/backup_shared_db.sql](script/snow_share/backup/backup_shared_db.sql) | Unravel's Snowflake account | Reads the customer's shared schema (as mounted in Unravel's account) and copies it into Unravel's own internal database (`TP_CDC_METADATA` + `CREATE_BACKUP_DB_TABLES` + `cdc_metadata` task). |

[script/snow_share/snow_share_procedure_upgrade_to_cortex.sql](script/snow_share/snow_share_procedure_upgrade_to_cortex.sql) unifies all three. Every core procedure
(`CREATE_ACCOUNT_USAGE_TABLES`, `CREATE_DERIVED_TABLES`, `REPLICATE_ACCOUNT_USAGE`,
`REPLICATE_HISTORY_QUERY`, `WAREHOUSE_PROC`, `CREATE_SHARED_DB_METADATA`) takes
`SOURCE_DB`/`SOURCE_SCHEMA` and `TARGET_DB`(`DB_NAME`)/`TARGET_SCHEMA`(`SCHEMA_NAME`)
as parameters instead of being hard-coded to `SNOWFLAKE.ACCOUNT_USAGE` →
`UNRAVEL_SHARE.SCHEMA_4827_T`. The same procedure body therefore serves two roles
depending only on which parameters the caller supplies:

- **Customer-side create + share**: `SOURCE_DB/SOURCE_SCHEMA = SNOWFLAKE/ACCOUNT_USAGE`,
  `TARGET = UNRAVEL_SHARE.SCHEMA_4827_T`, followed by `SHARE_TO_ACCOUNT`.
- **Unravel-side backup**: `SOURCE_DB/SOURCE_SCHEMA` = the customer's share as mounted
  in Unravel's account, `TARGET` = Unravel's own internal database/schema for that
  customer. This directly replaces `CREATE_BACKUP_DB_TABLES` + `TP_CDC_METADATA` +
  `cdc_metadata` task from `backup_shared_db.sql`.

`SHARE_TO_ACCOUNT` is the one procedure that is **not** dual-mode — it hard-codes
`UNRAVEL_SHARE.SCHEMA_4827_T` and a placeholder account ID, so it only makes sense
on the customer side.

## 2. Replication strategy per table category

The script buckets every source table into one of three load strategies:

1. **Time-bounded + primary key** (majority of history tables, e.g.
   `WAREHOUSE_METERING_HISTORY`, `QUERY_HISTORY`, `CORTEX_ANALYST_USAGE_HISTORY`):
   `INSERT ... SELECT ... FROM (windowed source) w WHERE NOT EXISTS (SELECT 1 FROM target t WHERE <PK join>)`.
   Window: `col >= DATEADD('day', -LOOK_BACK_DAYS, NOW) AND col <= NOW` (using
   `TO_TIMESTAMP_TZ(CURRENT_TIMESTAMP())` consistently as the upper bound, and
   `OR end_col >= window_start` when an end column exists, to catch long-running
   rows that started before the window but are still open).
2. **Time-bounded, no primary key** (`CORTEX_AGENT_USAGE_HISTORY`,
   `CORTEX_AI_FUNCTIONS_USAGE_HISTORY`, `CORTEX_SEARCH_BATCH_QUERY_USAGE_HISTORY`,
   `CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY`, `SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY`,
   `QUERY_ATTRIBUTION_HISTORY`, `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY`,
   `CORTEX_CODE_CLI_USAGE_HISTORY`, `CORTEX_AI_GUARDRAILS_USAGE_HISTORY`, and the
   `SNOWFLAKE.INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY()` fresh-install
   path): plain `INSERT ... SELECT` over the window, no dedup.
3. **No time column / full snapshot** (`TABLES`, `TABLE_STORAGE_METRICS`, `COLUMNS`,
   `TAGS`, `TAG_REFERENCES`, `ROLES`, `GRANTS_TO_ROLES`, `GRANTS_TO_USERS`, `USERS`,
   `CORTEX_REST_API_RATE_LIMIT_POLICIES`): `INSERT INTO target SELECT * FROM source`
   — the entire source table, every run, with **no truncate and no dedup**. (Note:
   `REPLICATE_ACCOUNT_USAGE`'s `replicateData()`, where these tables actually live,
   has no `truncateTable(...)` call at all, commented or otherwise. The commented-out
   `//truncateTable(tableName);` lines live in `REPLICATE_HISTORY_QUERY` instead,
   which only handles category-1 tables (`SESSIONS`, `ACCESS_HISTORY`,
   `QUERY_HISTORY`, `QUERY_INSIGHTS`). Those calls are also dead code as written:
   the only `truncateTable` function defined anywhere in the script lives inside
   `REPL_CLEANUP_RETENTION`, takes 4 args (`tableName, startCol, endCol,
   truncateDurationHours`), does a `DELETE`-by-age rather than a real `TRUNCATE`,
   and is out of scope for `REPLICATE_HISTORY_QUERY`'s single-arg call — uncommenting
   it as-is would throw a `ReferenceError`, not truncate anything.)

Cleanup is handled separately by `REPL_CLEANUP_RETENTION`, which deletes rows whose
event-time column (or `STATUS_DATE` for snapshot tables) is older than a fixed
48-hour cutoff, run on its own 6-hour cron task.

`STATUS_DATE` is injected as `CURRENT_DATE()` at **insert time** on every row
(`buildSelectLists` rewrites any `STATUS_DATE` target column to
`CURRENT_DATE() AS "STATUS_DATE"`). It is a load/ingestion marker used only by the
retention job — it is not an event timestamp and must never be used for
chronological ordering or "as of" queries.

## 3. Exactly-once evaluation

**Category 1 (time-bounded + PK) is the only category that is actually exactly-once
for row insertion**, and only insertion — see the freshness caveat below.

**Category 2 (time-bounded, no PK) is NOT exactly-once.** Any overlap between two
runs' windows (guaranteed by design: `LOOK_BACK_DAYS=2` re-scans the last 2 days on
every run, and `replicate_metadata` is scheduled every 6 hours) will re-insert every
row from the previous run's overlap. After the first run, every subsequent run
duplicates rows for these 9 tables. This is a correctness bug, not just an
inefficiency, for any dashboard/report that sums or counts rows from these tables.

**Category 3 (full snapshot) is actively duplicate-generating by design.** Every
single invocation re-inserts the *entire* source table with no truncate and no
dedup. After `N` runs, `TABLES`/`COLUMNS`/`TAGS`/`USERS`/etc. contain `N` full
copies of the source. This is both a correctness bug (row counts, joins, and
"latest metadata" queries are all wrong) and the primary driver of unbounded
storage growth and query slowdown on these tables — for large accounts, `COLUMNS`
alone can be tens of thousands of rows per snapshot, growing without bound.

**`REPL_CLEANUP_RETENTION` does not fix category 2/3 duplication** — it only
deletes rows older than 48 hours. Between the last two cleanup runs (up to 6 hours
of accumulated duplicates for category 2, up to 6 hours × however many replicate
runs happened for category 3), duplicates exist and are visible to any consumer
querying live data.

### The freshness gap in category 1 (the timeline-respect issue)

Category 1 uses `INSERT ... WHERE NOT EXISTS (PK match)` — this is insert-once,
never update. Snowflake's `ACCOUNT_USAGE.QUERY_HISTORY` (and similarly
`WAREHOUSE_EVENTS_HISTORY`, `SESSIONS`, etc.) can show a row for a query that is
still `RUNNING` before the view later reflects its final `END_TIME`,
`TOTAL_ELAPSED_TIME`, and `EXECUTION_STATUS`. Because the PK
(`QUERY_ID` for `QUERY_HISTORY`, etc.) never changes, once a row is captured by
`NOT EXISTS`, **it is captured forever in its first-seen state** — there is no
`MERGE`/`UPDATE` anywhere in the script. A query captured mid-flight will never be
corrected in the target table even after Snowflake finalizes it. This is the most
important gap relative to the stated goal ("data respects the Snowflake
timeline"): row *presence* is exactly-once, but row *content* can silently go
stale relative to the source of truth.

### Comparison with the old backup script's dedup logic

`backup_shared_db.sql`'s `TP_CDC_METADATA` used:

```sql
WHERE <condition columns> NOT IN (SELECT <condition columns> FROM target)
```

This has a known, severe bug: **`NOT IN` returns no rows at all if the subquery
produces even a single `NULL`** in any of the condition columns. Several of its
condition strings (`"TABLE_STORAGE_METRICS"`, `"WAREHOUSE_PARAMETERS"`,
`"WAREHOUSES"`, `"AUTO_REFRESH_REGISTRATION_HISTORY"`, `"IS_QUERY_HISTORY"`) use an
empty condition (`""` → falls back to `1=1`, meaning **no dedup and no filtering
at all**, i.e. always a full duplicate reload), and others concatenate columns
that can legitimately be `NULL` (e.g. `WAREHOUSE_ID` on some rows), which can
silently stop *all* future inserts into that table once a single `NULL` row
exists in the target. The new script's `NOT EXISTS` pattern with explicit PK
columns avoids the `NOT IN`/`NULL` trap entirely — this is a genuine correctness
improvement over the old backup script.

## 4. Loopholes, bottlenecks, and correctness issues found

| # | Location | Issue | Impact | Status |
|---|---|---|---|---|
| 1 | `REPLICATE_ACCOUNT_USAGE` / `REPLICATE_HISTORY_QUERY` category 3 tables | No truncate, no dedup, full reload every run | Unbounded duplicate accumulation; storage growth; slow downstream queries | **Fixed** — `insertToTable()`'s no-date branch now builds the fresh copy in a transient staging table, then `ALTER TABLE ... SWAP WITH` it in atomically (zero-copy metadata op). If the staging build fails, the real target is never touched, so this is also self-recovering. Validated live against an X-Small warehouse: two back-to-back runs against identical source data produce identical row counts. |
| 2 | `REPLICATE_ACCOUNT_USAGE` category 2 tables (9 Cortex/AI tables + fresh-install `AUTO_REFRESH_REGISTRATION_HISTORY`) | No PK-based dedup despite time window | Duplicates on every overlapping run | **Fixed** — these tables now get a stored `ROW_HASH NUMBER` column (added via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` alongside `STATUS_DATE`), and dedup via `NOT EXISTS (... WHERE t."ROW_HASH" = HASH(w.col1, w.col2, ...) AND t."STATUS_DATE" >= ...)`. This avoids guessing an unverified business PK for newer Cortex views while still being exact (only true duplicate rows are skipped) and cheap (the target-side `STATUS_DATE` filter prunes the comparison to only the last `LOOK_BACK_DAYS`, not the whole table). Validated live: a second run against identical source data produces zero new rows, and a third run with genuinely new source rows correctly inserts only the new ones. |
| 3 | All PK-deduped tables | `INSERT ... WHERE NOT EXISTS`, never `UPDATE`/`MERGE` | Rows captured while "in flight" (e.g. running queries) never get corrected once Snowflake finalizes them | Not yet implemented |
| 4 | `getColumns()` (duplicated in 6+ procedures) | Runs a fresh `LISTAGG` over `INFORMATION_SCHEMA.COLUMNS` on **every call, every table, every run** — no caching. In `REPLICATE_REALTIME_QUERY_BY_WAREHOUSE` specifically, the call sits *inside* the per-warehouse `while` loop, so it re-queries the same table's columns once per warehouse per run, not just once per run | Extra warehouse compute per run; on the XS test warehouse this metadata lookup was slow enough to need cancellation during a live test even in an empty scratch schema | **Fixed** — every `getColumns()` implementation (7 across both scripts, including the 3-arg variant in `CREATE_SHARED_DB_METADATA`) now memoizes results in a per-procedure-call `_columnsCache` map; the `REPLICATE_REALTIME_QUERY_BY_WAREHOUSE` call is also hoisted out of the per-warehouse loop entirely. Validated live: 5 logical calls for the same table produced exactly 1 real `INFORMATION_SCHEMA.COLUMNS` query (confirmed via `QUERY_HISTORY`). |
| 5 | `REPLICATE_ACCOUNT_USAGE`, full dispatcher | Category-3 full reloads and category-1 windowed loads run in the same procedure/task with no per-table timeout or circuit breaker | One slow/large table (e.g. `COLUMNS` on a large account) can consume most of the 4-hour procedure budget and starve later tables in the same call | **Fixed differently** — a risk-ordered dispatch + time-budget circuit breaker was proposed and validated but the decision was made not to include it (adds runtime complexity for a benefit already covered by the fix below). Instead, `REPLICATE_ACCOUNT_USAGE` gained a `TABLE_GROUP` parameter (`'HISTORY'`/`'SNAPSHOTS'`/`'ALL'`) and is now invoked as two independent tasks on the same schedule — see recommendation #5 below for the full verification + implementation. |
| 6 | `REPLICATE_REALTIME_QUERY_BY_WAREHOUSE` | `DELETE FROM IS_QUERY_HISTORY WHERE WAREHOUSE_NAME = ...` followed by a separate `INSERT` per warehouse, in a loop, with no transaction wrapping the pair | If two overlapping task runs interleave, a `DELETE` from one run can remove rows just inserted by another, or a warehouse's data can be briefly empty between the `DELETE` and `INSERT` | Not yet implemented |
| 7 | `CREATE_QUERY_PROFILE` | Loops row-by-row over `NOT EXISTS`-diffed query IDs and issues one `get_query_operator_stats()` call per row, serially, on a 60-minute schedule | Scales linearly with query volume; can approach or exceed the schedule interval (or the 4h procedure timeout) on high-query-volume accounts | Not yet implemented |
| 8 | `CREATE_SHARED_DB_METADATA` | The `SOURCE_DB` explicit-parameter code path does not filter out the `SNOWFLAKE` database the way the `SHOW SHARES` fallback path does | Inconsistent behavior depending on which call site is used | Not yet implemented |
| 9 | `WAREHOUSE_PROC` | Two mutually exclusive code paths in the same procedure: a truncate-and-full-reload path (no `SOURCE_DB`) and a PK-deduped incremental path (`SOURCE_DB` provided) | If a customer/task ever calls both variants against the same target table, the semantics of `WAREHOUSES`/`WAREHOUSE_PARAMETERS` become inconsistent (partially truncated-and-reloaded, partially incrementally CDC'd) | Not yet implemented |
| 10 | `REPL_CLEANUP_RETENTION` | Fixed 48-hour retention window applies uniformly, including to snapshot tables silently accumulating duplicates from issue #1, and `REPLICATION_LOG` itself was never cleaned up at all | Retention masks but does not fix the duplication; between cleanup runs, duplicate rows are fully visible to consumers; `REPLICATION_LOG` could grow unbounded | **Partially fixed** — issue #1's root cause is now fixed, so there are no more category-3 duplicates for retention to mask. Additionally, `REPLICATION_LOG` is now included in `REPL_CLEANUP_RETENTION` with a 180-day (not 48-hour) window, since it's the debugging trail rather than customer usage data. This required a small fix to `truncateTable()`: `REPLICATION_LOG.eventDate` is stored as `TIMESTAMP_TZ`, and Snowflake's `TRY_TO_TIMESTAMP_TZ()` does not support a `TIMESTAMP_TZ -> TIMESTAMP_TZ` cast (compile error) the way it supports the `TIMESTAMP_LTZ` columns used by every other table here — confirmed live against a real Snowflake sandbox. The repeated `48`-hour literal (52 occurrences) was also consolidated into a single `DEFAULT_RETENTION_HOURS` constant (and the `REPLICATION_LOG` window into `REPLICATION_LOG_RETENTION_HOURS`), so changing the retention window is now a one-line edit. **⚠️ New finding, not yet fixed:** while validating this, live testing revealed `TRY_TO_TIMESTAMP_TZ()` actually fails to cast **any** already-timestamp-typed column — confirmed for both `TIMESTAMP_LTZ` (the type real `ACCOUNT_USAGE` columns use) and `TIMESTAMP_NTZ` columns, not just `TIMESTAMP_TZ` as originally documented above. This means `truncateTable()`'s `DELETE` likely throws a caught-and-swallowed compile error for every regular history/snapshot table, every run — retention for anything other than `REPLICATION_LOG` (which uses the direct-comparison special case) may never have actually deleted a row. This needs its own fix (the same direct-comparison approach used for `REPLICATION_LOG`, generalized) and was out of scope for this pass. |
| 11 | `SHARE_TO_ACCOUNT` | Hard-coded `UNRAVEL_SHARE.SCHEMA_4827_T` and a literal placeholder account ID (`'<Unravel Account Identifier>'`) | Not reusable for the backup/Unravel-side use case; must be manually edited per deployment; a forgotten edit fails loudly (good) but is a manual step with no validation | **Partially fixed** — the ~55 individually-enumerated `GRANT SELECT ON TABLE ...` statements were replaced with a single dynamic `GRANT SELECT ON ALL TABLES IN SCHEMA ...` (Snowflake does not support `FUTURE TABLES ... TO SHARE`, confirmed live — restricted — so a newly added table still needs one re-call of this procedure, but with zero script edits). `DB_NAME`/`SCHEMA_NAME` are now declared as local `DECLARE`d constants inside the procedure body instead of being hardcoded ~55 times, so retargeting the deployment is a two-line edit. They were deliberately **not** added as procedure parameters: doing so creates a second, differently-defaulted overload of the same name, which Snowflake rejects as "ambiguous PROCEDURE overloading" for any existing deployment that already has the legacy `SHARE_TO_ACCOUNT(VARCHAR)` signature — confirmed live, and caught by the existing test suite. The account ID placeholder remains a manual per-deployment edit at the single `CALL SHARE_TO_ACCOUNT(...)` site, unchanged. |

Every table also now gets a per-table completion log row in `REPLICATION_LOG`
(`table=<name> rows=<n> ms=<duration>` on success, or
`table=<name> err=<truncated message> ms=<duration>` on failure) instead of only
a single start/end line for the whole procedure — one row per table per run, not
per source record, to keep the log informative without flooding it.

## 5. Recommendations

1. ~~**Either truncate-and-reload or PK-dedupe category 3 tables.**~~ **Implemented**
   via build-in-staging + `SWAP` (see issue #1 above).
2. ~~**Give category-2 tables a PK-based `NOT EXISTS` join** the same as category 1~~
   **Implemented differently**: rather than guessing unverified PK columns for the
   newer Cortex usage views, dedup uses a stored content hash (`ROW_HASH`) instead
   (see issue #2 above). If real composite PKs for these views are later confirmed
   against a live account's `INFORMATION_SCHEMA.COLUMNS`, switching to a PK-based
   join remains a drop-in change (just pass the PK CSV instead of `useHashDedup=true`
   at the `replicateData(...)` call site).
3. **Add `MERGE` semantics for at least `QUERY_HISTORY`/`ACCESS_HISTORY`/`SESSIONS`**
   (or any table whose rows can still be open/mutable when first captured), so a
   row's terminal state is reflected once Snowflake finalizes it, instead of
   freezing at first-seen state.
4. ~~**Cache `getColumns()` results per table per procedure call**~~ **Implemented**
   (see issue #4 above).
5. ~~**Splitting heavy procedures into smaller ones (verified, not yet implemented).**~~
   **Implemented.** `REPLICATE_ACCOUNT_USAGE` gained a `TABLE_GROUP STRING DEFAULT 'ALL'`
   parameter (`'HISTORY'` = ~33 time-windowed tables, categories 1+2; `'SNAPSHOTS'` = 10
   full-refresh tables, category 3; `'ALL'` = both, preserving the original single-call
   behavior for any caller that omits the parameter). The `replicate_metadata` task now
   calls `TABLE_GROUP => 'HISTORY'`, and a new `replicate_metadata_snapshots` task calls
   `TABLE_GROUP => 'SNAPSHOTS'` — both on the **same** cron schedule as independent tasks
   (not a DAG/`AFTER` chain), since the two table sets are fully disjoint and there is no
   reason to couple their success/failure. This confirms and acts on the verified finding
   above: each task now gets its own `STATEMENT_TIMEOUT_IN_SECONDS`/`USER_TASK_TIMEOUT_MS`
   budget, so a slow/large snapshot table (e.g. `COLUMNS` on a big account) can no longer
   threaten the ~33 bounded history tables' run, and a failed snapshots run shows up
   distinctly in task history instead of masking an otherwise-successful history batch.
   Validated live: `TABLE_GROUP='HISTORY'` and `'SNAPSHOTS'` each touch only their own
   disjoint table set with no overlap or duplication, the omitted-parameter default
   reproduces the original full-coverage behavior, and both `CREATE TASK` DDL statements
   compile, bind, and resume correctly.
6. **Parameterize `SHARE_TO_ACCOUNT`** (target db/schema/share name/account list)
   so it does not require manual literal edits per deployment.
