# Migration: Legacy Snow Share Scripts → `snow_share_procedure_upgrade_to_cortex.sql`

This covers two independent legacy consumers that both migrate to the same
unified script, using different parameters:

- **Path A — Customer environment**: currently running
  [snow_share_procedure.sql](script/snow_share/snow_share_procedure.sql) (+ optionally
  [cortex_tables_snow_share.sql](script/snow_share/cortex_tables_snow_share.sql)).
- **Path B — Unravel's own environment**: currently running
  [backup/backup_shared_db.sql](script/snow_share/backup/backup_shared_db.sql) to copy each
  customer's shared schema into an internal Unravel database.

Both paths deploy the same file,
[snow_share_procedure_upgrade_to_cortex.sql](script/snow_share/snow_share_procedure_upgrade_to_cortex.sql),
but Path B does **not** run the file's built-in migration tail (that tail is
customer-side only — it references `UNRAVEL_SHARE.SCHEMA_4827_T` and
`SHARE_TO_ACCOUNT` explicitly). Path B instead deploys just the procedure
definitions and creates its own tasks with backup-appropriate parameters.

---

## Path A: Customer environment (`snow_share_procedure.sql` → unified script)

### What changes for the customer

- Legacy procedures have 2–4 required arguments
  (e.g. `REPLICATE_ACCOUNT_USAGE(DBNAME, SCHEMANAME, LOOK_BACK_DAYS)`). The new
  procedures add `SOURCE_DB`/`SOURCE_SCHEMA` as **defaulted** trailing parameters
  (e.g. `REPLICATE_ACCOUNT_USAGE(DB_NAME, SCHEMA_NAME, LOOK_BACK_DAYS, SOURCE_DB DEFAULT 'SNOWFLAKE', SOURCE_SCHEMA DEFAULT 'ACCOUNT_USAGE')`).
- **Snowflake will not let the old and new signatures coexist** — deploying the
  new defaulted overload while the old fixed-arity overload still exists raises
  `SQL compilation error: Cannot overload PROCEDURE '...' as it would cause
  ambiguous PROCEDURE overloading` (confirmed by live testing). The old overloads
  must be dropped, and any task bound to them must be rebound, in the same
  maintenance window.
- Ten new Cortex/AI usage tables are added; `STATUS_DATE` is backfilled on
  existing legacy tables.

### Steps

1. **Prepare**: capture current task DDL, procedure DDL, and task states for
   rollback:
   ```sql
   SELECT GET_DDL('TASK', 'UNRAVEL_SHARE.SCHEMA_4827_T.' || task_name) ...
   SHOW TASKS IN SCHEMA UNRAVEL_SHARE.SCHEMA_4827_T;
   SHOW GRANTS TO SHARE S_SECURE_SHARE;
   ```
2. **Edit the script copy** before running it:
   - Replace `'<Unravel Account Identifier>'` in the `SHARE_TO_ACCOUNT` call.
   - Confirm `UNRAVELDATA` is the customer's approved XS warehouse (or replace it).
   - Confirm `UNRAVEL_SHARE.SCHEMA_4827_T` matches the customer's actual legacy
     database/schema names (only if they deviated from the default).
3. **Run the edited script once**, as a role with rights to create
   databases/schemas/tables/tasks/shares and to read `SNOWFLAKE.ACCOUNT_USAGE`.
   In order, the script:
   - Suspends the six legacy tasks (`replicate_metadata`, `replicate_history_query`,
     `createProfileTable`, `replicate_warehouse_and_realtime_query`,
     `shared_db_metadata_task`, `REPL_TASK_CLEANUP_RETENTION`).
   - Drops the eight legacy fixed-arity procedure overloads.
   - Creates all new procedures (defaulted signatures, Cortex-aware).
   - Runs `CREATE_ACCOUNT_USAGE_TABLES` / `CREATE_DERIVED_TABLES` to add the ten
     Cortex tables and any missing derived tables to the existing schema.
   - Backfills `STATUS_DATE` on legacy rows where it is `NULL`.
   - Calls `SHARE_TO_ACCOUNT` to extend share grants to the new tables.
   - Recreates the six tasks bound to the new procedure signatures.
   - Resumes all six tasks.
   - Runs verification queries (new tables present, tasks resumed, share grants).
4. **Verify** the printed result sets: all ten Cortex table names returned, all
   six tasks shown with `state = started`, and the expected grants on
   `S_SECURE_SHARE`.
5. **Do not manually invoke `REPLICATE_ACCOUNT_USAGE`** as a post-migration
   catch-up call. Its snapshot tables and `INFORMATION_SCHEMA` table functions
   are not bounded by `LOOK_BACK_DAYS` and can run far longer than a short
   catch-up should. Let the rebound `replicate_metadata` task perform the next
   scheduled replication.

### Verification performed

A live integration test
([script/snow_share/test_snowshare_upgrade_to_cortex.py](script/snow_share/test_snowshare_upgrade_to_cortex.py))
was run against a disposable Snowflake database, starting from the actual
legacy `snow_share_procedure.sql` definitions:

- Legacy `CREATE_TABLES` + a legacy-signature task were created first.
- Legacy overloads were dropped and the new procedure set deployed, reproducing
  the documented ambiguous-overload failure until the drop order was corrected
  (drop legacy overloads **before** creating the new defaulted procedures).
- The task was rebound to the new signature and its DDL confirmed to reference
  the new `SOURCE_DB`/`SOURCE_SCHEMA` arguments.
- All ten Cortex tables were created; `CREATE_ACCOUNT_USAGE_TABLES` was proven
  idempotent (safe to re-run).
- A pre-existing table comment survived the migration unchanged.
- `REPLICATE_HISTORY_QUERY(..., LOOK_BACK_DAYS='0')` and
  `REPL_CLEANUP_RETENTION(...)` both completed successfully.
- `REPLICATE_ACCOUNT_USAGE` was attempted with `LOOK_BACK_DAYS='0'` and had to be
  cancelled — it was still running an `INFORMATION_SCHEMA`/snapshot-table path
  well past the XS smoke-test budget, confirming design issue #1/#5 in
  [DESIGN_snowshare.md](DESIGN_snowshare.md). This procedure is **not** safe to
  invoke ad hoc as part of a migration; only its scheduled task should run it.
- `SHARE_TO_ACCOUNT` was not invoked live (hard-coded production database/share
  name; must be validated in a dedicated sandbox account before first customer
  use).
- The scratch database was dropped in all runs (including after the cancelled
  query), confirming test cleanup does not leak Snowflake objects.

---

## Path B: Unravel's own backup pipeline (`backup_shared_db.sql` → unified script)

### What changes for Unravel's internal use

`backup_shared_db.sql`'s `CREATE_BACKUP_DB_TABLES` + `TP_CDC_METADATA` +
`cdc_metadata` task read from a customer's shared schema (mounted in Unravel's
account under some `SOURCE_DB.SOURCE_SCHEMA`) and copy it into an internal
`TARGET_DB.TARGET_SCHEMA`. The unified script's `CREATE_ACCOUNT_USAGE_TABLES` /
`CREATE_DERIVED_TABLES` / `REPLICATE_ACCOUNT_USAGE` / `REPLICATE_HISTORY_QUERY`
already accept exactly these parameters and cover a superset of the tables the
old backup script handled. There is no separate "backup script" to write —
the same procedures used on the customer side serve this role when called with
the customer's share as `SOURCE_DB`/`SOURCE_SCHEMA`.

### Steps

1. Deploy only the **procedure definitions** from
   [snow_share_procedure_upgrade_to_cortex.sql](script/snow_share/snow_share_procedure_upgrade_to_cortex.sql)
   in Unravel's account (skip the migration tail — it is customer-side only and
   references `UNRAVEL_SHARE.SCHEMA_4827_T` and `SHARE_TO_ACCOUNT` explicitly,
   neither of which applies here).
2. For each customer's mounted share database (`<CUSTOMER_SHARE_DB>.<SCHEMA>`)
   and its corresponding internal backup target
   (`<INTERNAL_DB>.<INTERNAL_SCHEMA>`):
   ```sql
   CALL CREATE_ACCOUNT_USAGE_TABLES('<CUSTOMER_SHARE_DB>', '<SCHEMA>', '<INTERNAL_DB>', '<INTERNAL_SCHEMA>');
   CALL CREATE_DERIVED_TABLES(TARGET_DB => '<INTERNAL_DB>', TARGET_SCHEMA => '<INTERNAL_SCHEMA>', SOURCE_DB => '<CUSTOMER_SHARE_DB>', SOURCE_SCHEMA => '<SCHEMA>');
   ```
3. Replace the old `cdc_metadata` task with new tasks bound to
   `REPLICATE_ACCOUNT_USAGE`/`REPLICATE_HISTORY_QUERY`, parameterized per
   customer:
   ```sql
   CREATE OR REPLACE TASK backup_replicate_metadata_<customer>
       WAREHOUSE = <internal warehouse>
       SCHEDULE = 'USING CRON 0 2 * * * UTC'
   AS
   CALL REPLICATE_ACCOUNT_USAGE('<INTERNAL_DB>', '<INTERNAL_SCHEMA>', '2', '<CUSTOMER_SHARE_DB>', '<SCHEMA>');
   ```
4. Drop `CREATE_BACKUP_DB_TABLES`, `TP_CDC_METADATA`, and the `cdc_metadata` task
   once the new tasks have run successfully at least once per customer and row
   counts have been spot-checked against the old backup tables.
5. Apply the same caution as Path A: do not manually invoke the full
   `REPLICATE_ACCOUNT_USAGE` dispatcher outside its scheduled task; category-3
   snapshot tables in it are unbounded regardless of which side is the source.

### Known behavior differences vs. the old backup script

- The old `TP_CDC_METADATA` used a `NOT IN` dedup condition, which silently
  breaks (stops replicating entirely) if any condition column is `NULL`, or does
  nothing at all for tables whose `condition` was left empty (`""` → `1=1`). The
  new procedures use `NOT EXISTS` on explicit PK columns, which does not have
  this failure mode — see [DESIGN_snowshare.md](DESIGN_snowshare.md) section 3.
- The new script still reproduces the old script's category-3 duplication
  behavior (full reload, no truncate, no dedup) for the same non-PK tables. This
  is a pre-existing issue in both the old and new scripts, not a regression
  introduced by migration — see [DESIGN_snowshare.md](DESIGN_snowshare.md) section 4,
  issue #1.
