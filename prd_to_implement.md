PRD — LookML-Grounded Text-to-SQL (MVP) for E-commerce on BigQuery
1) Introduction / Overview

Build a single-turn, command-style Text-to-SQL assistant that outputs minimal BigQuery Standard SQL for an e-commerce dataset. The system is grounded in LookML (views, explores, model) stored in a GitHub repo and validated against BigQuery metadata (INFORMATION_SCHEMA), including column descriptions when present. The generator only considers fields/joins present in LookML, using BigQuery metadata strictly for validation and context (types, descriptions).

Output philosophy: minimal executable SQL (no annotations) with a forced LIMIT 100 when the user omits a limit.

2) Data Sources & Provenance
2.1 LookML (Primary Semantic Layer)

Source: 

Eventually GitHub repository (read-only), for now leverage data found from these instructions on this link: https://cloud.google.com/looker/docs/looker-core-sample-project.

Files: LookML model files (*.model.lkml), explores, and views (*.view.lkml).

Access: Clone locally via deploy key or HTTPS; parse locally (no Looker API). (Eventually, download leveraging api for now)

Use: Parse sql_table_name, dimensions (name, type, sql, description), measures (name, type, sql, description), primary keys, timeframes, hidden flags; parse explore joins (join type, sql_on, relationship, required, view_name). Treat LookML as an allow-list of eligible fields/joins.

2.2 BigQuery Warehouse (Validation & Descriptions)

Dataset (public, read-only): bigquery-public-data.thelook_ecommerce (US region).
More info found here: https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce?project=brainrot-453319
Purpose: Validate physical tables/columns/types referenced by LookML and enrich with column descriptions when present.

Metadata views:

DATASET.INFORMATION_SCHEMA.COLUMNS — column names & types.

DATASET.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS — nested paths + DESCRIPTION.

(Optional) DATASET.INFORMATION_SCHEMA.TABLES — table presence/row_count (if available).

Example metadata queries

-- Column names & types
SELECT table_name, column_name, data_type
FROM `bigquery-public-data.thelook_ecommerce.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name IN (<tables_from_lookml>);

-- Nested fields & descriptions (preferred where available)
SELECT table_name, column_name, field_path, data_type, description
FROM `bigquery-public-data.thelook_ecommerce.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
WHERE table_name IN (<tables_from_lookml>);


Tables (illustrative; actual usage is constrained by LookML):
orders, order_items, products, events, users, inventory_items, distribution_centers, order_payments (subset may vary by LookML).

Regioning: Ensure queries run in the US location to match the dataset; do not cross regions.

3) Goals

Accept a one-shot natural-language question and return valid BigQuery SQL aligned to an Explore’s allowed fields & joins.

Respect LookML semantics (explore join graph, field visibility, sql_table_name mappings).

Validate physical columns/types and enrich grounding with BigQuery column descriptions when present.

Enforce cost guardrail: automatically append LIMIT 100 when absent.

Low latency: p50 < 5s on cached metadata; generation only (no execution).

4) User Stories

As an analyst, I can ask “average order value by device in the last 30 days” and receive minimal SQL that runs as-is in BigQuery.

As a BI dev, I can trust the SQL won’t reference fields or joins outside those defined in LookML.

As a product owner, I want the model to leverage field/column descriptions to correctly interpret business terms (e.g., AOV, repeat customer).

5) Functional Requirements
FR-1: LookML Ingestion

Parse LookML from local clone.

Extract per-view: name, sql_table_name, dimensions (name, type, sql, description), measures (name, type, sql, description), primary key, hidden flag, timeframes.

Extract per-explore: base view, available fields, declared joins (type, sql_on, relationship, required, view_name).

Only fields/joins declared in the Explore graph are considered eligible in generated SQL.

FR-2: BigQuery Metadata Loader (Validation + Descriptions)

For each table referenced by LookML sql_table_name, load columns & types from INFORMATION_SCHEMA.COLUMNS.

Load descriptions from INFORMATION_SCHEMA.COLUMN_FIELD_PATHS when present.

Cache metadata locally (in-memory + optional JSON cache).

FR-3: Grounding Index

Map LookML fields → physical columns/expressions (resolve ${TABLE} and ${view.field}).

Support simple sql: expressions (CASE, arithmetic) captured verbatim.

Merge LookML description and BQ column description into a unified glossary for synonym mapping.

FR-4: NL → SQL Generation (Single Turn)

Explore selection: heuristic term overlap + tie-break rules.

Field selection: use only fields available within selected Explore.

Join plan: include only declared joins; prune unused.

SQL generation: minimal BigQuery Standard SQL; no comments; only necessary CTEs.

Guardrail: if no LIMIT present, append LIMIT 100.

FR-5: Validation

Optional BigQuery dry-run (config flag). If compile fails, return SQL plus short error tag (no auto-repair).

FR-6: I/O Contract

Input: plain text string.

Output: plain text string containing only the SQL.

Telemetry (internal): question, selected Explore, fields used, limit-auto-applied (bool).

6) Non-Goals (MVP)

No multi-turn chat or interactive clarifications.

No charts, result previews, or Looker Explore URLs.

No row-level security emulation or access filters.

No inference of fields/joins outside LookML (BQ is for validation only).

7) Design Considerations

Parsing: Python lkml for *.lkml files.

Expression resolution: ${TABLE} and ${view.field}; simple expressions only (defer complex PDTs).

Explore routing: keyword/synonym overlap from the merged glossary; LLM tie-breaker allowed but constrained by LookML.

BQ location: run metadata queries in US to match dataset region.

8) Architecture & Flow

LookML Loader → Explore Graph

BQ Metadata Loader → COLUMNS + COLUMN_FIELD_PATHS

Grounding Index → (LookML + BQ + descriptions)

Planner → explore selection, field selection, join pruning

Generator → minimal SQL + enforced LIMIT 100

(Optional) Dry-run → compile check

Return SQL

9) Implementation Plan

Stack: Python 3.11+, lkml, google-cloud-bigquery, pydantic (schemas), optional sqlglot.

Config: YAML/ENV for repo path, project/dataset, region, dry-run flag, default limit.

Limit enforcement: regex \bLIMIT\s+\d+\b; otherwise append LIMIT 100.

Tests: offline fixtures for LookML and JSON metadata snapshots.

10) Success Metrics

≥90% valid SQL using only LookML-declared fields on a 50-question eval.

100% have LIMIT when user omits it.

p50 latency < 5s (metadata cached).

<5% dry-run failures (if enabled).

11) Risks & Mitigations

LookML↔warehouse drift: add startup check + refresh command.

Ambiguous intent: conservative routing; default to high-coverage explores.

Sparse descriptions: fallback to names; maintain small synonym list.

Join explosion: prune unused; cap depth.

12) Open Questions

Tie between explores: prefer orders or fewest joins?

Support “last 30 days” natural phrasing now or defer?

Optional flag to return commented SQL alongside minimal?

13) Acceptance Criteria

Given a LookML repo and BigQuery access to bigquery-public-data.thelook_ecommerce, the service:

Parses LookML, builds the Explore Graph, loads BQ metadata + descriptions for referenced tables.

Generates minimal, executable BigQuery SQL for a one-shot question using only fields available in the selected Explore.

Appends LIMIT 100 when absent.

(If dry-run enabled) the SQL compiles without invalid references in ≥95% of a curated eval set.

14) Milestones

M1 (Parsing & Metadata): LookML parsed, tables discovered, BQ metadata & descriptions cached.

M2 (Planner & Generator): Explore routing, field selection, join pruning; SQL generator with limit enforcement.

M3 (Validation & Eval): Optional dry-run wiring; 50-question eval; metrics dashboard.

M4 (Polish): Config, CLI wrapper, README, sample prompts for the e-commerce dataset.