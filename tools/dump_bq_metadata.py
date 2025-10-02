#!/usr/bin/env python3
"""
Dump BigQuery DDL and schema JSON for all tables (and routines) in a dataset.

Usage:
  python tools/dump_bq_metadata.py --project YOUR_PROJECT --dataset YOUR_DATASET --outdir infra/bq

Requires:
  pip install google-cloud-bigquery
Auth:
  gcloud auth application-default login
"""
from __future__ import annotations
import argparse, json, os, pathlib
from typing import Any
from google.cloud import bigquery

def to_schema_json(fields):
    def field_to_dict(f):
        d = {
            "name": f.name,
            "type": f.field_type,
            "mode": f.mode,
            "description": f.description or "",
        }
        if f.fields:  # RECORD
            d["fields"] = [field_to_dict(sf) for sf in f.fields]
        return d
    return [field_to_dict(f) for f in fields]

def write_text(path: pathlib.Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8")

def write_json(path: pathlib.Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--outdir", default="infra/bq")
    args = ap.parse_args()

    client = bigquery.Client(project=args.project)
    dataset_id = f"{args.project}.{args.dataset}"
    out = pathlib.Path(args.outdir)

    # ---- Tables & Views: DDL + schema JSON
    ddl_sql = f"""
    SELECT table_name, table_type, ddl
    FROM `{dataset_id}.INFORMATION_SCHEMA.TABLES`
    ORDER BY table_name
    """
    ddl_rows = list(client.query(ddl_sql).result())

    for r in ddl_rows:
        table_name = r["table_name"]
        table_type = r["table_type"]  # BASE TABLE | VIEW | EXTERNAL
        ddl = r["ddl"] or ""
        # Write DDL
        subdir = "ddl"  # keep views here too; optional: split to views/
        ddl_path = out / subdir / f"{args.dataset}.{table_name}.sql"
        if ddl:
            write_text(ddl_path, ddl)

        # Write schema JSON for tables (views don't have data schema)
        if table_type.upper() in ("BASE TABLE", "EXTERNAL"):
            tbl_ref = f"{dataset_id}.{table_name}"
            tbl = client.get_table(tbl_ref)
            schema_json = {
                "table": tbl_ref,
                "type": table_type,
                "partitioning": getattr(tbl, "time_partitioning", None).type_ if getattr(tbl, "time_partitioning", None) else None,
                "partition_field": getattr(tbl, "time_partitioning", None).field if getattr(tbl, "time_partitioning", None) else None,
                "clustering_fields": getattr(tbl, "clustering_fields", None),
                "schema": to_schema_json(tbl.schema),
            }
            schema_path = out / "schema" / f"{args.dataset}.{table_name}.schema.json"
            write_json(schema_path, schema_json)

    # ---- Routines (UDFs / Procedures): dump SQL if any
    try:
        routines_sql = f"""
        SELECT routine_name, routine_type, routine_language, routine_definition
        FROM `{dataset_id}.INFORMATION_SCHEMA.ROUTINES`
        ORDER BY routine_name
        """
        for r in client.query(routines_sql).result():
            name = r["routine_name"]
            definition = r["routine_definition"] or ""
            if definition:
                write_text(out / "routines" / f"{args.dataset}.{name}.sql", definition)
    except Exception:
        pass  # dataset may have no routines, which is fine

    print(f"✅ Wrote DDL & schema to {out.resolve()}")

if __name__ == "__main__":
    main()
