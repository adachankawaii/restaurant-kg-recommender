from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
RGCN_MAIN = REPO_ROOT / "src" / "rgcn_pipeline" / "main.py"
DEFAULT_NESTED_SCENARIOS = REPO_ROOT / "rgcn_pipeline" / "user_scenarios_70_befood_bachkhoa.csv"
DEFAULT_PHASE2_SCENARIOS = REPO_ROOT / "rgcn_pipeline" / "data" / "user_scenarios_phase2_top5.csv"

sys.path.insert(0, str(REPO_ROOT / "src" / "scripts"))
from build_rgcn_training_from_scenarios import OUTPUT_FIELDS, rows_from_nested, rows_from_phase2  # noqa: E402

sys.path.insert(0, str(REPO_ROOT / "src" / "rgcn_pipeline"))
from src.data_loader import DEFAULT_EXCLUDED_RELATIONS, export_graphrag_from_neo4j  # noqa: E402


VARIANTS: dict[str, list[str]] = {
    "full": [],
    "phase2_repr_only": ["--query-mode", "phase2-representation"],
    "random_neg": ["--query-hard-negative-ratio", "0", "--query-model-hard-negative-ratio", "0"],
    "score_based": ["--query-mode", "score-based"],
}


STORE_METADATA_FIELDS = [
    "store_node_id",
    "store_id",
    "name",
    "latitude",
    "longitude",
    "median_price",
    "price_min",
    "price_max",
    "rating",
    "review_count",
    "district",
    "city",
    "area_id",
]


def pick(props: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = props.get(key)
        if value not in (None, ""):
            return value
    return ""


def as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def export_store_metadata_from_neo4j(uri: str, user: str, password: str, output_path: Path) -> Path:
    try:
        from neo4j import GraphDatabase
    except ImportError as exc:
        raise ImportError("Install neo4j first: pip install neo4j") from exc

    output_path.parent.mkdir(parents=True, exist_ok=True)
    driver = GraphDatabase.driver(uri, auth=(user, password))
    try:
        with driver.session() as session:
            rows = session.run(
                """
                MATCH (r:Restaurant)
                RETURN properties(r) AS props
                ORDER BY coalesce(r.store_key, r.restaurant_id, r.id, r.name)
                """
            )
            out_rows: list[dict[str, str]] = []
            for row in rows:
                props = dict(row["props"] or {})
                store_id = pick(props, "store_key", "restaurant_id", "store_id", "id")
                if not store_id:
                    continue
                price_min = pick(props, "price_min", "menu_price_min", "min_price")
                price_max = pick(props, "price_max", "menu_price_max", "max_price")
                median_price = pick(props, "median_price", "price_median", "menu_price_median", "avg_price")
                out_rows.append(
                    {
                        "store_node_id": f"store:{store_id}",
                        "store_id": as_text(store_id),
                        "name": as_text(pick(props, "name", "restaurant_name", "title")),
                        "latitude": as_text(pick(props, "latitude", "lat")),
                        "longitude": as_text(pick(props, "longitude", "lng", "lon")),
                        "median_price": as_text(median_price),
                        "price_min": as_text(price_min),
                        "price_max": as_text(price_max),
                        "rating": as_text(pick(props, "rating", "avg_rating")),
                        "review_count": as_text(pick(props, "review_count", "reviews_count", "num_reviews")),
                        "district": as_text(pick(props, "district")),
                        "city": as_text(pick(props, "city")),
                        "area_id": as_text(pick(props, "area_id")),
                    }
                )
    finally:
        driver.close()

    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=STORE_METADATA_FIELDS)
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"Exported store metadata: {output_path} ({len(out_rows)} rows)")
    return output_path


def merge_scenarios(nested_path: Path, phase2_path: Path, output_path: Path) -> Path:
    merged = rows_from_phase2(phase2_path) + rows_from_nested(nested_path)
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for row in merged:
        key = (row.get("query_node_id", ""), row.get("store_node_id", ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append({field: row.get(field, "") for field in OUTPUT_FIELDS})

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(deduped)
    print(
        "Merged scenarios: "
        f"{output_path} ({len(deduped)} rows, {len({row['query_node_id'] for row in deduped})} queries)"
    )
    return output_path


def parse_pipeline_metrics(output_dir: Path, topk: int) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    logs_path = output_dir / "logs.txt"
    pipeline_logs_path = output_dir / "pipeline_logs.txt"

    if logs_path.exists():
        text = logs_path.read_text(encoding="utf-8", errors="replace")
        acc = re.search(r"Test Acc:\s*([0-9.]+)", text)
        auc = re.search(r"Test AUC:\s*([0-9.]+|nan)", text)
        if acc:
            metrics["stage1_test_acc"] = float(acc.group(1))
        if auc and auc.group(1) != "nan":
            metrics["stage1_test_auc"] = float(auc.group(1))

    if pipeline_logs_path.exists():
        text = pipeline_logs_path.read_text(encoding="utf-8", errors="replace")
        pattern = (
            rf"(?:Stage2 Final nDCG@{topk}|nDCG@{topk}):\s*([0-9.]+),\s*"
            rf"MRR@{topk}:\s*([0-9.]+),\s*HR@{topk}:\s*([0-9.]+)"
        )
        matches = re.findall(pattern, text)
        match = matches[-1] if matches else None
        if match:
            metrics[f"ndcg@{topk}"] = float(match[0])
            metrics[f"mrr@{topk}"] = float(match[1])
            metrics[f"hr@{topk}"] = float(match[2])
    return metrics


def run_variant(
    name: str,
    variant_args: list[str],
    args: argparse.Namespace,
    *,
    nodes_path: Path,
    edges_path: Path,
    store_metadata_path: Path,
    scenarios_path: Path,
) -> dict[str, Any]:
    output_dir = args.output_dir / "runs" / name
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(RGCN_MAIN),
        "--nodes",
        str(nodes_path),
        "--edges",
        str(edges_path),
        "--store-metadata",
        str(store_metadata_path),
        "--queries",
        str(scenarios_path),
        "--outputs",
        str(output_dir),
        "--epochs",
        str(args.epochs),
        "--query-epochs",
        str(args.query_epochs),
        "--online-ratio",
        str(args.online_ratio),
        "--online-epochs",
        str(args.online_epochs),
        "--query-eval-ratio",
        str(args.query_eval_ratio),
        "--topk",
        str(args.topk),
        "--seed",
        str(args.seed),
        "--patience",
        str(args.patience),
        "--batch-size",
        str(args.batch_size),
        *variant_args,
    ]
    if args.add_reverse_edges:
        command.append("--add-reverse-edges")

    console_log = output_dir / "console.log"
    print(f"\n=== Ablation variant: {name} ===")
    print(" ".join(command))
    if args.dry_run:
        return {"variant": name, "status": "dry_run", "output_dir": str(output_dir), "command": command}

    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
    with console_log.open("w", encoding="utf-8") as log_file:
        result = subprocess.run(command, cwd=REPO_ROOT, env=env, stdout=log_file, stderr=subprocess.STDOUT)

    row: dict[str, Any] = {
        "variant": name,
        "status": "ok" if result.returncode == 0 else "failed",
        "returncode": result.returncode,
        "output_dir": str(output_dir),
    }
    row.update(parse_pipeline_metrics(output_dir, args.topk))
    if result.returncode != 0:
        row["console_log"] = str(console_log)
    return row


def write_summary(rows: list[dict[str, Any]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "summary.json"
    csv_path = output_dir / "summary.csv"
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    metric_keys = sorted(
        {
            key
            for row in rows
            for key in row.keys()
            if re.match(r"^(ndcg|mrr|hr)@\d+$", key)
        },
        key=lambda key: (key.split("@", 1)[1], {"ndcg": 0, "mrr": 1, "hr": 2}[key.split("@", 1)[0]]),
    )
    fieldnames = ["variant", *metric_keys]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
    print(f"\nWrote summary: {csv_path}")
    print(f"Wrote summary: {json_path}")


def parse_args() -> argparse.Namespace:
    load_dotenv(REPO_ROOT / ".env", override=False)
    load_dotenv(REPO_ROOT / ".env.graphrag", override=False)
    load_dotenv(REPO_ROOT / "production" / ".env", override=False)

    parser = argparse.ArgumentParser(description="Run R-GCN ablations on a Neo4j-exported KG snapshot.")
    parser.add_argument("--neo4j-uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.getenv("NEO4J_USER", os.getenv("NEO4J_USERNAME", "neo4j")))
    parser.add_argument("--neo4j-password", default=os.getenv("NEO4J_PASSWORD", "password"))
    parser.add_argument("--nested-scenarios", type=Path, default=DEFAULT_NESTED_SCENARIOS)
    parser.add_argument("--phase2-scenarios", type=Path, default=DEFAULT_PHASE2_SCENARIOS)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "artifacts" / "rgcn_ablation" / datetime.now().strftime("%Y%m%d_%H%M%S"),
    )
    parser.add_argument("--variants", default=",".join(VARIANTS.keys()))
    parser.add_argument("--skip-export", action="store_true", help="Reuse graph/metadata/scenario files in output-dir.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--epochs", type=int, default=60)
    parser.add_argument("--query-epochs", type=int, default=30)
    parser.add_argument("--online-ratio", type=float, default=0.0)
    parser.add_argument("--online-epochs", type=int, default=5)
    parser.add_argument("--query-eval-ratio", type=float, default=0.2)
    parser.add_argument("--topk", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--patience", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=4096)
    parser.add_argument("--add-reverse-edges", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    graph_dir = args.output_dir / "graph"
    nodes_path = graph_dir / "nodes.csv"
    edges_path = graph_dir / "edges.csv"
    store_metadata_path = graph_dir / "store_metadata.csv"
    scenarios_path = args.output_dir / "merged_scenarios.csv"

    args.output_dir.mkdir(parents=True, exist_ok=True)
    if not args.skip_export:
        export_graphrag_from_neo4j(
            args.neo4j_uri,
            args.neo4j_user,
            args.neo4j_password,
            nodes_path,
            edges_path,
            exclude_relations=DEFAULT_EXCLUDED_RELATIONS,
        )
        print(f"Exported graph: {nodes_path}")
        print(f"Exported graph: {edges_path}")
        export_store_metadata_from_neo4j(args.neo4j_uri, args.neo4j_user, args.neo4j_password, store_metadata_path)
        merge_scenarios(args.nested_scenarios, args.phase2_scenarios, scenarios_path)

    selected = [name.strip() for name in args.variants.split(",") if name.strip()]
    unknown = [name for name in selected if name not in VARIANTS]
    if unknown:
        raise ValueError(f"Unknown variants: {', '.join(unknown)}. Allowed: {', '.join(VARIANTS)}")

    rows = [
        run_variant(
            name,
            VARIANTS[name],
            args,
            nodes_path=nodes_path,
            edges_path=edges_path,
            store_metadata_path=store_metadata_path,
            scenarios_path=scenarios_path,
        )
        for name in selected
    ]
    write_summary(rows, args.output_dir)
    failed = [row["variant"] for row in rows if row.get("status") == "failed"]
    if failed:
        print(f"Failed variants: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
