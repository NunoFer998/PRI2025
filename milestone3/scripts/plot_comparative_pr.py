#!/usr/bin/env python3
"""
Generate comparative precision-recall curves for multiple search systems.
Runs trec_eval for each system and plots all curves on the same graph.
Supports both aggregated and per-query comparisons.
"""
import sys
import argparse
import subprocess
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Color palette for different systems
COLORS = ["#2ecc71", "#3498db", "#e74c3c", "#9b59b6", "#f39c12", "#1abc9c"]
MARKERS = ["o", "s", "^", "D", "v", "p"]


def get_trec_eval_results(
    trec_eval_bin: str, qrels_file: str, results_file: str
) -> dict:
    """Run trec_eval and parse the output."""
    cmd = [trec_eval_bin, "-q", "-m", "all_trec", qrels_file, results_file]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error running trec_eval: {result.stderr}", file=sys.stderr)
        return {}

    # Parse the output
    metrics = {}
    for line in result.stdout.strip().split("\n"):
        parts = line.split()
        if len(parts) >= 3:
            name, query_id, value = parts[0], parts[1], parts[2]
            if query_id not in metrics:
                metrics[query_id] = {}
            metrics[query_id][name] = value

    return metrics


def plot_system_curve(
    ax, query_metrics: dict, system_name: str, color: str, marker: str
):
    """Plot a single system's PR curve."""
    recall = np.arange(0, 1.1, 0.1)
    pr_keys = [f"iprec_at_recall_{k:.2f}" for k in recall]

    try:
        iprecision = np.array([float(query_metrics[k]) for k in pr_keys])
        ap_score = float(query_metrics.get("map", 0))
        auc_score = float(query_metrics.get("11pt_avg", 0))
        p_20 = float(query_metrics.get("P_20", 0))
    except (KeyError, ValueError) as e:
        print(
            f"Warning: Could not parse metrics for {system_name}: {e}", file=sys.stderr
        )
        return None

    (line,) = ax.plot(
        recall,
        iprecision,
        drawstyle="steps-post",
        label=f"{system_name}: MAP={ap_score:.3f}, AUC={auc_score:.3f}, P@20={p_20:.3f}",
        linewidth=2.5,
        color=color,
        marker=marker,
        markersize=6,
        markerfacecolor="white",
        markeredgewidth=2,
    )

    return line


def main():
    parser = argparse.ArgumentParser(
        description="Generate comparative PR curves for multiple systems"
    )
    parser.add_argument(
        "--trec-eval", default="trec_eval/trec_eval", help="Path to trec_eval binary"
    )
    parser.add_argument("--qrels", default="qrels_trec.txt", help="Path to qrels file")
    parser.add_argument(
        "--output", default="evaluation_plots/comparative_pr.png", help="Output file"
    )
    parser.add_argument(
        "--systems",
        nargs="+",
        required=True,
        help="System specifications as name:results_file pairs",
    )
    parser.add_argument(
        "--title", default="Comparative Precision-Recall Curves", help="Plot title"
    )
    parser.add_argument(
        "--query",
        type=str,
        default=None,
        help='Specific query ID to plot (e.g., "0001"). If not set, uses "all" aggregated.',
    )

    args = parser.parse_args()

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))

    query_key = args.query if args.query else "all"

    # Process each system
    for idx, system_spec in enumerate(args.systems):
        if ":" in system_spec:
            name, results_file = system_spec.split(":", 1)
        else:
            name = (
                os.path.basename(system_spec)
                .replace("results_", "")
                .replace(".txt", "")
            )
            results_file = system_spec

        if not os.path.exists(results_file):
            print(f"Warning: Results file not found: {results_file}", file=sys.stderr)
            continue

        metrics = get_trec_eval_results(args.trec_eval, args.qrels, results_file)

        # Try both padded and unpadded query IDs
        query_metrics = None
        if query_key in metrics:
            query_metrics = metrics[query_key]
        elif query_key.lstrip("0") in metrics:
            query_metrics = metrics[query_key.lstrip("0")]
        elif query_key.zfill(4) in metrics:
            query_metrics = metrics[query_key.zfill(4)]

        if query_metrics:
            color = COLORS[idx % len(COLORS)]
            marker = MARKERS[idx % len(MARKERS)]
            plot_system_curve(ax, query_metrics, name, color, marker)
            print(f"✓ Added {name}")
        else:
            available = [k for k in metrics.keys()]
            print(
                f"Warning: Query '{query_key}' not found for {name}. Available: {available}",
                file=sys.stderr,
            )

    # Customize plot
    ax.set_title(args.title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Recall", fontsize=12)
    ax.set_ylabel("Precision", fontsize=12)
    ax.set_xlim(-0.02, 1.02)
    ax.set_ylim(-0.02, 1.02)
    ax.legend(loc="lower left", fontsize=10, framealpha=0.9)
    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.7)

    plt.tight_layout()

    # Save
    os.makedirs(
        os.path.dirname(args.output) if os.path.dirname(args.output) else ".",
        exist_ok=True,
    )
    plt.savefig(args.output, dpi=150, bbox_inches="tight")
    print(f"✓ Comparative plot saved to: {args.output}")


if __name__ == "__main__":
    main()
