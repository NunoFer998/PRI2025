#!/usr/bin/env python3
"""
Create a comparative plot of all 3 systems (basic, enhanced, treatment_sorting).
"""
import sys
import argparse
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np


def parse_trec_eval_file(filepath):
    """Parse TREC eval output file and return structured results."""
    results = {}
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split()
            if len(parts) < 3:
                continue
            
            metric_name = parts[0]
            query_id = parts[1]
            value = parts[2]
            
            try:
                value = float(value)
            except ValueError:
                value = str(value)
            
            if query_id not in results:
                results[query_id] = {}
            
            results[query_id][metric_name] = value
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Create comparative PR curve plot for all systems"
    )
    parser.add_argument('--basic', default='eval_results_basic.txt',
                       help='Path to basic system eval results')
    parser.add_argument('--enhanced', default='eval_results_enhanced.txt',
                       help='Path to enhanced system eval results')
    parser.add_argument('--treatment', default='eval_results_treatment_sorting.txt',
                       help='Path to treatment_sorting system eval results')
    parser.add_argument('--output', default='evaluation_plots/comparative_all_systems_pr.png',
                       help='Output file path')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = os.path.dirname(args.output)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    
    # Parse all evaluation files
    systems = {
        'basic': parse_trec_eval_file(args.basic),
        'enhanced': parse_trec_eval_file(args.enhanced),
        'treatment_sorting': parse_trec_eval_file(args.treatment)
    }
    
    # Get all query IDs (excluding 'all')
    all_query_ids = set()
    for system_data in systems.values():
        all_query_ids.update(system_data.keys())
    all_query_ids.discard('all')
    all_query_ids = sorted(list(all_query_ids), key=lambda x: int(x) if x.isdigit() else x)
    
    # Create figure with subplots for each query
    n_queries = len(all_query_ids)
    n_cols = 3
    n_rows = (n_queries + n_cols - 1) // n_cols
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 4 * n_rows))
    if n_rows == 1 and n_cols == 1:
        axes = [[axes]]
    elif n_rows == 1 or n_cols == 1:
        axes = axes.reshape(-1, 1) if n_cols == 1 else axes.reshape(1, -1)
    
    axes = np.array(axes)
    if axes.ndim == 1:
        axes = axes.reshape(-1, 1) if n_cols == 1 else axes.reshape(1, -1)
    
    colors = {'basic': '#1f77b4', 'enhanced': '#ff7f0e', 'treatment_sorting': '#2ca02c'}
    
    for idx, query_id in enumerate(all_query_ids):
        row = idx // n_cols
        col = idx % n_cols
        ax = axes[row, col]
        
        for system_name, system_data in systems.items():
            if query_id not in system_data:
                continue
            
            metrics = system_data[query_id]
            
            # Extract precision-recall points
            recall = np.arange(0, 1.1, 0.1)
            pr_keys = [f"iprec_at_recall_{k:.2f}" for k in recall]
            
            try:
                iprecision = np.array([float(metrics.get(k, 0)) for k in pr_keys])
            except (ValueError, TypeError):
                continue
            
            # Get additional metrics
            ap_score = float(metrics.get("map", 0))
            p_10 = float(metrics.get("P_20", 0))
            
            label = f"{system_name}: AP={ap_score:.3f}, P@20={p_10:.3f}"
            ax.plot(recall, iprecision, marker='o', linewidth=2, markersize=5,
                   label=label, color=colors[system_name], drawstyle='steps-post')
        
        ax.set_xlabel('Recall', fontsize=10)
        ax.set_ylabel('Precision', fontsize=10)
        ax.set_title(f'Query {query_id}', fontsize=11, fontweight='bold')
        ax.legend(loc='best', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.set_xlim(-0.05, 1.05)
        ax.set_ylim(-0.05, 1.05)
    
    # Hide unused subplots
    for idx in range(n_queries, n_rows * n_cols):
        row = idx // n_cols
        col = idx % n_cols
        axes[row, col].axis('off')
    
    plt.suptitle('Precision-Recall Curves - All Systems Comparison', 
                 fontsize=14, fontweight='bold', y=0.995)
    plt.tight_layout()
    
    plt.savefig(args.output, dpi=150, bbox_inches='tight')
    print(f"✓ Plot saved to: {args.output}")
    plt.close()


if __name__ == '__main__':
    main()
