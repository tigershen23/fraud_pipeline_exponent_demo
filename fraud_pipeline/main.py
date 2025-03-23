#!/usr/bin/env python3
"""
Main Orchestration Script for Financial Transaction Fraud Analytics Pipeline

This script orchestrates the entire pipeline:
1. Generate synthetic transaction data
2. Load the data into DuckDB
3. Perform risk analysis
4. Generate visualizations and reports

Usage:
    python main.py [--transactions NUM_TRANSACTIONS] [--fraud FRAUD_RATIO]

Options:
    --transactions NUM_TRANSACTIONS    Number of transactions to generate (default: 1000)
    --fraud FRAUD_RATIO                Ratio of fraudulent transactions (default: 0.05)
"""

import os
import sys
import argparse
from datetime import datetime
import prefect
from prefect import flow, task
from rich.console import Console
from rich.panel import Panel

# Import modules from our package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from fraud_pipeline.src.data_generation import generate_transaction_data, save_transaction_data
from fraud_pipeline.src.data_loading import load_data_to_duckdb
from fraud_pipeline.src.risk_analysis import run_risk_analysis
from fraud_pipeline.src.visualization import create_visualizations

# Set up console for rich output
console = Console()

@task(name="Generate Synthetic Transaction Data")
def task_generate_data(num_transactions=1000, fraud_ratio=0.05):
    """Generate synthetic transaction data."""
    console.print(Panel.fit("Step 1: Generating Synthetic Transaction Data", 
                           style="bold blue"))
    
    # Generate data
    transactions_df = generate_transaction_data(
        num_transactions=num_transactions, 
        fraud_ratio=fraud_ratio
    )
    
    # Save to CSV
    csv_path = "fraud_pipeline/data/transactions.csv"
    save_transaction_data(transactions_df, csv_path)
    
    return csv_path

@task(name="Load Data into DuckDB")
def task_load_data(csv_path):
    """Load transaction data into DuckDB."""
    console.print(Panel.fit("Step 2: Loading Data into DuckDB", 
                           style="bold blue"))
    
    db_path = "fraud_pipeline/data/transactions.duckdb"
    con = load_data_to_duckdb(csv_path, db_path)
    con.close()
    
    return db_path

@task(name="Perform Risk Analysis")
def task_risk_analysis(db_path):
    """Perform risk analysis on transaction data."""
    console.print(Panel.fit("Step 3: Performing Risk Analysis", 
                           style="bold blue"))
    
    run_risk_analysis(db_path)
    
    return db_path

@task(name="Generate Visualizations and Reports")
def task_create_visualizations(db_path):
    """Generate visualizations and reports."""
    console.print(Panel.fit("Step 4: Generating Visualizations and Reports", 
                           style="bold blue"))
    
    output_dir = "fraud_pipeline/output"
    create_visualizations(db_path, output_dir)
    
    return output_dir

@flow(name="Financial Transaction Fraud Analytics Pipeline")
def fraud_analytics_pipeline(num_transactions=1000, fraud_ratio=0.05):
    """Run the complete fraud analytics pipeline."""
    start_time = datetime.now()
    
    console.print(Panel.fit(
        f"[bold]Financial Transaction Fraud Analytics Pipeline[/bold]\n"
        f"Starting pipeline at {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Generating {num_transactions} transactions with {fraud_ratio:.1%} fraud ratio",
        style="bold green"
    ))
    
    # Ensure output directories exist
    os.makedirs("fraud_pipeline/data", exist_ok=True)
    os.makedirs("fraud_pipeline/output", exist_ok=True)
    
    # Execute pipeline steps
    csv_path = task_generate_data(num_transactions, fraud_ratio)
    db_path = task_load_data(csv_path)
    db_path = task_risk_analysis(db_path)
    output_dir = task_create_visualizations(db_path)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    console.print(Panel.fit(
        f"[bold]Pipeline Completed Successfully![/bold]\n"
        f"Finished at {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Total duration: {duration:.2f} seconds\n\n"
        f"Output files available at:\n"
        f"- CSV data: {csv_path}\n"
        f"- DuckDB database: {db_path}\n"
        f"- Reports and visualizations: {output_dir}",
        style="bold green"
    ))
    
    # Display links to output files
    console.print("\n[bold]Generated outputs:[/bold]")
    console.print(f"- Transaction Volume Chart: {os.path.join(output_dir, 'transaction_volume.png')}")
    console.print(f"- Risk Distribution Chart: {os.path.join(output_dir, 'risk_distribution.png')}")
    console.print(f"- Detection Results Chart: {os.path.join(output_dir, 'risk_distribution_detection.png')}")
    console.print(f"- Summary Report: {os.path.join(output_dir, 'risk_report.md')}")
    
    return {
        "csv_path": csv_path,
        "db_path": db_path,
        "output_dir": output_dir,
        "duration": duration
    }

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Financial Transaction Fraud Analytics Pipeline"
    )
    parser.add_argument(
        "--transactions", 
        type=int, 
        default=1000, 
        help="Number of transactions to generate (default: 1000)"
    )
    parser.add_argument(
        "--fraud", 
        type=float, 
        default=0.05, 
        help="Ratio of fraudulent transactions (default: 0.05)"
    )
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_arguments()
    
    # Run the pipeline
    fraud_analytics_pipeline(
        num_transactions=args.transactions,
        fraud_ratio=args.fraud
    )