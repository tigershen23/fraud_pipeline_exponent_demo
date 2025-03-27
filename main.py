#!/usr/bin/env python3
"""
Fraud Analytics Pipeline

This is the main script that orchestrates the entire fraud analytics pipeline,
from data generation to risk assessment and visualization.
"""

import os
import time
import argparse
from datetime import datetime
from prefect import flow, task
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

# Import our modules
from src.data_generation import generate_transaction_data
from src.data_processing import process_transaction_data
from src.risk_assessment import assess_transaction_risk
from src.visualization import generate_visualizations
from src.utils import (
    log_step, log_success, log_error, log_warning, 
    time_execution, ensure_dir, display_markdown,
    format_duration
)

console = Console()

# Define Prefect tasks for each step in the pipeline
@task
def generate_data_task(num_records, output_path):
    """Generate synthetic transaction data."""
    log_step("Step 1: Data Generation", "Generating synthetic financial transaction data")
    try:
        start_time = time.time()
        csv_path = generate_transaction_data(num_records=num_records, output_path=output_path)
        elapsed = time.time() - start_time
        log_success(f"Generated {num_records} transactions in {format_duration(elapsed)}")
        log_success(f"Data saved to {csv_path}")
        return csv_path
    except Exception as e:
        log_error("Failed to generate transaction data", e)
        raise

@task
def process_data_task(csv_path, db_path):
    """Process and load transaction data into DuckDB."""
    log_step("Step 2: Data Processing", "Processing transaction data and loading into DuckDB")
    try:
        start_time = time.time()
        success = process_transaction_data(csv_path=csv_path, db_path=db_path)
        elapsed = time.time() - start_time
        if success:
            log_success(f"Transaction data processed in {format_duration(elapsed)}")
            log_success(f"Data loaded into DuckDB at {db_path}")
        else:
            log_error("Data processing completed with errors")
        return success
    except Exception as e:
        log_error("Failed to process transaction data", e)
        raise

@task
def assess_risk_task(db_path, export_path):
    """Perform risk assessment on transaction data."""
    log_step("Step 3: Risk Assessment", "Analyzing transactions for potential fraud indicators")
    try:
        start_time = time.time()
        success = assess_transaction_risk(db_path=db_path, export_path=export_path)
        elapsed = time.time() - start_time
        if success:
            log_success(f"Risk assessment completed in {format_duration(elapsed)}")
            log_success(f"Risk assessment results saved to {export_path}")
        else:
            log_error("Risk assessment completed with errors")
        return success
    except Exception as e:
        log_error("Failed to perform risk assessment", e)
        raise

@task
def generate_visualizations_task(db_path, output_dir):
    """Generate visualizations from transaction and risk data."""
    log_step("Step 4: Visualization", "Generating interactive visualizations and dashboards")
    try:
        start_time = time.time()
        viz_paths = generate_visualizations(db_path=db_path, output_dir=output_dir)
        elapsed = time.time() - start_time
        if viz_paths:
            log_success(f"Generated {len(viz_paths)} visualizations in {format_duration(elapsed)}")
            for path in viz_paths:
                log_success(f"Visualization saved to {path}")
        else:
            log_error("Visualization generation completed with errors")
        return viz_paths
    except Exception as e:
        log_error("Failed to generate visualizations", e)
        raise

@task
def generate_report_task(output_dir):
    """Generate a summary report."""
    log_step("Step 5: Report Generation", "Creating a summary report")
    try:
        # Create a basic report
        report_content = f"""# Fraud Analytics Pipeline Report

Generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## Summary

The fraud analytics pipeline has successfully completed all steps:

1. Data Generation: Synthetic transaction data was generated
2. Data Processing: Data was cleaned, transformed, and loaded into DuckDB
3. Risk Assessment: Transactions were analyzed and risk scores were assigned
4. Visualization: Interactive dashboards were created

## Visualizations

The following visualizations are available in the `{output_dir}` directory:

- Transaction Time Series: Shows transaction volumes and average amounts over time
- Risk Distribution: Shows the distribution of transactions by risk level
- Transaction Heatmap: Shows transaction activity by hour and day of week
- Amount Distribution: Shows transaction amount distribution by risk level

## Next Steps

1. Review the high-risk transactions in the DuckDB database
2. Explore the interactive visualizations
3. Investigate patterns and anomalies in the data
"""
        
        # Save the report
        report_path = os.path.join(output_dir, "fraud_analytics_report.md")
        with open(report_path, "w") as f:
            f.write(report_content)
        
        # Display the report in the console
        display_markdown(report_content)
        
        log_success(f"Report generated and saved to {report_path}")
        return report_path
    except Exception as e:
        log_error("Failed to generate report", e)
        raise

# Define the main Prefect flow
@flow(name="Fraud Analytics Pipeline")
def fraud_analytics_pipeline(
    num_records=1000,
    csv_path="data/transactions.csv",
    db_path="database/transactions.duckdb",
    export_path="data/risk_assessment_results.csv",
    output_dir="outputs",
    steps=None
):
    """
    Execute the fraud analytics pipeline.
    
    Args:
        num_records (int): Number of synthetic transactions to generate
        csv_path (str): Path to save/load transaction CSV
        db_path (str): Path to DuckDB database
        export_path (str): Path to export risk assessment results
        output_dir (str): Directory to save visualizations and reports
        steps (list): Specific steps to run, or None for all steps
    """
    # Create necessary directories
    ensure_dir(os.path.dirname(csv_path))
    ensure_dir(os.path.dirname(db_path))
    ensure_dir(output_dir)
    
    # Display pipeline banner
    console.print(Panel.fit(
        "[bold blue]FRAUD ANALYTICS PIPELINE[/bold blue]\n\n"
        "[cyan]A Comprehensive Solution for Financial Transaction Risk Assessment[/cyan]",
        title="Pipeline Started", 
        subtitle=f"Configuration: {num_records} transactions"
    ))
    
    # Set default steps if not specified
    all_steps = ["generate", "process", "assess", "visualize", "report"]
    if steps is None:
        steps = all_steps
    steps = [s.lower() for s in steps]
    
    # Track overall pipeline timing
    pipeline_start = time.time()
    
    # Execute each step if included
    data_generated = False
    data_processed = False
    risk_assessed = False
    
    if "generate" in steps:
        csv_path = generate_data_task(num_records, csv_path)
        data_generated = True
    else:
        log_warning(f"Skipping data generation, using existing data at {csv_path}")
        # Check if the CSV file exists
        if not os.path.exists(csv_path):
            log_error(f"CSV file {csv_path} not found. Cannot proceed with data processing.")
            data_generated = False
        else:
            data_generated = True
    
    if "process" in steps and data_generated:
        process_success = process_data_task(csv_path, db_path)
        if not process_success:
            log_error("Data processing failed. Skipping dependent steps.")
            data_processed = False
        else:
            data_processed = True
    elif "process" not in steps:
        log_warning(f"Skipping data processing, using existing database at {db_path}")
        # Check if the database file exists
        if not os.path.exists(db_path):
            log_error(f"Database file {db_path} not found. Cannot proceed with risk assessment.")
            data_processed = False
        else:
            data_processed = True
    
    if "assess" in steps and data_processed:
        assess_success = assess_risk_task(db_path, export_path)
        if not assess_success:
            log_error("Risk assessment failed. Skipping dependent steps.")
            risk_assessed = False
        else:
            risk_assessed = True
    elif "assess" not in steps:
        log_warning("Skipping risk assessment, using existing risk assessment results")
        risk_assessed = True  # Allow visualizations to run even if we skip risk assessment
    
    if "visualize" in steps and data_processed:  # Visualization only depends on data processing, not risk assessment
        viz_paths = generate_visualizations_task(db_path, output_dir)
    else:
        log_warning("Skipping visualization generation")
    
    if "report" in steps:
        report_path = generate_report_task(output_dir)
    else:
        log_warning("Skipping report generation")
    
    # Calculate and display overall pipeline time
    pipeline_elapsed = time.time() - pipeline_start
    
    # Display completion message
    console.print(Panel.fit(
        f"[bold green]Pipeline completed in {format_duration(pipeline_elapsed)}![/bold green]\n\n"
        f"Review the results in the following locations:\n"
        f"- Raw Data: {csv_path}\n"
        f"- Database: {db_path}\n"
        f"- Risk Results: {export_path}\n"
        f"- Visualizations: {output_dir}\n\n"
        f"To explore the data further, you can connect to the DuckDB database "
        f"or open the HTML visualizations in a web browser.",
        title="Pipeline Complete"
    ))

def main():
    """Parse command line arguments and run the pipeline."""
    parser = argparse.ArgumentParser(description="Fraud Analytics Pipeline")
    parser.add_argument(
        "--num-records", 
        type=int, 
        default=1000,
        help="Number of synthetic transactions to generate (default: 1000)"
    )
    parser.add_argument(
        "--csv-path", 
        type=str, 
        default="data/transactions.csv",
        help="Path to save/load transaction CSV (default: data/transactions.csv)"
    )
    parser.add_argument(
        "--db-path", 
        type=str, 
        default="database/transactions.duckdb",
        help="Path to DuckDB database (default: database/transactions.duckdb)"
    )
    parser.add_argument(
        "--export-path", 
        type=str, 
        default="data/risk_assessment_results.csv",
        help="Path to export risk assessment results (default: data/risk_assessment_results.csv)"
    )
    parser.add_argument(
        "--output-dir", 
        type=str, 
        default="outputs",
        help="Directory to save visualizations and reports (default: outputs)"
    )
    parser.add_argument(
        "--steps", 
        type=str, 
        nargs="+",
        choices=["generate", "process", "assess", "visualize", "report", "all"],
        default=["all"],
        help="Specific steps to run (default: all)"
    )
    
    args = parser.parse_args()
    
    # Handle "all" steps
    steps = args.steps
    if "all" in steps:
        steps = None
    
    # Run the pipeline
    fraud_analytics_pipeline(
        num_records=args.num_records,
        csv_path=args.csv_path,
        db_path=args.db_path,
        export_path=args.export_path,
        output_dir=args.output_dir,
        steps=steps
    )

if __name__ == "__main__":
    main()