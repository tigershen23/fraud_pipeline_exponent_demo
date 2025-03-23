"""
Visualization Module for Financial Transaction Fraud Analytics Pipeline

This module generates visualizations and reports based on the transaction data
and risk assessment results.
"""

import os
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

def create_transaction_volume_chart(con, output_path):
    """
    Create a line chart showing transaction volumes and average amounts over time.
    
    Args:
        con: DuckDB connection
        output_path: Path to save the chart image
    """
    console.print("[bold blue]Creating transaction volume chart...[/bold blue]")
    
    # Query to get daily transaction counts and average amounts
    daily_stats = con.execute("""
        SELECT 
            DATE_TRUNC('day', timestamp) AS date,
            COUNT(*) AS transaction_count,
            AVG(amount) AS avg_amount
        FROM transactions
        GROUP BY DATE_TRUNC('day', timestamp)
        ORDER BY date
    """).fetchall()
    
    # Extract data into lists
    dates = [row[0] for row in daily_stats]
    counts = [row[1] for row in daily_stats]
    avgs = [row[2] for row in daily_stats]
    
    # Create figure with two y-axes
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot transaction count on left y-axis
    color = 'tab:blue'
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Transaction Count', color=color)
    ax1.plot(dates, counts, color=color, marker='o', linestyle='-', label='Transaction Volume')
    ax1.tick_params(axis='y', labelcolor=color)
    
    # Create second y-axis for average amount
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Average Amount ($)', color=color)
    ax2.plot(dates, avgs, color=color, marker='x', linestyle='--', label='Average Amount')
    ax2.tick_params(axis='y', labelcolor=color)
    
    # Format x-axis date labels
    fig.autofmt_xdate()
    
    # Add title and grid
    plt.title('Daily Transaction Volume and Average Amount')
    ax1.grid(True, alpha=0.3)
    
    # Add legend
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save figure
    plt.tight_layout()
    plt.savefig(output_path)
    console.print(f"[bold green]Transaction volume chart saved to {output_path}[/bold green]")
    
    # Close figure to free memory
    plt.close(fig)

def create_risk_distribution_chart(con, output_path):
    """
    Create a bar chart showing the distribution of risk levels and flagged transactions.
    
    Args:
        con: DuckDB connection
        output_path: Path to save the chart image
    """
    console.print("[bold blue]Creating risk distribution chart...[/bold blue]")
    
    # Query to get transaction counts by risk level
    risk_counts = con.execute("""
        SELECT 
            risk_level,
            COUNT(*) AS count
        FROM risk_assessment
        GROUP BY risk_level
        ORDER BY 
            CASE 
                WHEN risk_level = 'HIGH' THEN 1
                WHEN risk_level = 'MEDIUM' THEN 2
                WHEN risk_level = 'LOW' THEN 3
            END
    """).fetchall()
    
    # Extract data into lists
    risk_levels = [row[0] for row in risk_counts]
    counts = [row[1] for row in risk_counts]
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 6))
    
    # Create bar chart
    bars = ax.bar(risk_levels, counts, color=['#FF5555', '#FFAA55', '#55AA55'])
    
    # Add data labels on top of each bar
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'{height}', ha='center', va='bottom')
    
    # Add title and labels
    ax.set_title('Transaction Count by Risk Level')
    ax.set_xlabel('Risk Level')
    ax.set_ylabel('Number of Transactions')
    
    # Add grid
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add a second subplot for the detection results
    # Query to get detection results
    detection_results = con.execute("""
        SELECT
            CASE 
                WHEN risk_level = 'HIGH' AND known_fraud = TRUE THEN 'True Positive'
                WHEN risk_level != 'HIGH' AND known_fraud = TRUE THEN 'False Negative'
                WHEN risk_level = 'HIGH' AND known_fraud = FALSE THEN 'False Positive'
                ELSE 'True Negative'
            END AS detection_result,
            COUNT(*) as count
        FROM risk_assessment
        GROUP BY detection_result
        ORDER BY detection_result
    """).fetchall()
    
    # Create a new figure for detection results
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    
    # Extract data
    detection_categories = [row[0] for row in detection_results]
    detection_counts = [row[1] for row in detection_results]
    
    # Define colors for different categories
    colors = {
        'True Positive': '#55AA55',  # Green
        'True Negative': '#5555AA',  # Blue
        'False Positive': '#FFAA55',  # Orange
        'False Negative': '#FF5555'   # Red
    }
    
    detection_colors = [colors.get(cat, '#AAAAAA') for cat in detection_categories]
    
    # Create bar chart
    bars2 = ax2.bar(detection_categories, detection_counts, color=detection_colors)
    
    # Add data labels
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height + 5,
                f'{height}', ha='center', va='bottom')
    
    # Add title and labels
    ax2.set_title('Fraud Detection Results')
    ax2.set_xlabel('Detection Category')
    ax2.set_ylabel('Number of Transactions')
    
    # Add grid
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save both figures
    plt.figure(fig.number)
    plt.tight_layout()
    plt.savefig(output_path)
    console.print(f"[bold green]Risk distribution chart saved to {output_path}[/bold green]")
    
    # Save the second figure
    detection_output = output_path.replace('.png', '_detection.png')
    plt.figure(fig2.number)
    plt.tight_layout()
    plt.savefig(detection_output)
    console.print(f"[bold green]Detection results chart saved to {detection_output}[/bold green]")
    
    # Close figures
    plt.close(fig)
    plt.close(fig2)

def generate_summary_report(con, output_path):
    """
    Generate a summary report of the risk assessment.
    
    Args:
        con: DuckDB connection
        output_path: Path to save the report
    """
    console.print("[bold blue]Generating summary report...[/bold blue]")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Get various statistics
    total_count = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    fraud_count = con.execute("SELECT COUNT(*) FROM transactions WHERE is_flagged = TRUE").fetchone()[0]
    
    risk_summary = con.execute("""
        SELECT 
            risk_level, 
            COUNT(*) as count,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM risk_assessment) as percentage
        FROM risk_assessment
        GROUP BY risk_level
        ORDER BY 
            CASE 
                WHEN risk_level = 'HIGH' THEN 1
                WHEN risk_level = 'MEDIUM' THEN 2
                WHEN risk_level = 'LOW' THEN 3
            END
    """).fetchall()
    
    detection_results = con.execute("""
        SELECT
            CASE 
                WHEN risk_level = 'HIGH' AND known_fraud = TRUE THEN 'True Positive'
                WHEN risk_level != 'HIGH' AND known_fraud = TRUE THEN 'False Negative'
                WHEN risk_level = 'HIGH' AND known_fraud = FALSE THEN 'False Positive'
                ELSE 'True Negative'
            END AS detection_result,
            COUNT(*) as count
        FROM risk_assessment
        GROUP BY detection_result
    """).fetchall()
    
    # Calculate some metrics
    detection_dict = {result[0]: result[1] for result in detection_results}
    true_positives = detection_dict.get('True Positive', 0)
    false_negatives = detection_dict.get('False Negative', 0)
    false_positives = detection_dict.get('False Positive', 0)
    true_negatives = detection_dict.get('True Negative', 0)
    
    precision = true_positives / (true_positives + false_positives) if (true_positives + false_positives) > 0 else 0
    recall = true_positives / (true_positives + false_negatives) if (true_positives + false_negatives) > 0 else 0
    f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
    
    # Write the report
    with open(output_path, 'w') as f:
        f.write("# Financial Transaction Risk & Fraud Analysis Report\n\n")
        f.write(f"Report generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Summary Statistics\n\n")
        f.write(f"* Total Transactions: {total_count}\n")
        f.write(f"* Known Fraudulent Transactions: {fraud_count} ({fraud_count/total_count*100:.2f}%)\n\n")
        
        f.write("## Risk Assessment Summary\n\n")
        f.write("| Risk Level | Count | Percentage |\n")
        f.write("|------------|-------|------------|\n")
        for level, count, percentage in risk_summary:
            f.write(f"| {level} | {count} | {percentage:.2f}% |\n")
        f.write("\n")
        
        f.write("## Fraud Detection Performance\n\n")
        f.write("| Detection Result | Count |\n")
        f.write("|------------------|-------|\n")
        for result, count in detection_results:
            f.write(f"| {result} | {count} |\n")
        f.write("\n")
        
        f.write("### Detection Metrics\n\n")
        f.write(f"* Precision: {precision:.2f} (ability to avoid false positives)\n")
        f.write(f"* Recall: {recall:.2f} (ability to find all fraudulent transactions)\n")
        f.write(f"* F1 Score: {f1_score:.2f} (balance between precision and recall)\n\n")
        
        f.write("## High Risk Transactions\n\n")
        high_risk = con.execute("""
            SELECT 
                transaction_id,
                timestamp,
                account_number,
                transaction_type,
                amount,
                risk_score,
                large_amount_flag,
                odd_hours_flag,
                suspicious_merchant_flag,
                rapid_tx_flag,
                known_fraud
            FROM risk_assessment
            WHERE risk_level = 'HIGH'
            ORDER BY risk_score DESC
            LIMIT 10
        """).fetchall()
        
        f.write("| Transaction ID | Timestamp | Account | Type | Amount | Risk Score | Risk Factors | Known Fraud |\n")
        f.write("|----------------|-----------|---------|------|--------|------------|--------------|-------------|\n")
        
        for row in high_risk:
            tx_id = row[0][:8] + "..."
            timestamp = row[1]
            account = row[2]
            tx_type = row[3]
            amount = f"${row[4]:.2f}"
            risk_score = str(row[5])
            
            # Compile risk factors
            risk_factors = []
            if row[6]: risk_factors.append("Large Amount")
            if row[7]: risk_factors.append("Odd Hours")
            if row[8]: risk_factors.append("Suspicious Merchant")
            if row[9]: risk_factors.append("Rapid Transactions")
            risk_factors_str = ", ".join(risk_factors)
            
            known_fraud = "Yes" if row[10] else "No"
            
            f.write(f"| {tx_id} | {timestamp} | {account} | {tx_type} | {amount} | {risk_score} | {risk_factors_str} | {known_fraud} |\n")
        
        f.write("\n## Conclusion\n\n")
        f.write("This report presents the results of a heuristic-based risk assessment on financial transaction data. ")
        f.write("The model uses threshold-based rules to identify potentially fraudulent transactions. ")
        
        if precision > 0.8:
            f.write("The model demonstrates high precision, suggesting it effectively avoids false alarms. ")
        else:
            f.write("The model's precision could be improved to reduce false alarms. ")
            
        if recall > 0.8:
            f.write("It also shows strong recall, capturing most fraudulent transactions. ")
        else:
            f.write("However, its recall is relatively low, indicating that many fraudulent transactions are not being caught. ")
        
        f.write("Further refinement of the risk rules and thresholds could improve overall detection performance.\n")
    
    console.print(f"[bold green]Summary report saved to {output_path}[/bold green]")

def create_visualizations(db_path, output_dir):
    """
    Create all visualizations and reports.
    
    Args:
        db_path: Path to the DuckDB database file
        output_dir: Directory to save visualizations and reports
    """
    console.print("[bold blue]Creating visualizations and reports...[/bold blue]")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    try:
        # Create transaction volume chart
        volume_chart_path = os.path.join(output_dir, "transaction_volume.png")
        create_transaction_volume_chart(con, volume_chart_path)
        
        # Create risk distribution chart
        risk_chart_path = os.path.join(output_dir, "risk_distribution.png")
        create_risk_distribution_chart(con, risk_chart_path)
        
        # Generate summary report
        report_path = os.path.join(output_dir, "risk_report.md")
        generate_summary_report(con, report_path)
        
        console.print("[bold green]All visualizations and reports created successfully![/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]Error creating visualizations: {e}[/bold red]")
        raise
    
    finally:
        # Close connection
        con.close()

if __name__ == "__main__":
    # Create visualizations
    db_path = "fraud_pipeline/data/transactions.duckdb"
    output_dir = "fraud_pipeline/output"
    
    create_visualizations(db_path, output_dir)