"""
Risk Analysis Module for Financial Transaction Fraud Analytics Pipeline

This module performs data transformation and risk analysis on the transaction data
using SQL queries in DuckDB.
"""

import duckdb
from rich.console import Console
from rich.table import Table

console = Console()

def perform_data_transformation(con):
    """
    Clean and enrich the transaction data.
    
    Args:
        con: DuckDB connection
    """
    console.print("[bold blue]Performing data transformation...[/bold blue]")
    
    # Create a view of the original data
    con.execute("CREATE OR REPLACE VIEW raw_transactions AS SELECT * FROM transactions")
    
    # Create a new table with transformed data
    con.execute("""
        CREATE OR REPLACE TABLE enriched_transactions AS
        SELECT 
            transaction_id,
            timestamp,
            EXTRACT(YEAR FROM timestamp) AS year,
            EXTRACT(MONTH FROM timestamp) AS month,
            EXTRACT(DAY FROM timestamp) AS day,
            EXTRACT(HOUR FROM timestamp) AS hour,
            EXTRACT(MINUTE FROM timestamp) AS minute,
            account_number,
            transaction_type,
            amount,
            -- Clean merchant code (replace NULL with 'N/A' for non-payment transactions)
            CASE 
                WHEN transaction_type = 'PAYMENT' AND merchant_code IS NULL THEN 'UNKNOWN'
                WHEN transaction_type != 'PAYMENT' THEN 'N/A'
                ELSE merchant_code 
            END AS merchant_code,
            -- Clean recipient account (replace NULL with 'N/A' for non-transfer transactions)
            CASE 
                WHEN transaction_type = 'TRANSFER' AND recipient_account IS NULL THEN 'UNKNOWN'
                WHEN transaction_type != 'TRANSFER' THEN 'N/A'
                ELSE recipient_account 
            END AS recipient_account,
            is_flagged AS known_fraud
        FROM raw_transactions
    """)
    
    # Verify transformation
    count = con.execute("SELECT COUNT(*) FROM enriched_transactions").fetchone()[0]
    console.print(f"[bold green]Transformed {count} transactions[/bold green]")
    
    # Show sample of transformed data
    console.print("[bold]Sample of transformed data:[/bold]")
    sample = con.execute("SELECT * FROM enriched_transactions LIMIT 3").fetchall()
    columns = [col[0] for col in con.execute("DESCRIBE enriched_transactions").fetchall()]
    
    # Create a rich table for better display
    table = Table(title="Transformed Data Sample")
    for column in columns:
        table.add_column(column)
    
    for row in sample:
        table.add_row(*[str(cell) for cell in row])
    
    console.print(table)

def apply_risk_rules(con):
    """
    Apply risk assessment rules to the transaction data.
    
    Args:
        con: DuckDB connection
    """
    console.print("[bold blue]Applying risk assessment rules...[/bold blue]")
    
    # Create risk assessment table
    con.execute("""
        CREATE OR REPLACE TABLE risk_assessment AS
        SELECT 
            t.transaction_id,
            t.timestamp,
            t.account_number,
            t.transaction_type,
            t.amount,
            t.merchant_code,
            t.recipient_account,
            
            -- Rule 1: Large Transaction Amount
            CASE 
                WHEN t.transaction_type = 'WITHDRAWAL' AND t.amount > 2000 THEN TRUE
                WHEN t.transaction_type = 'TRANSFER' AND t.amount > 3000 THEN TRUE
                WHEN t.transaction_type = 'PAYMENT' AND t.amount > 5000 THEN TRUE
                ELSE FALSE
            END AS large_amount_flag,
            
            -- Rule 2: Odd Hours Activity (midnight to 4 AM)
            CASE 
                WHEN t.hour >= 0 AND t.hour < 4 THEN TRUE
                ELSE FALSE
            END AS odd_hours_flag,
            
            -- Rule 3: Suspicious Merchant
            CASE 
                WHEN t.merchant_code LIKE 'SUSPICIOUS%' THEN TRUE
                ELSE FALSE
            END AS suspicious_merchant_flag,
            
            -- Known fraud (from original data, for validation)
            t.known_fraud
        FROM enriched_transactions t
    """)
    
    # Add transaction velocity analysis
    # First, create a window function to count transactions by account within a 1-hour window
    con.execute("""
        CREATE OR REPLACE TABLE transaction_velocity AS
        SELECT 
            t.transaction_id,
            t.account_number,
            t.timestamp,
            COUNT(*) OVER (
                PARTITION BY t.account_number 
                ORDER BY t.timestamp 
                RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
            ) AS tx_count_1h
        FROM enriched_transactions t
    """)
    
    # Add transaction velocity flag to risk assessment
    con.execute("""
        ALTER TABLE risk_assessment ADD COLUMN rapid_tx_flag BOOLEAN;
        
        UPDATE risk_assessment r
        SET rapid_tx_flag = (
            SELECT 
                CASE WHEN tv.tx_count_1h >= 3 THEN TRUE ELSE FALSE END
            FROM transaction_velocity tv
            WHERE tv.transaction_id = r.transaction_id
        );
    """)
    
    # Calculate overall risk score
    con.execute("""
        ALTER TABLE risk_assessment ADD COLUMN risk_score INTEGER;
        ALTER TABLE risk_assessment ADD COLUMN risk_level VARCHAR;
        
        UPDATE risk_assessment
        SET risk_score = (
            CASE WHEN large_amount_flag THEN 30 ELSE 0 END +
            CASE WHEN odd_hours_flag THEN 25 ELSE 0 END +
            CASE WHEN suspicious_merchant_flag THEN 40 ELSE 0 END +
            CASE WHEN rapid_tx_flag THEN 20 ELSE 0 END
        );
        
        UPDATE risk_assessment
        SET risk_level = CASE
            WHEN risk_score >= 50 THEN 'HIGH'
            WHEN risk_score >= 30 THEN 'MEDIUM'
            ELSE 'LOW'
        END;
    """)
    
    # Show risk assessment summary
    console.print("[bold]Risk Assessment Summary:[/bold]")
    
    # Count by risk level
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
    
    # Create a rich table for risk summary
    table = Table(title="Risk Level Summary")
    table.add_column("Risk Level")
    table.add_column("Count")
    table.add_column("Percentage")
    
    for row in risk_summary:
        table.add_row(row[0], str(row[1]), f"{row[2]:.2f}%")
    
    console.print(table)
    
    # Compare with known fraud
    fraud_detection = con.execute("""
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
    
    # Create a rich table for fraud detection results
    table = Table(title="Fraud Detection Results")
    table.add_column("Detection Result")
    table.add_column("Count")
    
    for row in fraud_detection:
        table.add_row(row[0], str(row[1]))
    
    console.print(table)
    
    # Show sample of high risk transactions
    high_risk_sample = con.execute("""
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
        LIMIT 5
    """).fetchall()
    
    # Create a rich table for high risk transactions
    table = Table(title="Sample High Risk Transactions")
    table.add_column("Transaction ID")
    table.add_column("Timestamp")
    table.add_column("Account")
    table.add_column("Type")
    table.add_column("Amount")
    table.add_column("Risk Score")
    table.add_column("Large Amount")
    table.add_column("Odd Hours")
    table.add_column("Suspicious Merchant")
    table.add_column("Rapid Transactions")
    table.add_column("Known Fraud")
    
    for row in high_risk_sample:
        table.add_row(
            str(row[0])[:8] + "...",  # Truncate transaction ID for display
            str(row[1]),
            str(row[2]),
            str(row[3]),
            f"${row[4]:.2f}",
            str(row[5]),
            "✓" if row[6] else "✗",
            "✓" if row[7] else "✗",
            "✓" if row[8] else "✗",
            "✓" if row[9] else "✗",
            "✓" if row[10] else "✗"
        )
    
    console.print(table)

def run_risk_analysis(db_path):
    """
    Run the complete risk analysis process.
    
    Args:
        db_path (str): Path to the DuckDB database file
    """
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    try:
        # Perform data transformation
        perform_data_transformation(con)
        
        # Apply risk rules
        apply_risk_rules(con)
        
        # Save the final tables into the database
        console.print("[bold green]Risk analysis completed successfully![/bold green]")
        
    except Exception as e:
        console.print(f"[bold red]Error during risk analysis: {e}[/bold red]")
        raise
    
    finally:
        # Close connection
        con.close()

if __name__ == "__main__":
    # Run risk analysis
    db_path = "fraud_pipeline/data/transactions.duckdb"
    run_risk_analysis(db_path)