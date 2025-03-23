"""
Data Loading Module for Financial Transaction Fraud Analytics Pipeline

This module handles loading the transaction data into a DuckDB database.
"""

import os
import duckdb
from rich.console import Console

console = Console()

def load_data_to_duckdb(csv_path, db_path):
    """
    Load transaction data from a CSV file into a DuckDB database.
    
    Args:
        csv_path (str): Path to the CSV file containing transaction data
        db_path (str): Path to save the DuckDB database file
    
    Returns:
        duckdb.DuckDBPyConnection: Connection to the DuckDB database
    """
    console.print("[bold blue]Loading transaction data into DuckDB...[/bold blue]")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to DuckDB
    con = duckdb.connect(db_path)
    
    try:
        # Create table and load data
        con.execute("""
            CREATE TABLE IF NOT EXISTS transactions AS
            SELECT * FROM read_csv_auto(?)
        """, [csv_path])
        
        # Count records to verify
        result = con.execute("SELECT COUNT(*) AS count FROM transactions").fetchone()
        count = result[0]
        
        console.print(f"[bold green]Successfully loaded {count} transactions into DuckDB[/bold green]")
        
        # Display database schema
        console.print("[bold]Database schema:[/bold]")
        schema = con.execute("DESCRIBE transactions").fetchall()
        for column in schema:
            console.print(f"  - {column[0]}: {column[1]}")
        
        # Sample query to show data - using fetchall instead of fetchdf
        console.print("[bold]Sample data from database:[/bold]")
        sample = con.execute("SELECT * FROM transactions LIMIT 5").fetchall()
        # Print column names
        columns = [col[0] for col in con.execute("DESCRIBE transactions").fetchall()]
        console.print(", ".join(columns))
        # Print sample data rows
        for row in sample:
            console.print(row)
        
        return con
    
    except Exception as e:
        console.print(f"[bold red]Error loading data into DuckDB: {e}[/bold red]")
        raise

if __name__ == "__main__":
    # Load data into DuckDB
    csv_path = "fraud_pipeline/data/transactions.csv"
    db_path = "fraud_pipeline/data/transactions.duckdb"
    
    if not os.path.exists(csv_path):
        console.print(f"[bold red]Error: CSV file not found at {csv_path}[/bold red]")
        console.print("Please run data_generation.py first to create the transaction data.")
        exit(1)
    
    con = load_data_to_duckdb(csv_path, db_path)
    
    # Additional verification queries
    fraud_count = con.execute("SELECT COUNT(*) FROM transactions WHERE is_flagged = TRUE").fetchone()[0]
    console.print(f"[bold]Number of flagged fraudulent transactions: {fraud_count}[/bold]")
    
    # Close connection
    con.close()