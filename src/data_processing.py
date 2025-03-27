"""
Data Processing Module

This module handles loading transaction data from CSV, preprocessing, 
and loading into DuckDB for analysis.
"""

import os
import polars as pl
import duckdb
from rich.console import Console
from rich.progress import Progress

console = Console()

class DataProcessor:
    """Class for processing transaction data and loading into DuckDB."""
    
    def __init__(self, db_path="database/transactions.duckdb"):
        """
        Initialize the DataProcessor.
        
        Args:
            db_path (str): Path to the DuckDB database file
        """
        self.db_path = db_path
        # Ensure the database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        # Initialize the connection
        self.conn = duckdb.connect(db_path)
    
    def load_data(self, csv_path):
        """
        Load transaction data from CSV file.
        
        Args:
            csv_path (str): Path to the CSV file
        
        Returns:
            pl.DataFrame: Loaded DataFrame
        """
        console.print(f"[bold cyan]Loading data from {csv_path}...[/bold cyan]")
        
        # Load data using Polars
        df = pl.read_csv(csv_path)
        
        # Show basic info
        console.print(f"[green]Loaded {len(df)} records[/green]")
        
        return df
    
    def clean_data(self, df):
        """
        Clean and preprocess transaction data.
        
        Args:
            df (pl.DataFrame): Input DataFrame
        
        Returns:
            pl.DataFrame: Cleaned DataFrame
        """
        console.print("[bold cyan]Cleaning transaction data...[/bold cyan]")
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Preprocessing...", total=5)
            
            # 1. Remove duplicates based on transaction_id
            initial_len = len(df)
            df = df.unique(subset=["transaction_id"])
            progress.update(task, advance=1)
            console.print(f"Removed {initial_len - len(df)} duplicate transactions")
            
            # 2. Convert timestamp to proper datetime if it's not already
            if df["timestamp"].dtype != pl.Datetime:
                df = df.with_columns([pl.col("timestamp").str.to_datetime()])
            progress.update(task, advance=1)
            
            # 3. Fill missing values for merchant fields
            df = df.with_columns([
                pl.when(pl.col("merchant_name").is_null())
                .then(pl.lit("N/A"))
                .otherwise(pl.col("merchant_name"))
                .alias("merchant_name"),
                
                pl.when(pl.col("merchant_category").is_null())
                .then(pl.lit("N/A"))
                .otherwise(pl.col("merchant_category"))
                .alias("merchant_category")
            ])
            progress.update(task, advance=1)
            
            # 4. Ensure all numeric fields are the right type
            df = df.with_columns([pl.col("amount").cast(pl.Float64)])
            progress.update(task, advance=1)
            
            # 5. Add a date column for easier aggregation
            df = df.with_columns([
                pl.col("timestamp").dt.date().alias("date")
            ])
            progress.update(task, advance=1)
        
        console.print("[green]Data cleaning completed[/green]")
        return df
    
    def enrich_data(self, df):
        """
        Enrich data with additional features for risk assessment.
        
        Args:
            df (pl.DataFrame): Input DataFrame
        
        Returns:
            pl.DataFrame: Enriched DataFrame
        """
        console.print("[bold cyan]Enriching data with additional features...[/bold cyan]")
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Feature engineering...", total=4)
            
            # 1. Add hour of day feature
            df = df.with_columns([
                pl.col("timestamp").dt.hour().alias("hour_of_day")
            ])
            progress.update(task, advance=1)
            
            # 2. Add day of week feature (0=Monday, 6=Sunday)
            df = df.with_columns([
                pl.col("timestamp").dt.weekday().alias("day_of_week")
            ])
            progress.update(task, advance=1)
            
            # 3. Calculate transaction recency (days from latest transaction)
            max_date = df["timestamp"].max()
            df = df.with_columns([
                ((max_date - pl.col("timestamp")).dt.total_days()).alias("recency_days")
            ])
            progress.update(task, advance=1)
            
            # 4. Add account transaction count
            account_counts = df.group_by("account_number").agg(
                pl.count().alias("account_transaction_count")
            )
            df = df.join(account_counts, on="account_number")
            progress.update(task, advance=1)
        
        console.print("[green]Data enrichment completed[/green]")
        return df
    
    def load_to_duckdb(self, df, table_name="transactions"):
        """
        Load DataFrame into DuckDB.
        
        Args:
            df (pl.DataFrame): DataFrame to load
            table_name (str): Table name in DuckDB
        
        Returns:
            bool: True if successful
        """
        console.print(f"[bold cyan]Loading data into DuckDB table '{table_name}'...[/bold cyan]")
        
        try:
            # Convert Polars DataFrame to Pandas for DuckDB compatibility
            pandas_df = df.to_pandas()
            
            # Create the table and load the data
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM pandas_df")
            
            # Verify the table was created and data was loaded
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            count = result[0] if result else 0
            
            console.print(f"[green]Successfully loaded {count} records into DuckDB table '{table_name}'[/green]")
            return True
            
        except Exception as e:
            console.print(f"[red]Error loading data into DuckDB: {str(e)}[/red]")
            return False
    
    def close(self):
        """Close the DuckDB connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            console.print("[green]DuckDB connection closed[/green]")

def process_transaction_data(csv_path="data/transactions.csv", db_path="database/transactions.duckdb"):
    """
    Process transaction data from CSV and load into DuckDB.
    
    Args:
        csv_path (str): Path to the input CSV file
        db_path (str): Path to the DuckDB database file
    
    Returns:
        bool: True if processing was successful
    """
    processor = DataProcessor(db_path)
    
    try:
        # Load data
        df = processor.load_data(csv_path)
        
        # Clean data
        df = processor.clean_data(df)
        
        # Enrich data
        df = processor.enrich_data(df)
        
        # Load to DuckDB
        success = processor.load_to_duckdb(df)
        
        # Close connection
        processor.close()
        
        return success
    
    except Exception as e:
        console.print(f"[red]Error processing transaction data: {str(e)}[/red]")
        processor.close()
        return False

if __name__ == "__main__":
    # Process data when run directly
    process_transaction_data()