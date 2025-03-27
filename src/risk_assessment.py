"""
Risk Assessment Module

This module handles risk assessment of financial transactions using 
heuristic rule-based approaches to identify potentially fraudulent activities.
"""

import os
import duckdb
import polars as pl
from rich.console import Console
from rich.table import Table

console = Console()

class RiskAssessor:
    """Class for assessing transaction risk using heuristic rules."""
    
    def __init__(self, db_path="database/transactions.duckdb"):
        """
        Initialize the RiskAssessor.
        
        Args:
            db_path (str): Path to the DuckDB database file
        """
        self.db_path = db_path
        # Initialize the connection
        self.conn = duckdb.connect(db_path)
    
    def create_risk_tables(self):
        """Create tables for risk assessment results."""
        console.print("[bold cyan]Setting up risk assessment tables...[/bold cyan]")
        
        # Create risk_scores table if it doesn't exist
        self.conn.execute("""
            DROP TABLE IF EXISTS risk_scores;
            CREATE TABLE risk_scores AS
            SELECT 
                transaction_id, 
                account_number,
                timestamp,
                transaction_type,
                amount,
                merchant_name,
                merchant_category,
                is_fraud,
                0.0 AS risk_score,
                'Low' AS risk_level
            FROM transactions;
        """)
        
        # Create risk rules table
        self.conn.execute("""
            DROP TABLE IF EXISTS risk_rules;
            CREATE TABLE risk_rules (
                rule_id INTEGER,
                rule_name VARCHAR,
                rule_description VARCHAR,
                risk_weight DOUBLE
            );
            
            INSERT INTO risk_rules VALUES
                (1, 'high_amount', 'Unusually high transaction amount', 0.6),
                (2, 'odd_hours', 'Transaction during unusual hours (1AM-5AM)', 0.4),
                (3, 'high_frequency', 'Multiple transactions in short time period', 0.5),
                (4, 'unusual_merchant', 'Transaction with unusual merchant category', 0.3),
                (5, 'account_velocity', 'Sudden increase in account activity', 0.4);
        """)
        
        console.print("[green]Risk tables created successfully[/green]")
    
    def apply_risk_rules(self):
        """Apply risk rules to transactions and calculate risk scores."""
        console.print("[bold cyan]Applying risk assessment rules...[/bold cyan]")
        
        # Apply Rule 1: High Amount Rule - Use alternative approach for high amounts
        self.conn.execute("""
            -- First calculate the average and standard deviation of amounts by transaction type
            CREATE TEMP TABLE amount_stats AS
            SELECT 
                transaction_type,
                AVG(amount) as avg_amount,
                STDDEV(amount) as stddev_amount
            FROM transactions
            GROUP BY transaction_type;
            
            -- Update risk scores for transactions with amount > avg + 2*stddev
            UPDATE risk_scores
            SET risk_score = risk_score + (
                SELECT risk_weight FROM risk_rules WHERE rule_id = 1
            )
            FROM amount_stats
            WHERE risk_scores.transaction_type = amount_stats.transaction_type
              AND risk_scores.amount > amount_stats.avg_amount + 2 * amount_stats.stddev_amount;
        """)
        console.print("[green]Rule 1 (high amount) applied[/green]")
        
        # Apply Rule 2: Odd Hours Rule
        self.conn.execute("""
            UPDATE risk_scores
            SET risk_score = risk_score + (
                SELECT risk_weight FROM risk_rules WHERE rule_id = 2
            )
            WHERE EXTRACT(hour FROM timestamp) >= 1 
              AND EXTRACT(hour FROM timestamp) <= 5;
        """)
        console.print("[green]Rule 2 (odd hours) applied[/green]")
        
        # Apply Rule 3: High Frequency Rule
        # Identify accounts with multiple transactions in a short period
        self.conn.execute("""
            CREATE OR REPLACE TEMP TABLE high_frequency_accounts AS
            SELECT account_number
            FROM (
                SELECT 
                    account_number,
                    timestamp,
                    lag(timestamp) OVER (PARTITION BY account_number ORDER BY timestamp) as prev_timestamp
                FROM transactions
            ) t
            WHERE prev_timestamp IS NOT NULL
              AND DATEDIFF('MINUTE', prev_timestamp, timestamp) < 10
            GROUP BY account_number
            HAVING COUNT(*) >= 3;
            
            UPDATE risk_scores
            SET risk_score = risk_score + (
                SELECT risk_weight FROM risk_rules WHERE rule_id = 3
            )
            WHERE account_number IN (SELECT account_number FROM high_frequency_accounts);
        """)
        console.print("[green]Rule 3 (high frequency) applied[/green]")
        
        # Apply Rule 4: Unusual Merchant Rule
        self.conn.execute("""
            UPDATE risk_scores
            SET risk_score = risk_score + (
                SELECT risk_weight FROM risk_rules WHERE rule_id = 4
            )
            WHERE merchant_category = 'suspicious'
               OR merchant_category NOT IN (
                   SELECT merchant_category 
                   FROM transactions 
                   WHERE merchant_category IS NOT NULL 
                   GROUP BY merchant_category 
                   HAVING COUNT(*) > 10
               );
        """)
        console.print("[green]Rule 4 (unusual merchant) applied[/green]")
        
        # Apply Rule 5: Account Velocity Rule
        self.conn.execute("""
            CREATE OR REPLACE TEMP TABLE account_daily_counts AS
            SELECT 
                account_number,
                CAST(timestamp AS DATE) as tx_date,
                COUNT(*) as daily_count
            FROM transactions
            GROUP BY account_number, CAST(timestamp AS DATE);
            
            CREATE OR REPLACE TEMP TABLE account_avg_activity AS
            SELECT 
                account_number,
                AVG(daily_count) as avg_daily_count,
                STDDEV(daily_count) as stddev_daily_count
            FROM account_daily_counts
            GROUP BY account_number;
            
            UPDATE risk_scores r
            SET risk_score = risk_score + (
                SELECT risk_weight FROM risk_rules WHERE rule_id = 5
            )
            FROM account_daily_counts c
            JOIN account_avg_activity a ON c.account_number = a.account_number
            WHERE r.account_number = c.account_number
              AND CAST(r.timestamp AS DATE) = c.tx_date
              AND c.daily_count > a.avg_daily_count + (2 * a.stddev_daily_count)
              AND a.avg_daily_count > 0;
        """)
        console.print("[green]Rule 5 (account velocity) applied[/green]")
        
        # Update risk levels based on final scores
        self.conn.execute("""
            UPDATE risk_scores
            SET risk_level = 
                CASE
                    WHEN risk_score >= 0.8 THEN 'Very High'
                    WHEN risk_score >= 0.6 THEN 'High'
                    WHEN risk_score >= 0.3 THEN 'Medium'
                    ELSE 'Low'
                END;
        """)
        
        console.print("[bold green]Risk assessment completed[/bold green]")
    
    def get_risk_summary(self):
        """
        Get a summary of risk assessment results.
        
        Returns:
            pl.DataFrame: Summary of risk assessment results
        """
        # Query risk summary
        result = self.conn.execute("""
            SELECT 
                risk_level,
                COUNT(*) as transaction_count,
                ROUND(AVG(amount), 2) as avg_amount,
                ROUND(SUM(amount), 2) as total_amount
            FROM risk_scores
            GROUP BY risk_level
            ORDER BY 
                CASE 
                    WHEN risk_level = 'Very High' THEN 1
                    WHEN risk_level = 'High' THEN 2
                    WHEN risk_level = 'Medium' THEN 3
                    WHEN risk_level = 'Low' THEN 4
                END;
        """).fetchdf()
        
        # Convert to Polars
        summary_df = pl.from_pandas(result)
        
        # Display summary table
        table = Table(title="Risk Assessment Summary")
        table.add_column("Risk Level", style="cyan")
        table.add_column("Transaction Count", style="magenta")
        table.add_column("Average Amount", style="green")
        table.add_column("Total Amount", style="yellow")
        
        for row in summary_df.iter_rows(named=True):
            table.add_row(
                row["risk_level"],
                str(row["transaction_count"]),
                f"${row['avg_amount']:,.2f}",
                f"${row['total_amount']:,.2f}"
            )
        
        console.print(table)
        
        return summary_df
    
    def get_high_risk_transactions(self, limit=10):
        """
        Get a list of high-risk transactions.
        
        Args:
            limit (int): Maximum number of transactions to return
        
        Returns:
            pl.DataFrame: High-risk transactions
        """
        result = self.conn.execute(f"""
            SELECT 
                transaction_id,
                account_number,
                timestamp,
                transaction_type,
                amount,
                merchant_name,
                merchant_category,
                risk_score,
                risk_level
            FROM risk_scores
            WHERE risk_level IN ('High', 'Very High')
            ORDER BY risk_score DESC
            LIMIT {limit};
        """).fetchdf()
        
        # Convert to Polars
        high_risk_df = pl.from_pandas(result)
        
        # Display high risk transactions
        table = Table(title="Top High-Risk Transactions")
        table.add_column("Transaction ID", style="dim")
        table.add_column("Account", style="cyan")
        table.add_column("Timestamp", style="magenta")
        table.add_column("Type", style="blue")
        table.add_column("Amount", style="green")
        table.add_column("Risk Score", style="red")
        table.add_column("Risk Level", style="yellow")
        
        for row in high_risk_df.iter_rows(named=True):
            table.add_row(
                str(row["transaction_id"])[:8] + "...",
                str(row["account_number"]),
                str(row["timestamp"]),
                row["transaction_type"],
                f"${row['amount']:,.2f}",
                f"{row['risk_score']:.2f}",
                row["risk_level"]
            )
        
        console.print(table)
        
        return high_risk_df
    
    def export_to_csv(self, output_path="data/risk_assessment_results.csv"):
        """
        Export risk assessment results to CSV.
        
        Args:
            output_path (str): Path to save the CSV file
        
        Returns:
            str: Path to the saved CSV file
        """
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Query and export results
        result = self.conn.execute("""
            SELECT * FROM risk_scores
            ORDER BY risk_score DESC;
        """).fetchdf()
        
        # Convert to Polars and save
        risk_df = pl.from_pandas(result)
        risk_df.write_csv(output_path)
        
        console.print(f"[bold green]Risk assessment results exported to {output_path}[/bold green]")
        return output_path
    
    def close(self):
        """Close the DuckDB connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            console.print("[green]DuckDB connection closed[/green]")

def assess_transaction_risk(db_path="database/transactions.duckdb", export_path="data/risk_assessment_results.csv"):
    """
    Assess transaction risk and generate risk scores.
    
    Args:
        db_path (str): Path to the DuckDB database file
        export_path (str): Path to save the risk assessment results
    
    Returns:
        bool: True if assessment was successful
    """
    risk_assessor = RiskAssessor(db_path)
    
    try:
        # Create risk tables
        risk_assessor.create_risk_tables()
        
        # Apply risk rules
        risk_assessor.apply_risk_rules()
        
        # Generate summary
        risk_assessor.get_risk_summary()
        
        # Show high risk transactions
        risk_assessor.get_high_risk_transactions()
        
        # Export results
        risk_assessor.export_to_csv(export_path)
        
        # Close connection
        risk_assessor.close()
        
        return True
    
    except Exception as e:
        console.print(f"[red]Error in risk assessment: {str(e)}[/red]")
        risk_assessor.close()
        return False

if __name__ == "__main__":
    # Run risk assessment when called directly
    assess_transaction_risk()