"""
Data Generation Module

This module handles the generation of synthetic financial transaction data
including normal transactions and potential fraud scenarios.
"""

import os
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from rich.console import Console
from rich.progress import Progress

console = Console()
fake = Faker()

# Transaction types
TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer", "payment", "refund"]

# Merchant categories
MERCHANT_CATEGORIES = ["retail", "food", "travel", "entertainment", "utilities", "healthcare", "financial"]

class DataGenerator:
    """Class for generating synthetic financial transaction data."""
    
    def __init__(self, num_records=1000, num_accounts=100, fraud_ratio=0.05):
        """
        Initialize the DataGenerator.
        
        Args:
            num_records (int): Number of transaction records to generate
            num_accounts (int): Number of unique accounts to simulate
            fraud_ratio (float): Percentage of transactions that will have fraud indicators
        """
        self.num_records = num_records
        self.num_accounts = num_accounts
        self.fraud_ratio = fraud_ratio
        self.account_numbers = [fake.unique.random_number(digits=10) for _ in range(num_accounts)]
    
    def generate_normal_transaction(self):
        """Generate a normal financial transaction."""
        account = np.random.choice(self.account_numbers)
        transaction_type = np.random.choice(TRANSACTION_TYPES)
        
        # Different amount ranges based on transaction type
        if transaction_type == "deposit":
            amount = round(np.random.uniform(10, 5000), 2)
        elif transaction_type == "withdrawal":
            amount = round(np.random.uniform(10, 1000), 2)
        elif transaction_type == "transfer":
            amount = round(np.random.uniform(5, 2000), 2)
        elif transaction_type == "payment":
            amount = round(np.random.uniform(5, 500), 2)
        else:  # refund
            amount = round(np.random.uniform(5, 200), 2)
        
        # Generate timestamp within the last 30 days
        timestamp = datetime.now() - timedelta(
            days=np.random.randint(0, 30),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        
        # Generate merchant info for certain transaction types
        merchant_name = None
        merchant_category = None
        if transaction_type in ["payment", "refund"]:
            merchant_name = fake.company()
            merchant_category = np.random.choice(MERCHANT_CATEGORIES)
        
        # Create transaction
        transaction = {
            "transaction_id": fake.uuid4(),
            "account_number": str(account),
            "timestamp": timestamp,
            "transaction_type": transaction_type,
            "amount": amount,
            "merchant_name": merchant_name,
            "merchant_category": merchant_category,
            "is_fraud": False,
            "risk_score": 0.0  # Will be calculated later
        }
        
        return transaction
    
    def generate_fraudulent_transaction(self):
        """Generate a transaction with fraud indicators."""
        # Start with a normal transaction
        transaction = self.generate_normal_transaction()
        
        # Apply one of several fraud patterns
        fraud_type = np.random.randint(0, 4)
        
        if fraud_type == 0:
            # Unusually large amount
            transaction["amount"] = round(np.random.uniform(5000, 50000), 2)
        
        elif fraud_type == 1:
            # Unusual time (late night)
            hour = np.random.randint(1, 5)  # 1 AM to 4 AM
            transaction["timestamp"] = transaction["timestamp"].replace(hour=hour)
        
        elif fraud_type == 2:
            # Multiple transactions in short time (this will be part of a sequence)
            # We'll handle this when generating the full dataset
            pass
        
        elif fraud_type == 3:
            # Unusual location/merchant
            transaction["merchant_name"] = fake.company()
            transaction["merchant_category"] = "suspicious"
        
        # Mark as fraud
        transaction["is_fraud"] = True
        
        return transaction
    
    def generate_data(self):
        """Generate the full synthetic transaction dataset."""
        transactions = []
        
        console.print("[bold green]Generating synthetic transaction data...[/bold green]")
        
        with Progress() as progress:
            task = progress.add_task("[cyan]Generating transactions...", total=self.num_records)
            
            # Calculate number of fraud transactions
            num_fraud = int(self.num_records * self.fraud_ratio)
            num_normal = self.num_records - num_fraud
            
            # Generate normal transactions
            for _ in range(num_normal):
                transactions.append(self.generate_normal_transaction())
                progress.update(task, advance=1)
            
            # Generate fraud transactions
            for _ in range(num_fraud):
                transactions.append(self.generate_fraudulent_transaction())
                progress.update(task, advance=1)
            
            # Shuffle to mix normal and fraudulent transactions
            np.random.shuffle(transactions)
        
        # Convert to DataFrame
        df = pl.DataFrame(transactions)
        
        # Sort by timestamp
        df = df.sort("timestamp")
        
        console.print(f"[bold green]Generated {self.num_records} transactions ({num_fraud} fraudulent)[/bold green]")
        
        return df
    
    def save_data(self, df, output_path="data/transactions.csv"):
        """Save the generated data to a CSV file."""
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.write_csv(output_path)
        console.print(f"[bold green]Data saved to {output_path}[/bold green]")
        
        return output_path

def generate_transaction_data(num_records=1000, output_path="data/transactions.csv"):
    """
    Generate synthetic transaction data and save it to a CSV file.
    
    Args:
        num_records (int): Number of transaction records to generate
        output_path (str): Path to save the CSV file
    
    Returns:
        str: Path to the saved CSV file
    """
    data_generator = DataGenerator(num_records=num_records)
    df = data_generator.generate_data()
    output_path = data_generator.save_data(df, output_path)
    return output_path

if __name__ == "__main__":
    # Generate 1000 transactions when run directly
    generate_transaction_data(1000)