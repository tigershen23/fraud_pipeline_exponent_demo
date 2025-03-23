"""
Data Generation Module for Financial Transaction Fraud Analytics Pipeline

This module simulates financial transaction data, including both normal transactions
and potential fraud scenarios.
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import polars as pl
from faker import Faker
from rich.console import Console
from rich.progress import Progress

# Initialize Faker and Console
fake = Faker()
console = Console()

def generate_account_number():
    """Generate a random account number."""
    return f"ACCT-{fake.random_number(digits=8, fix_len=True)}"

def generate_merchant_code():
    """Generate a random merchant code."""
    merchant_types = ['RETAIL', 'GROCERY', 'DINING', 'TRAVEL', 'ENTERTAINMENT', 'UTILITY', 'OTHER']
    merchant_type = random.choice(merchant_types)
    return f"{merchant_type}-{fake.random_number(digits=4, fix_len=True)}"

def generate_transaction_data(num_transactions=1000, fraud_ratio=0.05, start_date=None, end_date=None):
    """
    Generate synthetic financial transaction data.
    
    Args:
        num_transactions (int): Number of transactions to generate
        fraud_ratio (float): Ratio of fraudulent transactions (0.0 to 1.0)
        start_date (datetime): Start date for transactions (defaults to 90 days ago)
        end_date (datetime): End date for transactions (defaults to today)
    
    Returns:
        pl.DataFrame: DataFrame containing the generated transaction data
    """
    console.print("[bold green]Generating synthetic financial transaction data...[/bold green]")
    
    # Set default date range if not provided
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(days=90)
    
    # Determine number of fraudulent transactions
    num_fraud = int(num_transactions * fraud_ratio)
    num_normal = num_transactions - num_fraud
    
    # Create lists to hold our data
    transaction_ids = []
    timestamps = []
    account_numbers = []
    transaction_types = []
    amounts = []
    merchant_codes = []
    recipient_accounts = []
    is_frauds = []
    
    # Generate a pool of account numbers to use
    account_pool = [generate_account_number() for _ in range(100)]
    
    # Generate normal transactions
    with Progress() as progress:
        task = progress.add_task("[cyan]Generating normal transactions...", total=num_normal)
        
        for _ in range(num_normal):
            # Transaction basics
            transaction_ids.append(str(uuid.uuid4()))
            
            # Random timestamp within our date range
            random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
            timestamp = start_date + timedelta(seconds=random_seconds)
            timestamps.append(timestamp)
            
            # Account and transaction type
            account_number = random.choice(account_pool)
            account_numbers.append(account_number)
            
            transaction_type = random.choice(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PAYMENT'])
            transaction_types.append(transaction_type)
            
            # Amount (normal range)
            if transaction_type == 'DEPOSIT':
                amount = round(random.uniform(50, 5000), 2)
            elif transaction_type == 'WITHDRAWAL':
                amount = round(random.uniform(20, 500), 2)
            elif transaction_type == 'TRANSFER':
                amount = round(random.uniform(10, 2000), 2)
            else:  # PAYMENT
                amount = round(random.uniform(5, 1000), 2)
            amounts.append(amount)
            
            # Merchant code (only for PAYMENT type)
            if transaction_type == 'PAYMENT':
                merchant_codes.append(generate_merchant_code())
            else:
                merchant_codes.append(None)
            
            # Recipient account (only for TRANSFER type)
            if transaction_type == 'TRANSFER':
                recipient = random.choice([a for a in account_pool if a != account_number])
                recipient_accounts.append(recipient)
            else:
                recipient_accounts.append(None)
            
            # Not fraud
            is_frauds.append(False)
            
            progress.update(task, advance=1)
    
    # Generate fraudulent transactions
    with Progress() as progress:
        task = progress.add_task("[red]Generating fraudulent transactions...", total=num_fraud)
        
        for _ in range(num_fraud):
            # Transaction basics
            transaction_ids.append(str(uuid.uuid4()))
            
            # Random timestamp within our date range
            random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
            timestamp = start_date + timedelta(seconds=random_seconds)
            timestamps.append(timestamp)
            
            # Account number
            account_number = random.choice(account_pool)
            account_numbers.append(account_number)
            
            # Fraud scenarios
            fraud_type = random.choice([
                'large_withdrawal',
                'large_transfer',
                'unusual_merchant',
                'rapid_succession',
                'odd_hours_activity'
            ])
            
            if fraud_type == 'large_withdrawal':
                transaction_types.append('WITHDRAWAL')
                # Unusually large withdrawal
                amount = round(random.uniform(5000, 50000), 2)
                amounts.append(amount)
                merchant_codes.append(None)
                recipient_accounts.append(None)
                
            elif fraud_type == 'large_transfer':
                transaction_types.append('TRANSFER')
                # Unusually large transfer
                amount = round(random.uniform(10000, 100000), 2)
                amounts.append(amount)
                merchant_codes.append(None)
                recipient = random.choice([a for a in account_pool if a != account_number])
                recipient_accounts.append(recipient)
                
            elif fraud_type == 'unusual_merchant':
                transaction_types.append('PAYMENT')
                # Payment to suspicious merchant
                amount = round(random.uniform(500, 5000), 2)
                amounts.append(amount)
                # Create suspicious merchant codes
                merchant_codes.append(f"SUSPICIOUS-{fake.random_number(digits=4, fix_len=True)}")
                recipient_accounts.append(None)
                
            elif fraud_type == 'rapid_succession':
                # This will be handled differently - we'll add it as a feature later
                transaction_types.append(random.choice(['WITHDRAWAL', 'TRANSFER', 'PAYMENT']))
                amount = round(random.uniform(100, 1000), 2)
                amounts.append(amount)
                if transaction_types[-1] == 'PAYMENT':
                    merchant_codes.append(generate_merchant_code())
                else:
                    merchant_codes.append(None)
                if transaction_types[-1] == 'TRANSFER':
                    recipient = random.choice([a for a in account_pool if a != account_number])
                    recipient_accounts.append(recipient)
                else:
                    recipient_accounts.append(None)
                
            elif fraud_type == 'odd_hours_activity':
                # Transactions at odd hours (midnight to 4am)
                transaction_types.append(random.choice(['WITHDRAWAL', 'TRANSFER']))
                amount = round(random.uniform(100, 2000), 2)
                amounts.append(amount)
                merchant_codes.append(None)
                if transaction_types[-1] == 'TRANSFER':
                    recipient = random.choice([a for a in account_pool if a != account_number])
                    recipient_accounts.append(recipient)
                else:
                    recipient_accounts.append(None)
                # Adjust timestamp to be between midnight and 4am
                timestamp = timestamp.replace(hour=random.randint(0, 4), minute=random.randint(0, 59))
                timestamps[-1] = timestamp
            
            # Mark as fraud
            is_frauds.append(True)
            
            progress.update(task, advance=1)
    
    # Create DataFrame
    data = {
        'transaction_id': transaction_ids,
        'timestamp': timestamps,
        'account_number': account_numbers,
        'transaction_type': transaction_types,
        'amount': amounts,
        'merchant_code': merchant_codes,
        'recipient_account': recipient_accounts,
        'is_flagged': is_frauds  # Known fraud indicator for testing/validation
    }
    
    # Convert to Polars DataFrame
    df = pl.DataFrame(data)
    
    # Sort by timestamp
    df = df.sort("timestamp")
    
    console.print(f"[bold green]Successfully generated {num_transactions} transactions "
                 f"({num_fraud} fraudulent, {num_normal} normal)[/bold green]")
    
    return df

def save_transaction_data(df, output_path):
    """
    Save the transaction data to a CSV file.
    
    Args:
        df (pl.DataFrame): DataFrame containing transaction data
        output_path (str): Path to save the CSV file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to CSV
    df.write_csv(output_path)
    console.print(f"[bold green]Data saved to {output_path}[/bold green]")
    
    # Print sample of data
    console.print("[bold]Sample of generated data:[/bold]")
    console.print(df.head(5))

if __name__ == "__main__":
    # Generate transaction data
    transactions_df = generate_transaction_data(num_transactions=1000, fraud_ratio=0.05)
    
    # Save to CSV
    save_transaction_data(transactions_df, "fraud_pipeline/data/transactions.csv")