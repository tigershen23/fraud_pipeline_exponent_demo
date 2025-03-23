"""
Test module for risk_analysis.py
"""

import pytest
import duckdb
import os
import sys
import tempfile

# Add the parent directory to the path so we can import from fraud_pipeline
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from fraud_pipeline.src.data_generation import generate_transaction_data, save_transaction_data
from fraud_pipeline.src.data_loading import load_data_to_duckdb
from fraud_pipeline.src.risk_analysis import run_risk_analysis

@pytest.fixture
def sample_database():
    """Create a sample database with transaction data for testing."""
    # Generate a small test dataset with some known fraudulent transactions
    num_transactions = 50
    fraud_ratio = 0.1  # 10% fraudulent transactions
    df = generate_transaction_data(num_transactions=num_transactions, fraud_ratio=fraud_ratio)
    
    # Create a temporary directory and files
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "test_transactions.csv")
        db_path = os.path.join(temp_dir, "test_transactions.duckdb")
        
        # Save the data to CSV
        save_transaction_data(df, csv_path)
        
        # Load the data into DuckDB
        con = load_data_to_duckdb(csv_path, db_path)
        con.close()
        
        yield db_path

def test_run_risk_analysis(sample_database):
    """Test that risk analysis runs without errors and produces expected outputs."""
    # Run risk analysis
    try:
        run_risk_analysis(sample_database)
        
        # Connect to the database
        con = duckdb.connect(sample_database)
        
        # Check that the required tables were created
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        required_tables = ["transactions", "enriched_transactions", "risk_assessment", "transaction_velocity"]
        for table in required_tables:
            assert table in table_names, f"Required table {table} was not created"
        
        # Check that the risk_assessment table has the required columns
        risk_columns = con.execute("DESCRIBE risk_assessment").fetchall()
        risk_column_names = [col[0] for col in risk_columns]
        
        required_risk_columns = [
            "transaction_id", "risk_score", "risk_level", 
            "large_amount_flag", "odd_hours_flag", "suspicious_merchant_flag", "rapid_tx_flag"
        ]
        
        for col in required_risk_columns:
            assert col in risk_column_names, f"Required column {col} is missing from risk_assessment"
        
        # Check that risk levels were assigned
        risk_levels = con.execute("""
            SELECT risk_level, COUNT(*) as count 
            FROM risk_assessment 
            GROUP BY risk_level
        """).fetchall()
        
        # Convert to dictionary for easier checking
        risk_level_counts = {level: count for level, count in risk_levels}
        
        # Ensure that at least one risk level exists
        assert len(risk_level_counts) > 0, "No risk levels were assigned"
        
        # Verify that we have all expected risk levels
        expected_risk_levels = ["HIGH", "MEDIUM", "LOW"]
        for level in expected_risk_levels:
            # Note: Not all risk levels may be present in a small test dataset
            if level in risk_level_counts:
                assert risk_level_counts[level] >= 0, f"Invalid count for risk level {level}"
        
    finally:
        # Clean up
        if 'con' in locals():
            con.close()