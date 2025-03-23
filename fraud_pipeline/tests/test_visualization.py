"""
Test module for visualization.py
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
from fraud_pipeline.src.visualization import create_visualizations

@pytest.fixture
def sample_database_with_risk():
    """Create a sample database with transaction data and risk analysis for testing."""
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
        
        # Run risk analysis
        run_risk_analysis(db_path)
        
        yield {
            'db_path': db_path,
            'temp_dir': temp_dir
        }

def test_create_visualizations(sample_database_with_risk):
    """Test that visualizations are created without errors."""
    db_path = sample_database_with_risk['db_path']
    output_dir = os.path.join(sample_database_with_risk['temp_dir'], "output")
    
    # Create visualizations
    create_visualizations(db_path, output_dir)
    
    # Check that output directory was created
    assert os.path.exists(output_dir), f"Output directory {output_dir} was not created"
    
    # Check that expected output files were created
    expected_files = [
        "transaction_volume.png",
        "risk_distribution.png",
        "risk_distribution_detection.png",
        "risk_report.md"
    ]
    
    for file in expected_files:
        file_path = os.path.join(output_dir, file)
        assert os.path.exists(file_path), f"Expected output file {file} was not created"
        assert os.path.getsize(file_path) > 0, f"Output file {file} is empty"