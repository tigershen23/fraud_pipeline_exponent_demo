"""
Tests for the visualization module.
"""

import os
import pytest
import duckdb
from src.data_generation import DataGenerator
from src.data_processing import DataProcessor
from src.risk_assessment import RiskAssessor
from src.visualization import VisualizationGenerator, generate_visualizations

@pytest.fixture
def prepared_db_with_risk(tmp_path):
    """Create a DuckDB database with sample transaction data and risk scores for testing."""
    # Generate sample data
    generator = DataGenerator(num_records=100, num_accounts=10, fraud_ratio=0.2)
    df = generator.generate_data()
    
    # Save to CSV
    csv_path = os.path.join(tmp_path, "test_transactions.csv")
    generator.save_data(df, csv_path)
    
    # Process data and load into DuckDB
    db_path = os.path.join(tmp_path, "test.duckdb")
    processor = DataProcessor(db_path=db_path)
    
    # Load, clean, and enrich data
    df = processor.load_data(csv_path)
    cleaned_df = processor.clean_data(df)
    enriched_df = processor.enrich_data(cleaned_df)
    
    # Load to DuckDB
    processor.load_to_duckdb(enriched_df)
    processor.close()
    
    # Create risk tables and apply risk rules
    risk_assessor = RiskAssessor(db_path=db_path)
    risk_assessor.create_risk_tables()
    risk_assessor.apply_risk_rules()
    risk_assessor.close()
    
    return db_path

def test_visualization_generator_init(tmp_path):
    """Test VisualizationGenerator initialization."""
    db_path = os.path.join(tmp_path, "test.duckdb")
    output_dir = os.path.join(tmp_path, "test_visualizations")
    
    viz_generator = VisualizationGenerator(db_path=db_path, output_dir=output_dir)
    
    assert viz_generator.db_path == db_path
    assert viz_generator.output_dir == output_dir
    assert os.path.exists(output_dir)
    assert hasattr(viz_generator, 'conn')
    
    # Clean up
    viz_generator.close()

def test_generate_transaction_time_series(prepared_db_with_risk, tmp_path):
    """Test generating transaction time series visualization."""
    output_dir = os.path.join(tmp_path, "test_visualizations")
    viz_generator = VisualizationGenerator(db_path=prepared_db_with_risk, output_dir=output_dir)
    
    # Generate time series visualization
    output_path = viz_generator.generate_transaction_time_series()
    
    assert output_path == os.path.join(output_dir, "transaction_time_series.html")
    assert os.path.exists(output_path)
    assert os.path.exists(os.path.join(output_dir, "transaction_time_series.png"))
    
    # Clean up
    viz_generator.close()

def test_generate_risk_distribution(prepared_db_with_risk, tmp_path):
    """Test generating risk distribution visualization."""
    output_dir = os.path.join(tmp_path, "test_visualizations")
    viz_generator = VisualizationGenerator(db_path=prepared_db_with_risk, output_dir=output_dir)
    
    # Generate risk distribution visualization
    output_path = viz_generator.generate_risk_distribution()
    
    assert output_path == os.path.join(output_dir, "risk_distribution.html")
    assert os.path.exists(output_path)
    assert os.path.exists(os.path.join(output_dir, "risk_distribution.png"))
    
    # Clean up
    viz_generator.close()

def test_generate_transaction_by_hour_heatmap(prepared_db_with_risk, tmp_path):
    """Test generating transaction heatmap visualization."""
    output_dir = os.path.join(tmp_path, "test_visualizations")
    viz_generator = VisualizationGenerator(db_path=prepared_db_with_risk, output_dir=output_dir)
    
    # Generate transaction heatmap visualization
    output_path = viz_generator.generate_transaction_by_hour_heatmap()
    
    assert output_path == os.path.join(output_dir, "transaction_heatmap.html")
    assert os.path.exists(output_path)
    assert os.path.exists(os.path.join(output_dir, "transaction_heatmap.png"))
    
    # Clean up
    viz_generator.close()

def test_generate_amount_distribution(prepared_db_with_risk, tmp_path):
    """Test generating amount distribution visualization."""
    output_dir = os.path.join(tmp_path, "test_visualizations")
    viz_generator = VisualizationGenerator(db_path=prepared_db_with_risk, output_dir=output_dir)
    
    # Generate amount distribution visualization
    output_path = viz_generator.generate_amount_distribution()
    
    assert output_path == os.path.join(output_dir, "amount_distribution.html")
    assert os.path.exists(output_path)
    assert os.path.exists(os.path.join(output_dir, "amount_distribution.png"))
    
    # Clean up
    viz_generator.close()

def test_generate_visualizations(prepared_db_with_risk, tmp_path):
    """Test the main generate_visualizations function."""
    output_dir = os.path.join(tmp_path, "test_visualizations")
    
    # Generate visualizations
    paths = generate_visualizations(db_path=prepared_db_with_risk, output_dir=output_dir)
    
    assert isinstance(paths, list)
    assert len(paths) > 0
    
    # Check that expected visualization files exist
    assert os.path.exists(os.path.join(output_dir, "transaction_time_series.html"))
    assert os.path.exists(os.path.join(output_dir, "risk_distribution.html"))
    assert os.path.exists(os.path.join(output_dir, "transaction_heatmap.html"))
    assert os.path.exists(os.path.join(output_dir, "amount_distribution.html"))