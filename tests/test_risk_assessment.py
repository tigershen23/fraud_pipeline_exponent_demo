"""
Tests for the risk_assessment module.
"""

import os
import pytest
import polars as pl
import duckdb
from src.data_generation import DataGenerator
from src.data_processing import DataProcessor
from src.risk_assessment import RiskAssessor, assess_transaction_risk


@pytest.fixture
def prepared_db(tmp_path):
    """Create a DuckDB database with sample transaction data for testing."""
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

    return db_path


def test_risk_assessor_init(tmp_path):
    """Test RiskAssessor initialization."""
    db_path = os.path.join(tmp_path, "test.duckdb")
    risk_assessor = RiskAssessor(db_path=db_path)

    assert risk_assessor.db_path == db_path
    assert hasattr(risk_assessor, "conn")

    # Clean up
    risk_assessor.close()


def test_create_risk_tables(prepared_db):
    """Test creating risk assessment tables."""
    risk_assessor = RiskAssessor(db_path=prepared_db)

    # Create risk tables
    risk_assessor.create_risk_tables()

    # Verify tables exist
    result = risk_assessor.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('risk_scores', 'risk_rules')"
    ).fetchall()
    table_names = [r[0] for r in result]

    assert "risk_scores" in table_names
    assert "risk_rules" in table_names

    # Verify risk_scores table has data
    result = risk_assessor.conn.execute("SELECT COUNT(*) FROM risk_scores").fetchone()
    assert result[0] > 0

    # Verify risk_rules table has data
    result = risk_assessor.conn.execute("SELECT COUNT(*) FROM risk_rules").fetchone()
    assert result[0] > 0

    # Clean up
    risk_assessor.close()


def test_apply_risk_rules(prepared_db):
    """Test applying risk rules."""
    risk_assessor = RiskAssessor(db_path=prepared_db)

    # Create risk tables
    risk_assessor.create_risk_tables()

    # Get initial risk scores
    initial_scores = risk_assessor.conn.execute(
        "SELECT SUM(risk_score) FROM risk_scores"
    ).fetchone()[0]

    # Apply risk rules
    risk_assessor.apply_risk_rules()

    # Get updated risk scores
    updated_scores = risk_assessor.conn.execute(
        "SELECT SUM(risk_score) FROM risk_scores"
    ).fetchone()[0]

    # Verify risk scores have changed
    assert updated_scores > initial_scores

    # Verify risk levels are assigned
    risk_levels = risk_assessor.conn.execute(
        "SELECT DISTINCT risk_level FROM risk_scores"
    ).fetchall()
    risk_levels = [r[0] for r in risk_levels]

    assert len(risk_levels) > 1  # Should have multiple risk levels

    # Clean up
    risk_assessor.close()


def test_get_risk_summary(prepared_db):
    """Test getting risk assessment summary."""
    risk_assessor = RiskAssessor(db_path=prepared_db)

    # Create risk tables and apply rules
    risk_assessor.create_risk_tables()
    risk_assessor.apply_risk_rules()

    # Get risk summary
    summary_df = risk_assessor.get_risk_summary()

    assert isinstance(summary_df, pl.DataFrame)
    assert "risk_level" in summary_df.columns
    assert "transaction_count" in summary_df.columns
    assert "avg_amount" in summary_df.columns
    assert "total_amount" in summary_df.columns
    assert len(summary_df) > 0

    # Clean up
    risk_assessor.close()


def test_get_high_risk_transactions(prepared_db):
    """Test getting high-risk transactions."""
    risk_assessor = RiskAssessor(db_path=prepared_db)

    # Create risk tables and apply rules
    risk_assessor.create_risk_tables()
    risk_assessor.apply_risk_rules()

    # Get high-risk transactions
    high_risk_df = risk_assessor.get_high_risk_transactions(limit=5)

    assert isinstance(high_risk_df, pl.DataFrame)
    assert len(high_risk_df) <= 5  # Respects the limit

    if len(high_risk_df) > 0:
        assert all(high_risk_df["risk_level"].is_in(["High", "Very High"]))

    # Clean up
    risk_assessor.close()


def test_export_to_csv(prepared_db, tmp_path):
    """Test exporting risk assessment results to CSV."""
    risk_assessor = RiskAssessor(db_path=prepared_db)

    # Create risk tables and apply rules
    risk_assessor.create_risk_tables()
    risk_assessor.apply_risk_rules()

    # Export to CSV
    export_path = os.path.join(tmp_path, "test_risk_results.csv")
    result_path = risk_assessor.export_to_csv(export_path)

    assert result_path == export_path
    assert os.path.exists(export_path)

    # Check that we can read it back
    df = pl.read_csv(export_path)
    assert len(df) > 0
    assert "risk_score" in df.columns
    assert "risk_level" in df.columns

    # Clean up
    risk_assessor.close()


def test_assess_transaction_risk(prepared_db, tmp_path):
    """Test the main assess_transaction_risk function."""
    export_path = os.path.join(tmp_path, "test_risk_results.csv")

    # Assess risk
    success = assess_transaction_risk(db_path=prepared_db, export_path=export_path)

    assert success is True
    assert os.path.exists(export_path)

    # Check that we can read it back
    df = pl.read_csv(export_path)
    assert len(df) > 0
    assert "risk_score" in df.columns
    assert "risk_level" in df.columns
