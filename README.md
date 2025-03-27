# Enterprise Financial Transaction Risk & Fraud Analytics Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10.0+-green.svg)
![Prefect](https://img.shields.io/badge/Prefect-2.0.0+-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Ready-blue.svg)

## Overview

A fully autonomous pipeline designed for enterprise financial systems that processes simulated transaction data for basic heuristic risk and fraud analytics. This self-contained solution spans data generation, data processing, risk assessment, containerized deployment, and simple visualization—delivering actionable insights without external services, server setup, or cloud dependencies.

## Key Features

### Data Generation
- Simulates realistic financial transactions including deposits, withdrawals, and transfers
- Generates both normal transactions and crafted scenarios with potential fraud indicators
- Includes detailed attributes such as transaction IDs, account numbers, timestamps, amounts, and merchant codes

### Orchestrated Workflow
- Automates the entire pipeline from data ingestion to deployment
- Provides real-time progress tracking with detailed logs and status updates
- Incorporates error handling and self-healing mechanisms to maintain continuous operation

### Data Processing
- Cleanses and standardizes raw transaction data by removing duplicates and filling missing values
- Enriches data with basic metrics such as total transaction amounts per account
- Prepares data for risk assessment through simple normalization and feature engineering

### Basic Heuristic Risk Assessment
- Threshold-Based Risk Scoring:
  - Applies straightforward rules (e.g., flagging transactions above a preset amount or high transaction frequency within a short period)
  - Assigns risk scores based on these thresholds and flags suspicious transactions
  - Provides a summarized view of potential fraud alerts for quick review

### Local Database Storage
- Automatically provisions a local SQL database to store processed transaction data
- Enables SQL Queries: Users can run SQL queries to perform ad-hoc analysis and explore transaction details
- Operates entirely locally with no server setup or cloud dependencies required

### Visualization & Reporting
- Generates interactive dashboards:
  - A line chart depicting transaction volumes and average transaction amounts over time
  - A bar chart showing the number of flagged high-risk transactions
  - A heatmap of transaction activity by hour and day of week
  - A box plot of transaction amounts by risk level
- Produces a simple summary report of risk assessments and flagged transactions

## Architecture

The pipeline consists of several modular components:

1. **Data Generation Module** (`src/data_generation.py`): Generates synthetic transaction data with both normal patterns and potential fraud indicators.

2. **Data Processing Module** (`src/data_processing.py`): Cleans, transforms, and loads transaction data into a DuckDB database.

3. **Risk Assessment Module** (`src/risk_assessment.py`): Applies heuristic rules to assess transaction risk and identify potentially fraudulent activities.

4. **Visualization Module** (`src/visualization.py`): Creates interactive visualizations to help analyze transaction patterns and risk distribution.

5. **Utility Module** (`src/utils.py`): Provides common utility functions used throughout the pipeline.

6. **Main Orchestration Script** (`main.py`): Coordinates the entire pipeline workflow using Prefect for orchestration.

## Installation

### Local Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd fraud_pipeline_demo
```

2. Create a virtual environment and activate it:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

### Docker Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd fraud_pipeline_demo
```

2. Build the Docker image:

```bash
docker build -t fraud-analytics-pipeline .
```

## Usage

### Running Locally

To run the entire pipeline with default settings:

```bash
python main.py
```

To generate a larger dataset:

```bash
python main.py --num-records 10000
```

To run specific steps of the pipeline:

```bash
python main.py --steps generate process assess
```

### Available Command-Line Arguments:

- `--num-records`: Number of synthetic transactions to generate (default: 1000)
- `--csv-path`: Path to save/load transaction CSV (default: data/transactions.csv)
- `--db-path`: Path to DuckDB database (default: database/transactions.duckdb)
- `--export-path`: Path to export risk assessment results (default: data/risk_assessment_results.csv)
- `--output-dir`: Directory to save visualizations and reports (default: outputs)
- `--steps`: Specific steps to run (choices: generate, process, assess, visualize, report, all)

### Running with Docker

Run the pipeline with default settings:

```bash
docker run -v $(pwd)/data:/app/data -v $(pwd)/database:/app/database -v $(pwd)/outputs:/app/outputs fraud-analytics-pipeline
```

Generate 10,000 transactions:

```bash
docker run -v $(pwd)/data:/app/data -v $(pwd)/database:/app/database -v $(pwd)/outputs:/app/outputs fraud-analytics-pipeline --num-records 10000
```

## Example Outputs

After running the pipeline, you'll find:

1. **Raw Transaction Data**: CSV file in the `data/` directory
2. **DuckDB Database**: Database file in the `database/` directory
3. **Risk Assessment Results**: CSV file in the `data/` directory
4. **Visualizations**: HTML and PNG files in the `outputs/` directory
5. **Report**: Markdown file in the `outputs/` directory

### Visualizations

The pipeline generates several interactive visualizations:

1. **Transaction Time Series**: Shows transaction volumes and average amounts over time
2. **Risk Distribution**: Shows the distribution of transactions by risk level
3. **Transaction Heatmap**: Shows transaction activity by hour and day of week
4. **Amount Distribution**: Shows transaction amount distribution by risk level

## Testing

The project includes comprehensive unit tests for all components. To run the tests:

```bash
pytest
```

To run tests for a specific module:

```bash
pytest tests/test_data_generation.py
```

## Project Structure

```
fraud_pipeline/
├── data/                     # Directory to store generated data
├── database/                 # Directory to store DuckDB files
├── outputs/                  # Directory to store visualization outputs
├── src/                      # Source code
│   ├── __init__.py
│   ├── data_generation.py    # Data generation module
│   ├── data_processing.py    # Data processing module
│   ├── risk_assessment.py    # Risk assessment module
│   ├── visualization.py      # Visualization module
│   └── utils.py              # Utility functions
├── tests/                    # Test directory
│   ├── __init__.py
│   ├── test_data_generation.py
│   ├── test_data_processing.py
│   ├── test_risk_assessment.py
│   └── test_visualization.py
├── main.py                   # Main script to run the pipeline
├── Dockerfile                # Dockerfile for containerization
├── requirements.txt          # Dependencies
└── README.md                 # Project documentation
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgements

- This project uses DuckDB for efficient local database operations
- Visualizations are created using Plotly
- Pipeline orchestration is handled by Prefect
- Rich is used for console output formatting