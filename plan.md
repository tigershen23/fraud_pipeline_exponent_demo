# Enterprise Financial Transaction Risk & Fraud Analytics Pipeline

## Project Overview

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

- Generates two basic, interactive dashboards:
  - A line chart depicting transaction volumes and average transaction amounts over time
  - A bar chart showing the number of flagged high-risk transactions
- Produces a simple summary report of risk assessments and flagged transactions

### Documentation & Version Control

- Automatically generates clear documentation covering the pipeline architecture, data schemas, and risk assessment logic
- Integrates Git for version control to track all changes and facilitate collaboration
- Shares the complete project via a version-controlled GitHub repository

### Containerized Deployment

- Docker Containerization:
- Packages the entire application into a Docker container for consistent, local demo deployments
- Simplifies environment setup and ensures reproducible results with a pre-configured Dockerfile

## User Experience

Users execute the pipeline with a single command that initiates data generation, processing, and containerized deployment. The CLI provides real-time, informative updates throughout the process and concludes with interactive dashboards and a summary risk report.

## Technical Foundation

Built on modern Python libraries, this solution emphasizes performance and simplicity. Containerized with Docker, the demo runs locally with no server or cloud setup required, showcasing a complete enterprise analytics experience ideal for demonstration purposes.

## Deliverables

- Complete source code with detailed comments and unit tests
- Sample transaction data and a pre-configured SQL database
- Basic interactive dashboards and a summary risk report
- Comprehensive, automatically generated documentation
- Dockerfile and deployment scripts for containerized execution
- A fully version-controlled GitHub repository for code, documentation, and deployment assets

## Implementation Instructions

Below is a comprehensive, step-by-step set of instructions for working on the project.

Important notes to keep in mind:

- Never think for more than a few hundred tokens before you get back to actually taking steps in the project, so that we can continue to make progress.
- Pay careful attention to and make sure to follow all instructions for verifying your progress at each step. Avoid purely writing code without checking it, make sure you are balancing adding code with verifying your progress.

1. Environment Setup and Dependency Installation

   - You are already in a fresh python virtual environment
   - Install Required Dependencies with a long enough timeout to ensure all dependencies are installed:
     - Install all necessary packages including Prefect (for orchestration), Polars (for data manipulation), DuckDB (for local database storage), Rich (for terminal output), Matplotlib (for visualization), Faker (for data simulation), and Pytest (for testing)
     - Verify that these libraries are correctly installed by running a brief script that imports each dependency and prints version information

2. Data Generation: Simulate Financial Transactions

   - Define the Data Schema:
     - Plan to simulate realistic financial transactions with fields such as transaction ID, account number, timestamp, transaction type (deposit, withdrawal, transfer), amount, merchant code, and additional fraud indicators
   - Generate Synthetic Data:
     - Use a data simulation library (like Faker) in combination with a high-performance DataFrame library to generate at least 1,000 records
     - Ensure that both normal transactions and crafted fraud scenarios (for example, exceptionally high transaction amounts) are included
   - Save the Data:
     - Save the generated data to a CSV file within the project directory
     - Verify that the file exists and that its content meets the expected record count by inspecting a sample of the data

3. Load Raw Data into DuckDB

   - Direct Data Ingestion:
     - Load the CSV file directly into a local DuckDB database
     - Create (or connect to) a DuckDB database file and use its built-in CSV ingestion capabilities
   - Verification:
     - Run a query to count the number of records in the loaded table and log the result
     - The output should match the expected record count from the data generation step

4. Data Transformation and Risk Analysis Using DuckDB SQL

   - Perform Data Transformation:
     - Execute a series of SQL commands within DuckDB to clean and enrich the data
     - Tasks include removing duplicate records, converting timestamps to proper date-time formats, and deriving new fields (such as extracting the hour from a timestamp)
   - Heuristic Risk Assessment:
     - Define a set of simple, threshold-based risk rules to flag transactions
     - Use SQL to apply these rules directly within DuckDB, categorizing transactions into risk levels (for example, flagging transactions above a certain amount as "High Risk")
   - Verification:
     - Run summary queries that count and list transactions by risk level
     - Log these results using Rich to provide visually engaging, real-time feedback, confirming that the risk assessment logic is operating as expected

5. Generate Interactive Visualizations and Reporting

   - Prepare Data for Visualization:
     - Query summary metrics from DuckDB, such as daily transaction volumes, average transaction amounts, and counts of transactions by risk level
   - Create Visual Dashboards:
     - Generate at least two visualizations:
       - A line chart showing transaction volume and average amounts over time
       - A bar chart summarizing the distribution of risk levels
   - Output Visualizations:
     - Save the generated visualization images to the project directory
     - Verify file existence and log confirmation messages indicating that the visualization process has completed successfully

6. Documentation and Automated Reporting

   - Generate a README File:
     - Create a comprehensive README file
     - Document the project's overview, data schema, processing steps, risk assessment logic, and instructions for running the pipeline
     - Include the visualization images in the README file
   - Verification:
     - Verify that the README contains all necessary sections and that it is correctly formatted
     - Optionally, use a markdown linter to ensure quality

7. Testing: Unit and Integration Verification

   - Develop Unit Tests:
     - Create unit tests for key functions such as data generation, transformation logic, risk scoring, and database operations
   - Conduct Integration Tests:
     - Write tests that run the entire pipeline end-to-end, ensuring that the final outputs (both in the DuckDB database and the generated visualizations) meet the expected criteria
   - Verification:
     - Run all tests and log the outcomes, ensuring that every test passes
     - Capture and address any errors

8. Containerization (Dockerization) of the Application

   - Create a Dockerfile:
     - Develop a Dockerfile that packages the entire application
     - Include:
       - A base Python image
       - Copying of all project files
       - Installation of all dependencies listed in a requirements file
       - A default command that executes the entire pipeline
   - Build and Test the Docker Image:
     - Build the Docker image locally
     - Run a container to ensure that the entire application executes correctly within the container
   - Verification:
     - Verify that the container runs without errors and produces all expected outputs
     - Use file existence checks and log summaries to confirm successful containerization

9. Final Commit and GitHub Deployment
   - Commit All Project Files:
     - Stage all source code, tests, Dockerfile, documentation, CSV data, and generated visualizations
     - Commit these changes with a descriptive commit message that reflects the project's purpose
   - Push to GitHub:
     - Using the GitHub CLI, create a new repository and push all commits to GitHub
   - Verification:
     - Confirm that the GitHub repository is fully updated and accessible
     - Ensure version control and collaboration readiness

## Final Verification and Execution

### Single-Command Execution

- Execute the entire pipeline with a single command (by running the Docker container)
- This command should trigger:
  - Data generation
  - Loading into DuckDB
  - SQL-based transformation and risk assessment
  - Visualization generation
  - Comprehensive logging via Rich

### Real-Time Feedback

- Throughout the process, use Rich to provide clear, colorful status updates and logs for each major step

### Overall Project Review

Confirm that:

- The DuckDB database contains the processed, risk-assessed data
- All visualizations are generated and saved
- The README file is comprehensive and correctly formatted
- All unit and integration tests pass
- The project is fully containerized and available on GitHub
