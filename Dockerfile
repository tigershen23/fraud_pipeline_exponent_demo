FROM python:3.10-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . .

# Create output directories
RUN mkdir -p fraud_pipeline/data fraud_pipeline/output

# Set up entrypoint
ENTRYPOINT ["python", "fraud_pipeline/main.py"]

# Default command (can be overridden)
CMD ["--transactions", "1000", "--fraud", "0.05"]