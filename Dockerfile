FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p data database outputs

# Copy source code
COPY main.py .
COPY src/ src/
COPY tests/ tests/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
ENTRYPOINT ["python", "main.py"]

# Default arguments - can be overridden with docker run
CMD ["--num-records", "1000"]