# BTS Flight Data Processing System
FROM python:3.12.8-slim

# Install system dependencies including Java 11 for Spark
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment (Java 11 for Spark compatibility)
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt ./

# Install Python dependencies (all in requirements.txt for reproducibility)
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . .

# Create data directories
RUN mkdir -p data/bts_flight_data logs

# Expose common ports (for future web interface)
EXPOSE 8000

# Default command - run tests to verify setup
CMD ["python", "-m", "pytest", "tests/test_ci_unit.py", "-v"]

# Health check - verify core dependencies
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import neo4j, pyspark, pandas, pyarrow; print('Health check passed')" || exit 1
