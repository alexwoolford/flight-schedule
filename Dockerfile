# Flight Schedule Graph Database
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment (Java 17 is default in Debian bookworm)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt requirements-dev.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copy source code
COPY . .

# Create data directory
RUN mkdir -p data/flight_list

# Expose common ports (if running a web interface in the future)
EXPOSE 8000

# Default command
CMD ["python", "flight_search_demo.py"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import neo4j; print('Health check passed')" || exit 1