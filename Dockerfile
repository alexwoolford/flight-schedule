# BTS Flight Data Processing System
FROM continuumio/miniconda3:latest

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

# Copy environment file for conda
COPY environment.yml ./

# Create conda environment and install dependencies
RUN conda env create -f environment.yml && \
    conda clean -afy

# Activate the environment by default
ENV PATH /opt/conda/envs/neo4j/bin:$PATH
ENV CONDA_DEFAULT_ENV neo4j

# Copy source code
COPY . .

# Create data directories
RUN mkdir -p data/bts_flight_data logs

# Expose common ports (for future web interface)
EXPOSE 8000

# Default command - run tests to verify setup
CMD ["/bin/bash", "-c", "source activate neo4j && python -m pytest tests/test_ci_unit.py -v"]

# Health check - verify core dependencies
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD /bin/bash -c "source activate neo4j && python -c 'import neo4j, pyspark, pandas, pyarrow; print(\"Health check passed\")'" || exit 1
