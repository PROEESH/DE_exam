# # Dockerfile
# FROM python:3.10-slim

# ENV DEBIAN_FRONTEND=noninteractive

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# # Copy requirements first to leverage Docker caching
# COPY requirements.txt .
# RUN pip install --upgrade pip && pip install -r requirements.txt

# # Copy your pipeline code
# COPY src/Pipelines /app/src/Pipelines


# # Default entrypoint (Dataflow overrides it when running Flex Template)
# ENTRYPOINT ["sleep", "infinity"]


# Base image for Python Dataflow Flex Template
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

# Working directory inside container
WORKDIR /template

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your pipeline scripts
COPY src/Pipelines/ /template/src/Pipelines/

# Set the entrypoint to your pipeline script
ENTRYPOINT ["python", "/template/src/Pipelines/ingest-api2.py"]