# Use a slim, stable Python version (3.11) as the base for the ETL service.
FROM python:3.11-slim-bookworm

# Set working directory inside the container.
WORKDIR /usr/src/app

# Ensure proper user ID is set for volume permissions (Airflow best practice).
ARG AIRFLOW_UID=50000 
ENV AIRFLOW_UID=${AIRFLOW_UID} 

# Copy dependencies and install them first for better Docker layer caching.
COPY scripts/ingestion/requirements.txt .

# Install all Python dependencies.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project code into the container.
COPY . .