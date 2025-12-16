# Deployment Guide

## Docker Compose Deployment

### Prerequisites

- Docker and Docker Compose installed
- Sufficient resources (4GB RAM, 2 CPUs recommended)
- Git repository cloned locally

### Environment Configuration

1. **Copy .env template**:

```bash
cp .env.example .env
```

2. **Edit .env file** with your configuration:

```bash
# Data Configuration
DATA_FOLDER=/opt/airflow/plugins/data
DEFAULT_RETRIES=3

# Script Configuration
DEFAULT_SCRIPTS_FOLDER=/opt/airflow/plugins/scripts
INGESTION_FOLDER=ingestion
TRANSFORM_FOLDER=ingestion/transform

# File Sensor Configuration
FILE_SENSOR_SCHEDULE_MINUTES=2
FILE_SENSOR_POKE_INTERVAL=10
FILE_SENSOR_TIMEOUT=60
FILE_SENSOR_SOURCE_DIR=new
FILE_SENSOR_DEST_DIR=raw
FILE_SENSOR_MINDEPTH=1
MAIN_DAG_ID=shopzada_data_warehouse
```

### Starting Services

1. **Build and start all services**:

```bash
docker-compose up -d
```

2. **Initialize Airflow database**:

```bash
docker-compose run --rm airflow-init
```

3. **Check service status**:

```bash
docker-compose ps
```

### Access Points

- **Airflow UI**: http://localhost:8080 (airflow/airflow)
- **Metabase**: http://localhost:3000

### Service Management

#### Start Specific Services

```bash
# Start only Airflow services
docker-compose up -d airflow-apiserver airflow-scheduler airflow-worker

# Start with file sensor
docker-compose up -d file-sensor-dag
```

#### Stop Services

```bash
# Stop all services
docker-compose down

# Stop specific services
docker-compose stop airflow-scheduler
```

#### View Logs

```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f airflow-scheduler
```

### Performance

1. **Database Connection Pool**: Configure appropriate pool sizes
2. **Parallelism**: Adjust `max_active_runs` based on resources
3. **File Sensor Tuning**: Adjust `FILE_SENSOR_POKE_INTERVAL` based on file frequency
4. **Retry Logic**: Configure `DEFAULT_RETRIES` based on reliability requirements

### Monitoring

1. **Airflow Metrics**: Enable StatsD or OpenTelemetry
2. **Health Checks**: Monitor service health endpoints
3. **Log Aggregation**: Configure centralized logging
4. **Alerting**: Set up alerts for DAG failures

## Troubleshooting

### Common Issues

1. **Services Not Starting**:

   - Check port conflicts (8080, 5432, 5433, 3000)
   - Verify Docker daemon is running
   - Check resource availability

2. **DAGs Not Appearing**:

   - Verify volume mounts are correct
   - Check DAG file syntax
   - Review Airflow logs for parsing errors

3. **File Sensor Not Working**:

   - Verify DATA_FOLDER permissions
   - Check FILE_SENSOR_SOURCE_DIR exists
   - Review file sensor logs

4. **Script Execution Failures**:
   - Verify script files exist in DEFAULT_SCRIPTS_FOLDER
   - Check Python dependencies
   - Review task logs for specific errors

### Debug Commands

```bash
# Check Airflow configuration
docker-compose exec airflow-apiserver airflow config list

# Test database connection
docker-compose exec airflow-apiserver airflow connections test

# Run specific DAG manually
docker-compose exec airflow-apiserver airflow dags trigger shopzada_data_warehouse

# Check DAG parsing
docker-compose exec airflow-apiserver airflow dags report
```
