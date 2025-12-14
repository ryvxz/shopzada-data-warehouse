# Environment Variables Configuration

## Data Configuration
| Variable | Default | Description |
|-----------|---------|-------------|
| DATA_FOLDER | /opt/airflow/plugins/data | Base directory for data files |
| DEFAULT_RETRIES | 3 | Number of retry attempts for failed tasks |

## Script Configuration
| Variable | Default | Description |
|-----------|---------|-------------|
| DEFAULT_SCRIPTS_FOLDER | /opt/airflow/plugins/scripts | Base directory for Python scripts |
| INGESTION_FOLDER | ingestion | Subfolder for ingestion scripts |
| TRANSFORM_FOLDER | ingestion/transform | Subfolder for transformation scripts |

## Script Names
| Variable | Default | Description |
|-----------|---------|-------------|
| LOAD_TO_PARQUET_SCRIPT | load_to_parquet | Script for loading data to parquet format |
| DATA_QUALITY_CHECKS_SCRIPT | data_quality_checks | Script for data quality validation |
| LOAD_TO_STAGING_SCRIPT | load_to_staging | Script for loading data to staging database |
| TRANSFORM_SCRIPT | transform | Script for data transformation |
| QUALITY_CHECKS_SCRIPT | quality_checks | Script for transformation quality checks |
| CLEAN_PREPROCESSED_FILES_SCRIPT | clean_preprocessed_files | Script for cleaning temporary files |

## File Sensor Configuration
| Variable | Default | Description |
|-----------|---------|-------------|
| FILE_SENSOR_SCHEDULE_MINUTES | 2 | Sensor check interval in minutes |
| FILE_SENSOR_POKE_INTERVAL | 10 | FileSensor poke interval in seconds |
| FILE_SENSOR_TIMEOUT | 60 | FileSensor timeout in seconds |
| FILE_SENSOR_SOURCE_DIR | new | Directory to monitor for new files |
| FILE_SENSOR_DEST_DIR | raw | Directory to move processed files |
| FILE_SENSOR_MINDEPTH | 1 | Minimum depth for file search |
| MAIN_DAG_ID | shopzada_data_warehouse | DAG ID to trigger when files are detected |

## Usage Examples

### Basic Configuration
```bash
# Set custom data directory
export DATA_FOLDER="/custom/data/path"

# Set custom retry count
export DEFAULT_RETRIES=5

# Set custom script folder
export DEFAULT_SCRIPTS_FOLDER="/custom/scripts"
```

### File Sensor Configuration
```bash
# Set faster file checking (every 1 minute)
export FILE_SENSOR_SCHEDULE_MINUTES=1

# Set custom source directory
export FILE_SENSOR_SOURCE_DIR="incoming"

# Set custom destination directory
export FILE_SENSOR_DEST_DIR="processed"
```

### Script Configuration
```bash
# Use custom script names
export LOAD_TO_PARQUET_SCRIPT="custom_parquet_loader"
export TRANSFORM_SCRIPT="custom_transform"

# Use custom folders
export INGESTION_FOLDER="custom_ingestion"
export TRANSFORM_FOLDER="custom_transform"
```

## Validation

The DAGs include automatic validation for critical environment variables:

- **Positive Integers**: All timing and retry variables must be positive integers
- **Minimal Safe Fallbacks**: Invalid values fall back to minimal safe defaults
- **Logging**: Warnings are logged for invalid configurations

## Deployment Notes

1. **Docker Environment**: Set environment variables in `.env` file or docker-compose.yml
2. **Kubernetes**: Use ConfigMaps or Secrets for environment variables
3. **Local Development**: Export variables in shell or set in IDE environment

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure DATA_FOLDER is writable by Airflow user
2. **Script Not Found**: Verify DEFAULT_SCRIPTS_FOLDER path is correct
3. **File Sensor Not Triggering**: Check FILE_SENSOR_SOURCE_DIR exists and is accessible
4. **Invalid Values**: Check Airflow logs for validation warnings

### Debug Mode

Enable debug logging by setting:
```bash
export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG
```

This will show detailed environment variable processing and validation messages.