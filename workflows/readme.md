//https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#special-case-adding-dependencies-via-requirements-txt-file

## ShopZada Data Warehouse

## Guide on how to run airflow on docker (without dependencies)

# **Be sure to run these in the root of your project**

### 1. Run and fetch the compose file

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.3/docker-compose.yaml'
```

### 2. Disable example DAGs

The downloaded `docker-compose.yaml` file has example DAGs enabled by default. To disable them, run the following command:

```bash
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/" docker-compose.yaml
```

### 3. Setup file structure and .env

```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_PROJ_DIR=$(pwd)\nAIRFLOW__CORE__LOAD_EXAMPLES=false" > .env
```

### 5. SELinux/AppArmor permissions (Optional)

On systems with SELinux/AppArmor, you may run into permission issues. If this happens, edit your docker-compose.yaml file by added the suffix `:z` to all volumes:

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags:z
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs:z
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config:z
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins:z
```

If, after this change, you are still experiencing permission issues when creating the airflow.cfg file, you can apply a very permissive setting to the `config/` folder:

```bash
sudo chmod -R 777 ./config
```
