To run the full ShopZada DWH environment, use the following commands from the project root directory (`shopzada-data-warehouse/`).

|**Action**|**Command**|
|---|---|
|**Start Services** (DB, ETL, Airflow Web/Scheduler)|`docker compose -f ./infra/docker-compose.yml up -d`|
|**View Status/Health**|`docker compose -f ./infra/docker-compose.yml ps`|
|**Stop Services** (Retains DB data)|`docker compose -f ./infra/docker-compose.yml down`|
|**Clean Reset** (Stops all, **DELETES** DB data)|`docker compose -f ./infra/docker-compose.yml down -v`|
|**Airflow UI Access**|`http://localhost:8080` (Login: `airflow`/`airflow`)|
