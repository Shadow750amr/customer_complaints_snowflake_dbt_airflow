from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
# 1. Cambiamos el import para Snowflake
from cosmos.profiles import SnowflakeUserPasswordProfileMapping 

import os
from datetime import datetime

airflow_home = os.environ["AIRFLOW_HOME"]

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    # 2. Usamos el mapping de Snowflake
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn", # Asegúrate de que este ID exista en Airflow Connections
        profile_args={
            "database": "COMPLAINTS_DB", 
            "schema": "DEV"
        },
    ),
)

my_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        f"{airflow_home}/dags/complaints_pipeline",
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path=f"{airflow_home}/dbt_venv/bin/dbt",
    ),
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 2},
)