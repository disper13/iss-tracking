from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
import requests
from clickhouse_driver import Client
from datetime import timedelta


def fetch_iss_position(**context):
    response = requests.get("http://api.open-notify.org/iss-now.json", timeout=10)
    response.raise_for_status()
    data = response.json()

    position = {
        "timestamp": datetime.fromtimestamp(
            data["timestamp"], tz=timezone.utc
        ).replace(tzinfo=None),
        "latitude": float(data["iss_position"]["latitude"]),
        "longitude": float(data["iss_position"]["longitude"]),
    }

    context["ti"].xcom_push(key="position", value=position)


def get_country(**context):
    position = context["ti"].xcom_pull(key="position", task_ids="fetch_iss_position")

    lat = position["latitude"]
    lon = position["longitude"]

    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={
                "lat": lat,
                "lon": lon,
                "format": "json",
            },
            headers={"User-Agent": "iss-tracker/1.0"},
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()

        country = data.get("address", {}).get("country")
        country_code = data.get("address", {}).get("country_code", "").upper() or None
        is_over_land = 1

    except Exception:
        country = None
        country_code = None
        is_over_land = 0

    position.update({
        "country": country,
        "country_code": country_code,
        "is_over_land": is_over_land,
    })

    context["ti"].xcom_push(key="position_enriched", value=position)


def save_to_clickhouse(**context):
    position = context["ti"].xcom_pull(
        key="position_enriched", task_ids="get_country"
    )

    client = Client(
        host="clickhouse",
        port=9000,
        user="default",
        password="clickhouse",
        database="iss",
    )

    client.execute(
        """
        INSERT INTO iss.positions
            (timestamp, latitude, longitude, country, country_code, is_over_land)
        VALUES
        """,
        [[
            position["timestamp"],
            position["latitude"],
            position["longitude"],
            position["country"],
            position["country_code"],
            position["is_over_land"],
        ]],
    )


with DAG(
        dag_id="iss_tracker",
        start_date=datetime(2024, 1, 1),
        schedule_interval="* * * * *",
        catchup=False,
        max_active_runs=1,
        default_args = {
            "retries": 2,
            "retry_delay": timedelta(seconds = 10),
        },
) as dag:
    task_fetch = PythonOperator(
        task_id="fetch_iss_position",
        python_callable=fetch_iss_position,
    )

    task_country = PythonOperator(
        task_id="get_country",
        python_callable=get_country,
    )

    task_save = PythonOperator(
        task_id="save_to_clickhouse",
        python_callable=save_to_clickhouse,
    )

    task_fetch >> task_country >> task_save
