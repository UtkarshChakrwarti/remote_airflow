"""Default namespace demo DAG (airflow-user)."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="example_user_namespace",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["namespace-demo", "airflow-user"],
):
    BashOperator(
        task_id="print_namespace",
        bash_command=(
            "NS=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace) && "
            "echo \"DAG=example_user_namespace task=print_namespace namespace=$NS host=$(hostname)\" && "
            "date"
        ),
    )
