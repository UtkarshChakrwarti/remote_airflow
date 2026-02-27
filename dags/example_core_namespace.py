"""Namespace override demo DAG (airflow-core)."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s

CORE_NAMESPACE_EXECUTOR_CONFIG = {
    "pod_override": k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(namespace="airflow-core"),
    ),
}

with DAG(
    dag_id="example_core_namespace",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["namespace-demo", "airflow-core"],
):
    BashOperator(
        task_id="print_namespace",
        bash_command=(
            "NS=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace) && "
            "echo \"DAG=example_core_namespace task=print_namespace namespace=$NS host=$(hostname)\" && "
            "date"
        ),
        executor_config=CORE_NAMESPACE_EXECUTOR_CONFIG,
    )
