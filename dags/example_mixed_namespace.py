"""Single DAG proving both namespace routes in one run."""

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
    dag_id="example_mixed_namespace",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["namespace-demo", "mixed"],
):
    user_namespace_task = BashOperator(
        task_id="user_namespace_task",
        bash_command=(
            "NS=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace) && "
            "echo \"DAG=example_mixed_namespace task=user_namespace_task namespace=$NS host=$(hostname)\" && "
            "date"
        ),
    )

    core_namespace_task = BashOperator(
        task_id="core_namespace_task",
        bash_command=(
            "NS=$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace) && "
            "echo \"DAG=example_mixed_namespace task=core_namespace_task namespace=$NS host=$(hostname)\" && "
            "date"
        ),
        executor_config=CORE_NAMESPACE_EXECUTOR_CONFIG,
    )

    user_namespace_task >> core_namespace_task
