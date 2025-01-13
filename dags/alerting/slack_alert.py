# Importing the base book
from airflow.hooks.base_hook import BaseHook
# Importing the Slack Webhook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
#from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
# importing the common settings
# Get the System Environment


def task_success_slack_alert(task_id, slack_conn_id, dag):
    """
    Creates a SlackWebhookOperator for success messages.

    :param task_id: Unique task ID for the operator.
    :param slack_conn_id: Airflow connection ID for Slack.
    :param dag: Airflow DAG object to attach this task to.
    :return: Configured SlackWebhookOperator.
    """
    # Prepare the Slack message
    slack_msg = """
        :large_green_circle: {{ task_instance.dag_id }} Workflow completed successfully.
        *Task*: {{ task_instance.task_id }}
        *Dag*: {{ task_instance.dag_id }}
        *Execution Time*: {{ execution_date }}
    """

    # Create the SlackWebhookOperator
    return SlackWebhookOperator(
        task_id=task_id,
        slack_webhook_conn_id=slack_conn_id,
        message=slack_msg,
        dag=dag,)

def task_fail_slack_alert(task_id, slack_conn_id, dag):

    # Prepare the Slack message
    slack_msg = """
        :large_red__circle: {{ task_instance.dag_id }} Workflow Failed.
        *Task*: {{ task_instance.task_id }}
        *Dag*: {{ task_instance.dag_id }}
        *Execution Time*: {{ execution_date }}
    """

    return SlackWebhookOperator(
    task_id = task_id,
    slack_webhook_conn_id = slack_conn_id,
    message = slack_msg,
    dag = dag

            )



