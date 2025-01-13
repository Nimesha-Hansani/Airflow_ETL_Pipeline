from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
import sys
import boto3

#Loan environment variables from  .env file
load_dotenv()


sys.path.append('/home/airflow/Airflow_ETL_Pipeline/dags')
from source_load.data_load import run_script
from alerting.slack_alert import task_success_slack_alert

def send_sns_message(context):
    
    topic_arn = os.getenv("AIRFLOW_SNS_TOPIC_ARN")
    
    if not topic_arn:
        raise ValueError("SNS Topic ARN is not set in the environment variables.")

    sns_client = boto3.client('sns',region_name='us-east-1')

    # Publish the message
    response = sns_client.publish(
                TopicArn='arn:aws:sns:us-east-1:792281055989:Airflow_Failure',
                Message="Netflix_Data_Analytics DAG failed"
                              )



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': send_sns_message
}



dag = DAG(
          dag_id='Netflix_Data_Analytics',
          default_args=default_args,
          description='This dag runs data analytics on top of netflix datasets',
          schedule_interval=timedelta(days=1),
)

credits_sensor = S3KeySensor(
    task_id='credits_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/credits.csv',
    wildcard_match=True,
    bucket_name='nimesha-netflix-data-analytics',
    aws_conn_id='aws_default',
    dag=dag
)

titles_sensor = S3KeySensor(
    task_id='titles_rawfile_sensor',
    poke_interval=60 * 5,
    timeout=60 * 60 * 24 * 7,
    bucket_key='raw_files/titles.csv',
    wildcard_match=True,
    bucket_name='nimesha-netflix-data-analytics',
    aws_conn_id='aws_default',
    dag=dag
)

load_data_snowflake = PythonOperator(task_id='Load_Data_Snowflake',
                                     python_callable=run_script, 
                                     dag=dag)


run_stage_models = BashOperator(
    task_id='run_stage_models',
    bash_command='/home/airflow/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/airflow/Airflow_ETL_Pipeline/dbt_code/Netflix  --profile Netflix --target dev',
    dag=dag
)

run_fact_dim_models = BashOperator(
        task_id='run_fact_dim_models',
         bash_command='/home/airflow/dbt-env/bin/dbt run --model tag:"DIMENSION" --project-dir /home/airflow/Airflow_ETL_Pipeline/dbt_code/Netflix  --profile Netflix --target prod',
           dag=dag
         )



run_test_cases = BashOperator(
        task_id='run_test_cases',
        bash_command='/home/airflow/dbt-env/bin/dbt test --model tag:"TEST" --project-dir /home/airflow/Airflow_ETL_Pipeline/dbt_code/Netflix  --profile Netflix --target prod',
        dag=dag
        )



start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)


start_task >> credits_sensor >> titles_sensor >> load_data_snowflake >> run_stage_models >> run_fact_dim_models >> run_test_cases >>  end_task
