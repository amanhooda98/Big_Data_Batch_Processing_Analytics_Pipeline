from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
import datetime as dt

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1
}

# Define your DAG
dag = DAG(
    'spark_batch_job',
    schedule_interval=None,  # You can set a suitable schedule interval here
    start_date=dt.datetime(2023,9,26),  # Adjust the start date as needed
    catchup=False,
)

# Define the SSHHook with the remote server information
ssh_hook = SSHHook(
    remote_host='34.30.45.117',  # Replace with your remote server's IP
    username='amanhd9',
    key_file='/home/amanhd9/.ssh/gcp',  # Replace with your SSH key file path
    cmd_timeout=None
)


upload_to_data_lake_commands = """
source ~/.profile
cd ~
spark-submit /home/amanhd9/notebooks/upload_to_data_lake.py
"""

# Create an SSHOperator to run the Spark submit job
T1 = SSHOperator(
    task_id='upload_to_data_lake_commands',
    ssh_hook=ssh_hook,
    command=upload_to_data_lake_commands,
    dag=dag,
)
# Define the command to run the Spark submit job
upload_to_data_warehouse_commands = """
source ~/.profile
cd ~
spark-submit /home/amanhd9/notebooks/upload_to_data_warehouse.py
"""

T2 = SSHOperator(
    task_id='upload_to_data_warehouse_commands',
    ssh_hook=ssh_hook,
    command=upload_to_data_warehouse_commands,
    dag=dag,
)

T1>>T2

