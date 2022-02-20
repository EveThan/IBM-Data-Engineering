# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

# define DAG arguments
default_args = {
    'owner' : 'Eve Than',
    'start_date' : days_ago(0),
    'email' : ['zhengen@ualberta.ca'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks
# define the first task
unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xvf tolldata.tgz',
    dag=dag,
)

# define the second task
extract_data_from_csv = BashOperator(
    task_id='extract_from_csv',
    bash_command='cut -f1,4 -d"," vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# define the third task
extract_data_from_tsv = BashOperator(
    task_id='extract_from_tsv',
    bash_command='cut -f1,4 -d" " tollplaza-data.tsv > tsv_data.csv',
    dag=dag,
)

# define the fourth task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_from_fixed_width',
    bash_command='cut -f1,4 -d" " payment-data.txt > fixed_width_data.csv',
    dag=dag,
)

# define the fifth task
consolidate_data = BashOperator(
    task_id='consolidate',
    bash_command='paste csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

# define the sixth task
transform_data = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < extracted_data.csv > transformed_data.csv',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data