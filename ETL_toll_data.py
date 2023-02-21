# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Apple',
    'start_date': days_ago(0),
    'email': ['apple@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
# define the task unzip data
upzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='sudo tar -zxvf /home/project/airflow/dags/finalassignment/tolldata.tgz -c /home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)
# define the task named extract data from csv
Extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicel-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)
# define the task named extract data from tsv
Extract_data_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv| tr -d "\r" | tr “[:blank:] "," > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)
# define the task named extract data from fixed width
Extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt| tr " " "," > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)
# define the task named consolidate data
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = "paste -d “,” /home/project/airflow/dags/finalassignment/staging/tolldata/csv_data.csv /home/airflow/dags/finalassignment/staging/tolldata/tsv_data.csv /home/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/airflow/dags/finalassignment/staging/extracted_data.csv",
    dag = dag,
)
#define the task named transform data
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'tr "[a-z]" "[A-Z]" < /home/project/airflow/finalassignment/staging/extracted_data.csv > /home/project/airflow/finalassignment/staging/transformed_data.csv',
    dag = dag,
)
#define task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data

