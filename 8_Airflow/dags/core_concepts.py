from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from random import seed, random

from airflow.utils.helpers import chain, cross_downstream

default_arguments = {'owner': 'Amit K', 'start_date': days_ago(1)}

#dag = DAG('core_concepts', schedule_interval = '@daily', catchup = False)

with DAG('core_concepts', 
    schedule_interval = None, 
    catchup = False,
    default_args = default_arguments
) as dag:
    bash_task = BashOperator(task_id = 'bash_command', bash_command='echo $TODAY', 
        env={'TODAY': '2020-05-21'})
    
    def print_random_number(number):
        seed(number)
        print(random())

    python_task = PythonOperator(task_id = 'python_funtion', 
        python_callable=print_random_number, op_args=[1])
    
    bash_task >> python_task
    # bash_task.set_downstream(python_task)
    # op1 >> op2 >> op3 >> op4
    # chain(op1, op2, op3, op4)

    # cross_downstream([op1, op2], [op3, op4])
    # [op1, op2] >> op3
    # [op3, op4] >> op4
