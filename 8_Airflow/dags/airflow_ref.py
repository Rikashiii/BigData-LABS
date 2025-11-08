import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
dag = DAG(
    dag_id="examref",
    description="e",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval=None,
)


bashop = BashOperator(task_id = "print", 
                                bash_command="echo 'PGDBDA'",
                                dag=dag
                                )

sparksub = SparkSubmitOperator(
        application ='/home/talentum/spark-submit/data_pipeline.py' ,
		conn_id= 'spark_default', 
		task_id='sparksub', 
        #env_vars={"PYSPARK_DRIVER_PYTHON":"/usr/bin/python3.6", "PYSPARK_DRIVER_PYTHON_OPTS":"","PYSPARK_PYTHON":"/usr/bin/python3.6","PYTHONPATH":"/usr/lib/python3.6"},
		dag=dag
)

# sparksub = SparkSubmitOperator(
#         application ='/home/talentum/spark-submit/data_pipeline.py' ,
# 		conn_id= 'spark_default', 
# 		task_id='sparksub', 
#         py_files='ingest.py,transform.py,persist.py',
#         driver_memory='2g',
#         num_executors=2,
#         executor_memory='1g',
#         #env_vars={"PYSPARK_DRIVER_PYTHON":"/usr/bin/python3.6", "PYSPARK_DRIVER_PYTHON_OPTS":"","PYSPARK_PYTHON":"/usr/bin/python3.6","PYTHONPATH":"/usr/lib/python3.6"},
# 		dag=dag
# )

bashop >> sparksub

# from AirflowUI>connections>spark_default Host is set to local 