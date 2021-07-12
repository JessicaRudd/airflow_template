# -*- coding: utf-8 -*-
"""
Created on Sun Jul 4 16:57:18 2021

@author: daniel
"""

from datetime import timedelta
from textwrap import dedent
from sample_unit_test_script import *

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}

with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
 
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )
    
    
    # sample python operator usage
    def sample_python_function(x):
        print('Here we can call functions or run scripts from separate python files')

    t4 = PythonOperator(
        task_id='some_ds_process',
        python_callable=sample_python_function,
    )
    
    # call unit test script after running some process
    t5 = BashOperator(
        task_id='run_unittest',
        depends_on_past=False,
        bash_command='python sample_unit_test_scrip.py'
    )
    
    

    t1 >> [t2, t3] >> t4 >> t5