from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# Импорт функций из скриптов
from daily_script import aggregate_daily_logs
from weekly_script import aggregate_weekly_logs

# Задаем параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определяем DAG
with DAG(
    dag_id='daily_weekly_aggregation',
    default_args=default_args,
    description='Агрегация данных за день и неделю',
    schedule_interval='0 7 * * *',  # Запуск каждый день в 7:00
    catchup=False,
) as dag:

    # Функция для запуска дневной агрегации
    def run_daily_aggregation(**kwargs):
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        data_dir = '/absolute/path/to/data'
        daily_dir = '/absolute/path/to/daily'
        aggregate_daily_logs(yesterday)  # Вызываем без дополнительных аргументов
      
    # Таск для дневной агрегации
    daily_task = PythonOperator(
        task_id='daily_aggregation',
        python_callable=run_daily_aggregation,
        provide_context=True,
        dag=dag,
    )

    # Функция для запуска недельной агрегации
    def run_weekly_aggregation(**kwargs):
        today = datetime.now().strftime('%Y-%m-%d')
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, '..', 'data')
        daily_dir = os.path.join(base_dir, '..', 'daily')
        output_dir = os.path.join(base_dir, '..', 'output')
        aggregate_weekly_logs(today, daily_dir, output_dir, data_dir)

    # Таск для недельной агрегации
    weekly_task = PythonOperator(
        task_id='weekly_aggregation',
        python_callable=run_weekly_aggregation,
        provide_context=True,
        dag=dag,
    )

    # Задаем порядок выполнения задач
    daily_task >> weekly_task