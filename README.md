# CRUD Logs Aggregator

## Описание проекта

Этот проект представляет собой приложение для пакетной обработки данных, разработанное с целью агрегации логов CRUD (Create, Read, Update, Delete) пользователей. Приложение читает данные из CSV файлов, проводит агрегацию по каждому пользователю и сохраняет результаты в отдельные файлы. Проект также включает автоматизацию процессов с использованием Apache Airflow.

## Структура проекта

```
crud-logs-aggregator/
│
├── dags/                     # DAG для Airflow и основные скрипты
│   ├── dag_auto_aggregate.py # Автоматизация агрегации
│   ├── weekly_script.py      # Скрипт еженедельной агрегации
│   └── daily_script.py       # Скрипт ежедневной агрегации
├── data/                     # Входные данные (CSV файлы)
│   └── 2024-09-10.csv        # Пример файла с логами
├── output/                   # Агрегированные выходные файлы
├── daily/                    # Файлы для агрегирования по дням
├── logs/                     # Логи выполнения задач Airflow
├── docker-compose.yaml       # Docker Compose конфигурация
├── README.md                 # Описание проекта
└── .gitignore                # Не отслеживаемые файлы
```

## Скрипты

### daily_script.py

Скрипт для агрегации данных за один день. Он проверяет наличие файла с необходимой датой в папке `daily`. Если файл уже существует, данные загружаются из него. Если файла нет, скрипт ищет данные за этот день в папке `data`, агрегирует их и сохраняет в папку `daily`.

### weekly_script.py

Скрипт для агрегации данных за прошлые 7 дней. Он собирает данные из папки `daily`, проверяя наличие файлов за предыдущие дни. Если файл отсутствует, запускается `daily_script.py` для агрегации данных за этот день. В результате формируется общий отчет за неделю, который сохраняется в папке `output`.

### dag_auto_aggregate.py

DAG для Apache Airflow, который автоматизирует запуск процессов агрегации. Он запускает `daily_script.py` каждый день в 7:00, а затем запускает `weekly_script.py` для обновления недельной статистики.

## Использование

### Системные зависимости

- Python 3.6 или выше
- Pandas
- Apache Airflow
- Docker и Docker Compose (для развертывания Airflow в контейнерах)

### Установка и запуск

1. Склонируйте репозиторий:
   ```bash
   git clone https://github.com/rambayer/crud-logs-aggregator
   cd crud-logs-aggregator
   ```

2. Убедитесь, что у вас установлен Docker и Docker Compose.

3. Запустите Airflow (через Docker Compose):
   
- Команда для запуска контейнеров:   
   ```bash
   docker compose up -d
   ```
- Команда для остановки контейнеров:
   ```bash
   docker compose down
   ``` 

4. Откройте веб-интерфейс Airflow по адресу `http://localhost:8080`(Логин и Пароль: `Admin`)
   
- Активируйте DAG `daily_weekly_aggregation` и запустите его вручную через интерфейс.
- Агрегированные данные за день сохраняются в папку `daily`.<br>
- Агрегированные данные за неделю сохраняються в папку `output`.


5. Скрипты так же можно запустить вручную через терминал:

- Указывайте дату в интервале с `2024-09-11` по `2024-10-09`;

- ежедневная агрегация (дата по умолчанию `2024-10-01`):
  ```bash
  python dags/daily_script.py <YYYY-MM-DD>
  ```

- недельная агрегация (по умолчанию текущая дата):
  ```bash
  python dags/weekly_script.py <YYYY-MM-DD>
  ```
