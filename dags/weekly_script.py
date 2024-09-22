import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Функция для получения относительных путей к папкам
def get_paths():
    # Указываем относительные пути к директориям с данными, ежедневными и итоговыми файлами
    data_dir = os.path.join('.', 'data')
    daily_dir = os.path.join('.', 'daily')
    output_dir = os.path.join('.', 'output')
    return data_dir, daily_dir, output_dir

# Функция для агрегации данных за один день (если файл не существует в daily)
def aggregate_daily_logs(date, data_dir, daily_dir):
        
    # Формируем путь к файлу в папке daily
    daily_file = os.path.join(daily_dir, f'{date}.csv')
    
    # Проверяем, существует ли уже файл за эту дату
    if os.path.exists(daily_file):
        print(f"Файл за {date} уже существует. Загружаем данные из {daily_file}.")
        return pd.read_csv(daily_file)

    # Если файла нет, загружаем исходные данные из папки data
    data_file = os.path.join(data_dir, f'{date}.csv')
    if not os.path.exists(data_file):
        print(f"Данные за {date} отсутствуют в папке data.")
        return pd.DataFrame(columns=['email', 'create_count', 'read_count', 'update_count', 'delete_count'])
    
    # Агрегируем данные за этот день
    print(f"Агрегируем данные за {date}.")
    logs = pd.read_csv(data_file, names=['email', 'action', 'dt'])
    
    # Группируем данные по пользователям и типам действий
    aggregated_logs = logs.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()

    # Добавляем недостающие действия с нулевыми значениями, если они отсутствуют
    for action in ['CREATE', 'READ', 'UPDATE', 'DELETE']:
        if action not in aggregated_logs.columns:
            aggregated_logs[action] = 0

    # Переименовываем колонки для итогового результата
    aggregated_logs = aggregated_logs.rename(columns={
        'CREATE': 'create_count', 
        'READ': 'read_count', 
        'UPDATE': 'update_count', 
        'DELETE': 'delete_count'
    })
    
    # Сохраняем агрегированные данные в папку daily
    aggregated_logs.to_csv(daily_file, index=False)
    print(f"Агрегированные данные за {date} сохранены в {daily_file}.")
    
    return aggregated_logs

# Функция для агрегации данных за неделю (7 предыдущих дней)
def aggregate_weekly_logs(end_date, daily_dir, output_dir, data_dir):
    """Агрегирует данные за 7 предыдущих дней (без дня end_date)."""
    
    # Формируем путь к итоговому недельному файлу в папке output
    output_file = os.path.join(output_dir, f'{end_date}.csv')
    
    # Проверяем, существует ли уже файл за этот период
    if os.path.exists(output_file):
        print(f"Файл с недельной агрегатной статистикой за {end_date} уже существует.")
        return pd.read_csv(output_file)
    
    # Определяем дату окончания (end_date) и создаем пустую таблицу для агрегированных данных
    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
    weekly_aggregated = pd.DataFrame(columns=['email', 'create_count', 'read_count', 'update_count', 'delete_count'])
    
    # Проходим по 7 предыдущим дням, начиная с конца (end_date - 1)
    for i in range(1, 8):
        current_date = (end_date_dt - timedelta(days=i)).strftime('%Y-%m-%d')
        daily_file = os.path.join(daily_dir, f'{current_date}.csv')
        
        # Если файл за конкретный день отсутствует, выполняем агрегацию за этот день
        if not os.path.exists(daily_file):
            print(f"Файл за {current_date} отсутствует. Запускаем агрегацию.")
            aggregate_daily_logs(current_date, data_dir, daily_dir)
        
        # Если файл существует, загружаем данные и добавляем их в общую агрегацию
        if os.path.exists(daily_file):
            print(f"Загружаем данные за {current_date}.")
            daily_data = pd.read_csv(daily_file)
            weekly_aggregated = pd.concat([weekly_aggregated, daily_data], ignore_index=True)
    
    # Если агрегированные данные не пусты, группируем их по пользователям и суммируем значения
    if not weekly_aggregated.empty:
        weekly_aggregated = weekly_aggregated.groupby('email').sum().reset_index()
    
    # Сохраняем итоговые агрегированные данные в файл в папке output
    weekly_aggregated.to_csv(output_file, index=False)
    print(f"Недельные агрегированные данные за {end_date} сохранены в {output_file}.")
    
    return weekly_aggregated

# Основная часть программы
if __name__ == "__main__":
    # Получаем дату окончания периода из аргументов командной строки или используем текущую дату
    end_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    
    # Определяем пути к папкам с данными
    data_dir, daily_dir, output_dir = get_paths()  
    
    # Выполняем агрегацию за неделю и выводим результат
    weekly_data = aggregate_weekly_logs(end_date, daily_dir, output_dir, data_dir)
    print(weekly_data.head())