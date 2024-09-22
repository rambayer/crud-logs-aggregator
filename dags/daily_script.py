import pandas as pd
import sys
import os

# Функция для агрегации данных за один день
def aggregate_daily_logs(date):
    # Определяем абсолютные пути к папкам
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, '..', 'data')
    daily_dir = os.path.join(base_dir, '..', 'daily')

    # Формируем путь к файлу в папке daily для заданной даты
    daily_file = os.path.join(daily_dir, f'{date}.csv')
    
    # Проверяем, существует ли файл с агрегированными данными за эту дату
    if os.path.exists(daily_file):
        print(f"Файл за {date} уже существует. Загружаем данные из {daily_file}.")
        return pd.read_csv(daily_file)
    
    # Если файл не существует, пытаемся загрузить исходные данные из папки data
    data_file = os.path.join(data_dir, f'{date}.csv')
    
    # Если данных за указанную дату нет, возвращаем пустой DataFrame с нужными колонками
    if not os.path.exists(data_file):
        print(f"Данные за {date} отсутствуют в папке data.")
        return pd.DataFrame(columns=['email', 'create_count', 'read_count', 'update_count', 'delete_count'])
    
    print(f"Агрегируем данные за {date}.")
    
    # Загружаем данные из CSV файла
    logs = pd.read_csv(data_file, names=['email', 'action', 'dt'])
    
    # Группируем данные по пользователям и типам действий
    aggregated_logs = logs.groupby(['email', 'action']).size().unstack(fill_value=0).reset_index()
    
    # Добавляем колонки для каждого действия, если их нет, и присваиваем им значение 0
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

if __name__ == "__main__":
    # Получаем дату из аргументов командной строки или используем значение по умолчанию
    date = sys.argv[1] if len(sys.argv) > 1 else '2024-10-01'
    aggregated_data = aggregate_daily_logs(date)
    print(aggregated_data.head())