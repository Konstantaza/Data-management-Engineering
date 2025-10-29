# etl/main.py
import sys
import os
import argparse

# Мы добавляем путь к корневой папке репозитория
# (где лежит папка etl) в пути поиска Python.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Поднимаемся на один уровень вверх (из etl/ в корень)
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(ROOT_DIR)

from etl import extract, transform, load


def main(step_to_run):
    """
    Главная функция, запускающая ETL-процесс по шагам.
    """
    print(f"--- Запуск ETL процесса (ШАГ: {step_to_run}) ---")
    
    # Извлечение
    if step_to_run in ['extract', 'all']:
        # Передаем ROOT_DIR в функцию
        raw_paths = extract.extract_all_data(ROOT_DIR)
        if not raw_paths:
            print("ОШИБКА: ЭТАП ИЗВЛЕЧЕНИЯ не вернул данных. Остановка.")
            return

    # Трансформация
    if step_to_run in ['transform', 'all']:
        # Передаем ROOT_DIR в функцию
        transformed_data = transform.transform_all_data(ROOT_DIR)
        if not transformed_data:
            print("ОШИБКА: ЭТАП ТРАНСФОРМАЦИИ не вернул данных. Остановка.")
            return

    # Загрузка
    if step_to_run in ['load', 'all']:
        if step_to_run == 'load':
            print("Шаг 'load' требует данных от 'transform'. Запускаем 'transform'...")
            # Передаем ROOT_DIR в функцию
            transformed_data = transform.transform_all_data(ROOT_DIR)
            if not transformed_data:
                print("ОШИБКА: ЭТАП ТРАНСФОРМАЦИИ не вернул данных. Остановка.")
                return

        # Передаем ROOT_DIR в функцию
        load.load_all_data(transformed_data, ROOT_DIR)

    print("--- ETL процесс завершен ---")


if __name__ == "__main__":
    # Создаем парсер аргументов (CLI)
    parser = argparse.ArgumentParser(description="ETL-пайплайн.")
    
    parser.add_argument(
        '--step', 
        type=str, 
        required=True, 
        choices=['extract', 'transform', 'load', 'all'],
        help="Шаг ETL, который нужно выполнить: 'extract', 'transform', 'load' или 'all' (все по очереди)."
    )
    
    args = parser.parse_args()
    
    # Запускаем главную функцию с переданным шагом
    main(args.step)