#!/usr/bin/env python3
# data_loader.py
"""
Скачивает .xls файлы с Google Drive, приводит типы данных
и сохраняет результат в .csv и .parquet.
"""
import pandas as pd
import gdown
import os

def download_and_load_data(file_id: str, output_path: str) -> pd.DataFrame:
    """
    Скачивает .xls файл с Google Drive (если его еще нет)
    и читает его в pandas DataFrame.
    """
    # Формируем URL для скачивания
    url = f'https://drive.google.com/uc?id={file_id}'
    
    # Скачиваем файл, только если он отсутствует
    if not os.path.exists(output_path):
        print(f"Скачивание файла: {output_path}...")
        gdown.download(url, output_path, quiet=False)
    else:
        print(f"Файл '{output_path}' уже существует, пропускаем скачивание.")
    
    # Читаем .xls файл
    try:
        # Указываем 'xlrd', так как это старый формат .xls
        df = pd.read_excel(output_path, engine="xlrd")
        return df
    except Exception as e:
        print(f"Ошибка при чтении файла {output_path}: {e}")
        return None

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Приводит типы данных в DataFrame, используя
    встроенную функцию pandas.
    """
    print("Приведение типов...")
    original_types = df.dtypes
    
    # .convert_dtypes() - это умная функция, которая сама
    # пытается подобрать лучший тип (string, Int64, boolean, float и т.д.)
    df_transformed = df.convert_dtypes()
    
    # Выведем отчет, что изменилось
    for col in df.columns:
        if original_types[col] != df_transformed.dtypes[col]:
            print(f"  - Колонка '{col}': {original_types[col]} -> {df_transformed.dtypes[col]}")
            
    return df_transformed

def save_data(df: pd.DataFrame, base_path: str):
    """
    Сохраняет DataFrame в форматы .csv и .parquet.
    base_path - это путь *без* расширения,
    например 'processed_data/my_file'
    """
    
    #  Сохраняем в CSV
    csv_path = base_path + ".csv"
    try:
        df.to_csv(csv_path, index=False)
        print(f"Успешно сохранен: {csv_path}")
    except Exception as e:
        print(f"Ошибка сохранения {csv_path}: {e}")

    # Сохраняем в Parquet (требуется 'pyarrow' или 'fastparquet')
    parquet_path = base_path + ".parquet"
    try:
        df.to_parquet(parquet_path, index=False)
        print(f"Успешно сохранен: {parquet_path}")
    except Exception as e:
        print(f"Ошибка сохранения {parquet_path}: {e}")
        print(" (Для сохранения в Parquet убедитесь, что установлен 'pyarrow')")


if __name__ == "__main__":
    # ID ваших файлов
    FILE_IDS = {
        "ASVs_ITS2_sequences.xls": "1Ej41HIklJDCaaaJd6oGMrMiDwa9bgBED",
        "DADA2_clean_ASV_counts_ITS2_DPS.xls": "17CI7XNtneBPPJ3gHu3cPBw4pVitqTQuo",
        "DADA2_clean_ASV_counts_ITS2_Roots.xls": "1EfyjuGIFWWPfefwcPfb5-83qT21ozV-r",
        "DADA2_clean_ASV_taxonomy_ITS2_DPS.xls": "1RaVYEuaN6-tbNsMmCoeefunh7-MPQx2I",
        "DADA2_clean_ASV_taxonomy_ITS2_Roots.xls": "1A_TUJR031BX7WVoXgcr6-1cKG33ov6aU",
        "Metadata_DPS_ITS2.xls": "13T5frkJwgMUTzWCway3UCJ8hsf-hJqIc",
        "Metadata_Roots_ITS2.xls": "1m0ahdB3KhJKneJ36dKwr474jEZjinH0q"
    }

    # Папки для сырых и обработанных данных
    RAW_DIR = "data/raw"
    PROCESSED_DIR = "data/processed"

    # Создаем папки, если их нет
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # Словарь для хранения обработанных данных
    all_data = {}

    for filename, file_id in FILE_IDS.items():
        print(f"\n--- Обработка файла: {filename} ---")
        
        # Extract
        # Путь для скачивания сырого файла
        raw_file_path = os.path.join(RAW_DIR, filename)
        raw_df = download_and_load_data(file_id, raw_file_path)
        
        if raw_df is None:
            print(f"Ошибка загрузки {filename}, пропускаем.")
            continue
            
        print(f"Загружено {len(raw_df)} строк.")
        print("Первые 5 строк (до обработки):")
        print(raw_df.head(5))

        # Transform
        transformed_df = transform_data(raw_df)

        # Load
        # Убираем расширение .xls из имени файла
        base_name = os.path.splitext(filename)[0]
        # Путь для сохранения обработанного файла
        processed_base_path = os.path.join(PROCESSED_DIR, base_name)
        
        save_data(transformed_df, processed_base_path)
        
        print("Первые 5 строк (после обработки):")
        print(transformed_df.head(5))
        
        all_data[filename] = transformed_df

    print("\n--- Все файлы успешно обработаны ---")