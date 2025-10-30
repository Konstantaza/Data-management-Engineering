# etl/extract.py
import gdown
import pandas as pd
import os
import shutil  # Для удаления временных папок

# ID ваших файлов
FILE_IDS = {
    "ASVs_ITS2_sequences.csv": "1Ej41HIklJDCaaaJd6oGMrMiDwa9bgBED",
    "DADA2_clean_ASV_counts_ITS2_DPS.csv": "17CI7XNtneBPPJ3gHu3cPBw4pVitqTQuo",
    "DADA2_clean_ASV_counts_ITS2_Roots.csv": "1EfyjuGIFWWPfefwcPfb5-83qT21ozV-r",
    "DADA2_clean_ASV_taxonomy_ITS2_DPS.csv": "1RaVYEuaN6-tbNsMmCoeefunh7-MPQx2I",
    "DADA2_clean_ASV_taxonomy_ITS2_Roots.csv": "1A_TUJR031BX7WVoXgcr6-1cKG33ov6aU",
    "Metadata_DPS_ITS2.csv": "13T5frkJwgMUTzWCway3UCJ8hsf-hJqIc",
    "Metadata_Roots_ITS2.csv": "1m0ahdB3KhJKneJ36dKwr474jEZjinH0q",
}


def download_and_convert_to_csv(file_id: str, output_path: str):
    """
    Скачивает .xls с Google Drive, читает его
    и сохраняет как .csv в RAW_DATA_DIR.
    """

    # Создаем временную папку для скачивания .xls
    temp_dir = "temp_xls_download"
    os.makedirs(temp_dir, exist_ok=True)
    # Имя файла .xls будет временным
    temp_xls_path = os.path.join(
        temp_dir, os.path.basename(output_path).replace(".csv", ".xls")
    )

    # Скачиваем .xls файл
    url = f"https://drive.google.com/uc?id={file_id}"

    # Проверяем, существует ли конечный .csv файл
    if not os.path.exists(output_path):
        print(f"Скачивание: {output_path}...")
        try:
            # Качаем во временный .xls
            gdown.download(url, temp_xls_path, quiet=False)
        except Exception as e:
            print(f"  [Extract Ошибка] Скачивание {url} не удалось: {e}")
            shutil.rmtree(temp_dir)  # Чистим за собой
            return False  # Пропускаем, если не удалось скачать

        # Читаем .xls
        try:
            df = pd.read_excel(temp_xls_path, engine="xlrd")

            # Валидация
            if df.empty:
                print(
                    f"  [Extract Ошибка] Файл {temp_xls_path} пуст. Валидация источника не пройдена."
                )
                return False

            # Сохраняем как .csv
            df.to_csv(output_path, index=False)
            print(f"  [Extract] Успешно сохранен: {output_path}")

        except Exception as e:
            print(f"  [Extract Ошибка] Обработка {temp_xls_path} не удалась: {e}")
            return False
        finally:
            # Удаляем временную папку с .xls файлом
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    else:
        print(f"Файл '{output_path}' уже существует, пропускаем.")

    return True


def extract_all_data(root_dir: str):
    """
    Главная функция модуля extract.
    Скачивает и сохраняет все файлы из FILE_IDS.
    Возвращает список путей к созданным .csv файлам.
    """
    print("--- 1. Этап извлечения (EXTRACT) ---")

    # Строим путь от корня проекта
    raw_data_dir = os.path.join(root_dir, "my_project", "data", "raw")

    os.makedirs(raw_data_dir, exist_ok=True)

    raw_paths = []

    for filename, file_id in FILE_IDS.items():
        raw_file_path = os.path.join(raw_data_dir, filename)

        if download_and_convert_to_csv(file_id, raw_file_path):
            raw_paths.append(raw_file_path)

    print("--- ЭТАП извлечения завершен ---\n")
    return raw_paths


if __name__ == "__main__":
    # Этот блок нужен для самостоятельной проверки модуля
    # Мы не будем его запускать напрямую, т.к. наши
    # библиотеки (pandas, gdown) установлены в my_project
    print("Запущен etl/extract.py как самостоятельный скрипт.")
    extract_all_data()
