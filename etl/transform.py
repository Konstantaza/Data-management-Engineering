# etl/transform.py
import pandas as pd
import os


def transform_data(raw_file_path: str) -> pd.DataFrame | None:
    """
    Читает .csv из raw, приводит типы и
    выполняет специфичные трансформации (melt).
    """
    try:
        df = pd.read_csv(raw_file_path)
    except FileNotFoundError:
        print(f"  [Transform Ошибка] Файл не найден: {raw_file_path}")
        return None
    except Exception as e:
        print(f"  [Transform Ошибка] Чтение {raw_file_path}: {e}")
        return None

    # Общая Трансформация: приведение типов
    # .convert_dtypes() - умная функция, сама подберет (string, Int64, boolean)
    df_transformed = df.convert_dtypes()
    
    # Специфичная Трансформация: "плавление" (melt)
    # Мы применяем эту логику только к файлам, где есть "counts"
    filename = os.path.basename(raw_file_path)
    
    if "counts" in filename:
        print(f"  [Transform] Применяем 'melt' для {filename}...")
        try:
            # Убедимся, что первая колонка - это 'asv_id'
            id_col = df_transformed.columns[0]
            
            df_melted = df_transformed.melt(
                id_vars=[id_col], 
                var_name='sample_id', 
                value_name='count'
            )
            # Отфильтровываем строки, где count = 0, т.к. их нет смысла хранить
            df_melted = df_melted[df_melted['count'] > 0]
            print(f"  [Transform] 'melt' для {filename} завершен.")
            return df_melted
        
        except Exception as e:
            print(f"  [Transform Ошибка] 'melt' для {filename} не удался: {e}")
            return None # Пропускаем, если трансформация не удалась

    return df_transformed


def transform_all_data(root_dir: str) -> dict[str, pd.DataFrame]: # 
    """
    Главная функция модуля transform.
    Берет все .csv из /raw, трансформирует их.
    Возвращает словарь {имя_файла: DataFrame}.
    """
    print("--- 2. Этап трансформации (TRANSFORM) ---")
    
    raw_data_dir = os.path.join(root_dir, "my_project", "data", "raw")

    transformed_data = {}
    
    if not os.path.exists(raw_data_dir):
        print(f"  [Transform Ошибка] Папка {raw_data_dir} не найдена. Запустите extract.py")
        return {}

    for filename in os.listdir(raw_data_dir):
        if filename.endswith(".csv"):
            raw_path = os.path.join(raw_data_dir, filename)
            
            df = transform_data(raw_path)
            
            if df is not None:
                base_name = os.path.splitext(filename)[0]
                transformed_data[base_name] = df
                
    print("--- Этап трансформации завершен ---\n")
    return transformed_data

if __name__ == "__main__":
    print("Запущен etl/transform.py как самостоятельный скрипт.")
    transform_all_data()