import pandas as pd
import gdown
import os

def download_and_load_data(file_id, output_path):
    """
    Скачивает файл с Google Drive и загружает его в pandas DataFrame.
    """
    url = f'https://drive.google.com/uc?id={file_id}'
    
    print(f"\nСкачивание файла с ID: {file_id}...")
    gdown.download(url, output_path, quiet=False)
    
    if os.path.exists(output_path):
        print(f"Файл '{output_path}' успешно скачан. Чтение данных...")
        try:
            # ---> ИЗМЕНЕНИЕ: Используем read_excel вместо read_csv <---
            df = pd.read_excel(output_path)
            return df
        except Exception as e:
            print(f"Ошибка при чтении файла: {e}")
            return None
    else:
        print("Не удалось скачать файл.")
        return None

if __name__ == "__main__":
    # ---> ВАЖНО: Вставить сюда ID ВСЕХ .xls файлов <---
    FILE_IDS = {
    
        "ASVs_ITS2_sequences.xls": "1Ej41HIklJDCaaaJd6oGMrMiDwa9bgBED",
        "DADA2_clean_ASV_counts_ITS2_DPS.xls": "17CI7XNtneBPPJ3gHu3cPBw4pVitqTQuo",
        "DADA2_clean_ASV_counts_ITS2_Roots.xls": "1EfyjuGIFWWPfefwcPfb5-83qT21ozV-r",
	"DADA2_clean_ASV_taxonomy_ITS2_DPS.xls": "1RaVYEuaN6-tbNsMmCoeefunh7-MPQx2I",
	"DADA2_clean_ASV_taxonomy_ITS2_Roots.xls": "1A_TUJR031BX7WVoXgcr6-1cKG33ov6aU",
	"Metadata_DPS_ITS2.xls": "13T5frkJwgMUTzWCway3UCJ8hsf-hJqIc",
	"Metadata_Roots_ITS2.xls": "1m0ahdB3KhJKneJ36dKwr474jEZjinH0q"
	
    }
    
    all_data = {}

    for filename, file_id in FILE_IDS.items():
        print(f"--- Обработка файла: {filename} ---")
        raw_data = download_and_load_data(file_id, filename)
        
        if raw_data is not None:
            all_data[filename] = raw_data
            
            print(f"\nРезультат для файла '{filename}':")
            print(raw_data.head(5))
            print("------------------------------------------")
