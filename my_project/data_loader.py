#!/usr/bin/env python3
# data_loader.py
"""
Скачивает Excel-файлы с Google Drive, читает их в pandas DataFrame,
попыточно приводит колонки к подходящим типам (numeric, datetime, bool, category)
и сохраняет результаты в CSV и (если доступно) Parquet.
"""

import os
import json
import pandas as pd
import gdown
from typing import Tuple, Dict, Any

def download_file_from_gdrive(file_id: str, output_path: str, overwrite: bool = False) -> bool:
    """
    Скачивает файл с Google Drive по file_id в output_path.
    Если файл уже существует и overwrite=False — пропускает скачивание.
    Возвращает True при успехе (файл существует на диске).
    """
    if os.path.exists(output_path) and not overwrite:
        print(f"Файл '{output_path}' уже существует — пропускаем скачивание.")
        return True

    url = f"https://drive.google.com/uc?id={file_id}"
    print(f"Скачивание: {url} -> {output_path}")
    try:
        gdown.download(url, output_path, quiet=False)
    except Exception as e:
        print(f"[ERROR] gdown.download: {e}")
        return False

    if not os.path.exists(output_path):
        print(f"[ERROR] Файл не найден после попытки скачивания: {output_path}")
        return False

    return True

def try_parse_bool_series(s: pd.Series) -> Tuple[pd.Series, bool]:
    """
    Попытка интерпретировать серию как булевую.
    Поддерживает значения: true/false, yes/no, y/n, 0/1 (строки/числа).
    Возвращает (преобразованная_серия, True) если получилось, иначе (исходная, False).
    """
    if s.dropna().empty:
        return s, False

    mapping = {
        'true': True, 'false': False,
        't': True, 'f': False,
        'yes': True, 'no': False,
        'y': True, 'n': False,
        '1': True, '0': False
    }

    # Normalize to lowercase string
    def map_val(v):
        if pd.isna(v):
            return None
        if isinstance(v, bool):
            return v
        vs = str(v).strip().lower()
        if vs in mapping:
            return mapping[vs]
        return mapping.get(vs) if vs in mapping else None

    mapped = s.map(map_val)
    non_null = mapped.notna().sum()
    frac = non_null / max(1, len(s))
    # Если >= 80% значений удалось преобразовать -> считаем колонку булевой
    if frac >= 0.8:
        return mapped.astype('boolean'), True
    return s, False

def infer_and_cast_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Для каждой колонки пытаемся:
    1) привести к числовому (float/int), если parseable >= 80%
    2) иначе попробовать parse datetime (>= 60%)
    3) иначе попробовать булев (>= 80%)
    4) иначе привести к category, если уникальных значений мало
    Возвращает (новый DataFrame, отчет словарь по колонкам)
    """
    report = {}
    df = df.copy()
    n_rows = len(df)
    for col in df.columns:
        ser = df[col]
        orig_dtype = str(ser.dtype)
        col_report = {"original_dtype": orig_dtype}
        # Skip if empty
        if ser.dropna().empty:
            col_report.update({"action": "empty_column", "new_dtype": orig_dtype})
            report[col] = col_report
            continue

        # 1) Попробовать числовой
        coerced_num = pd.to_numeric(ser, errors='coerce')
        num_non_null = coerced_num.notna().sum()
        if num_non_null / n_rows >= 0.8:
            # если все целые — привести к Int64, иначе float
            if (coerced_num.dropna() % 1 == 0).all():
                new_ser = coerced_num.astype('Int64')  # nullable integer
                new_type = 'Int64'
            else:
                new_ser = coerced_num.astype('float64')
                new_type = 'float64'
            df[col] = new_ser
            col_report.update({"action": "to_numeric", "new_dtype": new_type,
                               "numeric_fraction": round(num_non_null / n_rows, 3)})
            report[col] = col_report
            continue

        # 2) Попробовать datetime
        coerced_dt = pd.to_datetime(ser, errors='coerce', infer_datetime_format=True)
        dt_non_null = coerced_dt.notna().sum()
        if dt_non_null / n_rows >= 0.6:
            df[col] = coerced_dt
            col_report.update({"action": "to_datetime", "new_dtype": "datetime64[ns]",
                               "datetime_fraction": round(dt_non_null / n_rows, 3)})
            report[col] = col_report
            continue

        # 3) Попробовать boolean
        bool_ser, ok_bool = try_parse_bool_series(ser)
        if ok_bool:
            df[col] = bool_ser
            col_report.update({"action": "to_boolean", "new_dtype": "boolean"})
            report[col] = col_report
            continue

        # 4) Попробовать category если мало уникальных значений
        nunique = ser.nunique(dropna=True)
        if nunique <= max(50, 0.05 * n_rows):
            df[col] = ser.astype('category')
            col_report.update({"action": "to_category", "new_dtype": "category",
                               "nunique": int(nunique)})
            report[col] = col_report
            continue

        # ничего не сделали
        col_report.update({"action": "leave", "new_dtype": orig_dtype, "nunique": int(nunique)})
        report[col] = col_report

    return df, report

def read_table(path: str) -> pd.DataFrame:
    """
    Читает файл .csv/.xls/.xlsx по расширению. Для .xls/.xlsx использует соответствующие движки.
    """
    ext = os.path.splitext(path)[1].lower()
    try:
        if ext == ".csv":
            return pd.read_csv(path)
        elif ext in (".xls", ".xlsx"):
            engine = "xlrd" if ext == ".xls" else "openpyxl"
            return pd.read_excel(path, engine=engine)
        else:
            # Попробуем по очереди csv, затем excel
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.read_excel(path)
    except Exception as e:
        print(f"[ERROR] Не удалось прочитать {path}: {e}")
        raise

def save_with_formats(df: pd.DataFrame, out_base: str, save_csv: bool = True, save_parquet: bool = True) -> Dict[str, str]:
    """
    Сохраняет DataFrame в CSV и по возможности в Parquet.
    Возвращает словарь с путями сохранённых файлов (если сохранены).
    """
    saved = {}
    if save_csv:
        out_csv = out_base + ".csv"
        try:
            # Сохраняем CSV без индекса
            df.to_csv(out_csv, index=False)
            saved['csv'] = out_csv
            print(f"[INFO] Сохранён CSV: {out_csv}")
        except Exception as e:
            print(f"[WARN] Не удалось сохранить CSV {out_csv}: {e}")

    if save_parquet:
        out_parquet = out_base + ".parquet"
        try:
            # Попытка сохранить parquet (pyarrow/fastparquet должны быть установлены)
            df.to_parquet(out_parquet, index=False)
            saved['parquet'] = out_parquet
            print(f"[INFO] Сохранён Parquet: {out_parquet}")
        except Exception as e:
            print(f"[WARN] Не удалось сохранить Parquet ({out_parquet}): {e}")
            print("       Установите 'pyarrow' или 'fastparquet' если хотите сохранить в Parquet.")
    return saved

def save_schema_report(report: Dict[str, Any], out_base: str) -> str:
    """
    Сохраняет JSON с отчетом по преобразованиям типов.
    """
    out_json = out_base + "_schema.json"
    try:
        with open(out_json, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2, ensure_ascii=False)
        print(f"[INFO] Сохранён отчет по типам: {out_json}")
    except Exception as e:
        print(f"[WARN] Не удалось сохранить schema report: {e}")
        out_json = ""
    return out_json

def download_and_load_data(file_id: str, output_path: str, overwrite: bool = False) -> pd.DataFrame:
    if not download_file_from_gdrive(file_id, output_path, overwrite=overwrite):
        return None
    return read_table(output_path)

if __name__ == "__main__":
    # Вставьте ваши ID как в предыдущей версии
    FILE_IDS = {
        "ASVs_ITS2_sequences.xls": "1Ej41HIklJDCaaaJd6oGMrMiDwa9bgBED",
        "DADA2_clean_ASV_counts_ITS2_DPS.xls": "17CI7XNtneBPPJ3gHu3cPBw4pVitqTQuo",
        "DADA2_clean_ASV_counts_ITS2_Roots.xls": "1EfyjuGIFWWPfefwcPfb5-83qT21ozV-r",
        "DADA2_clean_ASV_taxonomy_ITS2_DPS.xls": "1RaVYEuaN6-tbNsMmCoeefunh7-MPQx2I",
        "DADA2_clean_ASV_taxonomy_ITS2_Roots.xls": "1A_TUJR031BX7WVoXgcr6-1cKG33ov6aU",
        "Metadata_DPS_ITS2.xls": "13T5frkJwgMUTzWCway3UCJ8hsf-hJqIc",
        "Metadata_Roots_ITS2.xls": "1m0ahdB3KhJKneJ36dKwr474jEZjinH0q"
    }

    out_dir = "processed"
    os.makedirs(out_dir, exist_ok=True)

    all_data = {}
    for filename, file_id in FILE_IDS.items():
        print(f"\n=== Обработка файла: {filename} ===")
        # скачиваем в рабочую папку (оригинальное имя)
        if not download_file_from_gdrive(file_id, filename, overwrite=False):
            print(f"[ERROR] Пропускаем файл {filename}")
            continue

        try:
            raw_df = read_table(filename)
        except Exception as e:
            print(f"[ERROR] Чтение файла {filename} не удалось: {e}")
            continue

        print(f"[INFO] Прочитано {len(raw_df)} строк, {len(raw_df.columns)} колонок.")
        # Приведение типов
        casted_df, report = infer_and_cast_types(raw_df)
        # Сохранение отчёта по типам
        base = os.path.join(out_dir, os.path.splitext(filename)[0])
        save_schema_report(report, base)
        # Сохранение датасета
        saved = save_with_formats(casted_df, base, save_csv=True, save_parquet=True)

        # Показываем первые 10 строк преобразованного датафрейма
        print(casted_df.head(10))
        all_data[filename] = {
            "df": casted_df,
            "report": report,
            "saved": saved
        }

    # Опционально: сохранить сводку всех файлов
    summary_path = os.path.join(out_dir, "summary_files.json")
    try:
        summary = {fn: {"saved": all_data[fn]["saved"], "n_rows": len(all_data[fn]["df"]),
                        "n_cols": len(all_data[fn]["df"].columns)} for fn in all_data}
        with open(summary_path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)
        print(f"\n[INFO] Сводка сохранена: {summary_path}")
    except Exception as e:
        print(f"[WARN] Не удалось сохранить сводку: {e}")

