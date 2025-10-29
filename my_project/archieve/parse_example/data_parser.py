#!/usr/bin/env python3

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

URL = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
OUT_DIR = "processed"
OUT_FILE = os.path.join(OUT_DIR, "population.csv")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}

def fetch_page_content(url):
    """Функция загружает HTML-содержимое страницы."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def parse_population_table(html_content):
    """
    Функция парсит HTML с помощью BeautifulSoup, находит таблицу с населением
    и преобразует её в DataFrame.
    """
    if html_content is None:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable'})

    if not table:
        return None

    header_row = table.find('tr')
    headers = [th.get_text(strip=True) for th in header_row.find_all('th')]

    all_rows = []

    data_rows = table.find('tbody').find_all('tr')[1:]
    
    for row in data_rows:
        cells = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
        if cells:
            all_rows.append(cells)

    df = pd.DataFrame(all_rows, columns=headers)
    return df

def clean_data(df):
    """
    Функция очищает и форматирует DataFrame.
    """
    if df is None:
        return None

    df = df.head(100).copy()

    df.columns = [re.sub(r'\[\w+\]', '', col).strip() for col in df.columns]

    population_col = None
    for col in df.columns:
        if "population" in col.lower():
            population_col = col
            break

    if population_col:
        df[population_col] = df[population_col].astype(str).str.replace(r'[,\s]', '', regex=True)
        df[population_col] = pd.to_numeric(df[population_col], errors='coerce').astype('Int64')
    else:
        print("Колонка с населением не найдена.")

    df = df.applymap(lambda x: re.sub(r'\[\w+\]', '', str(x)).strip() if isinstance(x, str) else x)

    return df


def save_to_csv(df, path):
    """ Функция сохраняет DataFrame в CSV файл."""
    if df is None:

        return

    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)


def main():
    """Главная функция, управляющая всем процессом."""
    html = fetch_page_content(URL)
    df_raw = parse_population_table(html)
    df_clean = clean_data(df_raw)
    
    if df_clean is not None:
        print("\nРезультат:")
        print(df_clean.head(10).to_string())
        save_to_csv(df_clean, OUT_FILE)

if __name__ == "__main__":
    main()
