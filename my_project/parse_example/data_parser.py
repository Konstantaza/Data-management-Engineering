#!/usr/bin/env python3

#!/usr/bin/env python3
import os
import requests
import pandas as pd

url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population"
out_dir = "processed"
out_file = os.path.join(out_dir, "population.csv")

def main():
    # делаем запрос с заголовком User-Agent (иначе часто 403)
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    }
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        print("Ошибка при запросе страницы:", e)
        return

    # передаём HTML текст в pandas.read_html()
    try:
        tables = pd.read_html(resp.text)
    except Exception as e:
        print("Не удалось распарсить HTML:", e)
        return


    df = tables[0].copy()
    df = df.head(100)


    for col in df.columns:
        if "population" in str(col).lower():
            df[col] = df[col].astype(str).str.replace(r"[,\u00A0]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(out_file, index=False)

    print(df.head(10).to_string(index=False))
    print("----------------------------------")
    print("Saved to", out_file)

main()
