#!/usr/bin/env python3
import os
import requests
import pandas as pd

api_url = "https://v2.jokeapi.dev/joke/Any?amount=10"   # публичный JSON API
out_dir = "processed"
out_csv = os.path.join(out_dir, "jokes.csv")

def main():
    resp = requests.get(api_url, timeout=10)
    data = resp.json()

    jokes = data.get("jokes")
    if jokes is None:
        jokes = [data]

    rows = []
    for j in jokes:
        row = {
            "id": j.get("id"),
            "category": j.get("category"),
            "type": j.get("type"),
            "lang": j.get("lang"),
            "safe": j.get("safe"),
        }
        if j.get("type") == "single":
            row["joke"] = j.get("joke")
            row["setup"] = None
            row["delivery"] = None
        else:
            row["joke"] = None
            row["setup"] = j.get("setup")
            row["delivery"] = j.get("delivery")
        rows.append(row)

    df = pd.DataFrame(rows)
    print(df.head(10).to_string(index=False))


    os.makedirs(out_dir, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print("----------------------------")
    print("Saved .csv to", out_csv)


main()
