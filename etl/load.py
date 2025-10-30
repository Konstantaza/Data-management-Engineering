# etl/load.py
import os
import pandas as pd
from sqlalchemy import create_engine, exc

YOUR_LAST_NAME = "iamshchikov"
CREDS_TABLE_NAME = "access"


def get_creds_from_sqlite(db_path, table_name):
    """
    Подключается к 'creds.db' (SQLite) и читает креды для Postgres.
    """
    print(f"  [Load] Читаем учетные данные из {db_path}...")
    try:
        sqlite_engine = create_engine(f"sqlite:///{db_path}")

        with sqlite_engine.connect() as conn:
            creds_df = pd.read_sql(
                f'SELECT url as host, port, "user", "pass" as password FROM {table_name}',
                conn,
            )

        if creds_df.empty:
            print(f"  [Load Ошибка] Таблица '{table_name}' в {db_path} пуста.")
            return None

        creds = creds_df.iloc[0].to_dict()

        required_keys = ["host", "port", "user", "password"]
        if not all(key in creds for key in required_keys):
            print(
                f"  [Load Ошибка] В creds.db не хватает данных. Нужны: {required_keys}"
            )
            return None

        print("  [Load] Учетные данные для Postgres успешно прочитаны.")
        return creds

    except Exception as e:
        print(f"  [Load Ошибка] Ошибка при чтении {db_path}: {e}")
        return None


def load_to_postgres(df: pd.DataFrame, creds: dict, table_name: str):
    """
    Загружает DataFrame в PostgreSQL (первые 100 строк).
    """
    engine = None
    try:
        db_url = (
            f"postgresql+psycopg2://{creds['user']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/homeworks"
        )
        engine = create_engine(db_url)

        with engine.connect() as conn:
            print("  [Load] Проверка подключения к PostgreSQL... Успешно.")

        # Берем только 100 строк, как требует задание
        df_to_load = df.head(100)

        df_to_load.to_sql(
            name=table_name,
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
        )

        print(f"  [Load] Первые 100 строк успешно загружены в таблицу '{table_name}'.")
        return True

    except exc.OperationalError as e:
        print(f"  [Load Ошибка] Ошибка подключения к PostgreSQL: {e}")
        return False
    except Exception as e:
        print(f"  [Load Ошибка] Неизвестная ошибка при загрузке в PostgreSQL: {e}")
        return False
    finally:
        if engine:
            engine.dispose()


def save_as_parquet(df: pd.DataFrame, file_name: str, processed_dir: str):
    """
    Сохраняет DataFrame в .parquet в папку processed.
    """
    try:
        # Строим путь от processed_dir
        parquet_path = os.path.join(processed_dir, file_name + ".parquet")
        df.to_parquet(parquet_path, index=False)
        print(f"  [Load] Полные данные сохранены в: {parquet_path}")
        return True
    except Exception as e:
        print(f"  [Load Ошибка] Не удалось сохранить {parquet_path}: {e}")
        return False


def load_all_data(transformed_data: dict[str, pd.DataFrame], root_dir: str):
    """
    Главная функция модуля load.
    """
    print("--- 3. Этап загрузки (LOAD) ---")

    processed_data_dir = os.path.join(root_dir, "my_project", "data", "processed")
    sqlite_db_path = os.path.join(root_dir, "my_project", "creds.db")

    os.makedirs(processed_data_dir, exist_ok=True)

    # Получаем креды по правильному пути
    creds = get_creds_from_sqlite(sqlite_db_path, CREDS_TABLE_NAME)
    if creds is None:
        print("  [Load Ошибка] Не удалось получить креды. Загрузка в БД отменена.")
        db_ready = False
    else:
        db_ready = True

    # Проходим по всем трансформированным данным
    for file_name, df in transformed_data.items():
        print(f"  [Load] Обработка: {file_name}")

        # Валидация
        if df.empty:
            print(
                f"  [Load Предупреждение] DataFrame {file_name} пуст. Валидация не пройдена. Пропускаем."
            )
            continue  # Пропускаем этот файл и переходим к следующему

        # Сохраняем .parquet
        save_as_parquet(df, file_name, processed_data_dir)

        # Загружаем в БД
        if "count" in df.columns and db_ready:
            print(f"  [Load] Загружаем {file_name} в PostgreSQL...")
            table_name = file_name.lower().replace("dada2_clean_asv_counts_", "")

            if "roots" in table_name:
                load_to_postgres(df, creds, YOUR_LAST_NAME)
            else:
                print(
                    f"  [Load] Пропускаем загрузку {file_name} в БД (загружаем только 'roots')."
                )

        elif "count" not in df.columns and db_ready:
            print(
                f"  [Load] Пропускаем загрузку {file_name} в БД (это не 'counts' файл)."
            )

    print("--- Этап загрузки завершен ---\n")


if __name__ == "__main__":
    print("Запущен etl/load.py как самостоятельный скрипт.")
