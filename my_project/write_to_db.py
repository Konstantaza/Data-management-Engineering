# -*- coding: utf-8 -*-
import sys
import pandas as pd
from sqlalchemy import create_engine, exc

DATA_XLS_FILE = 'my_project/data/processed/DADA2_clean_ASV_counts_ITS2_Roots.parquet'
YOUR_LAST_NAME = 'iamshchikov'
SQLITE_DB_PATH = 'my_project/creds.db' 


def get_creds_from_sqlite(db_path):
    """
    Подключается к 'creds.db' (SQLite) и читает креды для Postgres.
    """
    print(f"Читаем учетные данные из {db_path}...")
    try:
        # Создаем подключение к SQLite
        sqlite_engine = create_engine(f'sqlite:///{db_path}')
        
        # Читаем данные из таблицы 
        with sqlite_engine.connect() as conn:
            creds_df = pd.read_sql("SELECT * FROM homeworks", conn) 
        
        # Берем первую строку и превращаем ее в словарь
        if creds_df.empty:
            print("Ошибка: Таблица 'homeworks' в creds.db пуста.")
            return None
            
        creds = creds_df.iloc[0].to_dict()
        
        # Проверяем, что все нужные ключи на месте
        required_keys = ['host', 'port', 'user', 'password', 'dbname']
        if not all(key in creds for key in required_keys):
            print(f"Ошибка: в creds.db не хватает данных. Нужны: {required_keys}")
            return None
            
        print("Учетные данные для Postgres успешно прочитаны из SQLite.")
        return creds
        
    except FileNotFoundError:
        print(f"Ошибка: Файл {db_path} не найден.")
        return None
    except Exception as e:
        print(f"Ошибка при чтении creds.db: {e}")
        return None


def prepare_data(data_file_path):
    """
    Загружает данные (CSV или Parquet) и приводит их к "длинному" формату.
    """
    try:
        print(f"Читаем файл: {data_file_path}")
        # Читаем Parquet
        if data_file_path.endswith(".parquet"):
            df = pd.read_parquet(data_file_path)
        else:
            print('Ошибка в чтении файла .parquet')

        # Длинный формат данных
        if 'asv_id' not in df.columns:
             df.index.name = 'asv_id'
             df = df.reset_index()

        df_melted = df.melt(
            id_vars=['asv_id'], 
            var_name='sample_id', 
            value_name='count'
        )
        
        
        assert not df_melted.empty, "Ошибка: DataFrame пуст после подготовки."
        print(f"Данные подготовлены. Получилось {len(df_melted)} строк.")
        return df_melted
        
    except FileNotFoundError:
        print(f"Ошибка: Файл данных не найден по пути: {data_file_path}")
        return None
    except Exception as e:
        print(f"Ошибка при подготовке данных: {e}")
        return None


def load_to_postgres(df, creds, table_name):
    """
    Загружает DataFrame в PostgreSQL, используя учетные данные.
    Берет только первые 100 строк.
    """
    engine = None
    try:
        # Собираем строку подключения
        db_url = (
            f"postgresql+psycopg2://{creds['user']}:{creds['password']}"
            f"@{creds['host']}:{creds['port']}/{creds['dbname']}"
        )
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("Проверка подключения к PostgreSQL... Успешно.")

        df_to_load = df.head(100)
        df_to_load.to_sql(
            name=table_name,
            con=engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        
        print("="*30)
        print(f"Данные успешно загружены в таблицу '{table_name}'.")
        print(f"БД: {creds['dbname']}")
        print("="*30)
        return True

    except exc.OperationalError as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        return False
    except Exception as e:
        print(f"Неизвестная ошибка при загрузке данных в PostgreSQL: {e}")
        return False
    finally:
        if engine:
            engine.dispose()


def main():
    """
    Главная функция, запускающая весь процесс.
    """
    try:
        # Читаем секреты из SQLite
        pg_creds = get_creds_from_sqlite(SQLITE_DB_PATH)
        assert pg_creds is not None, "Ошибка: Не удалось прочитать креды из SQLite."


        # Готовим данные
        data_df = prepare_data(DATA_XLS_FILE) 
        assert data_df is not None, "Ошибка: Не удалось подготовить данные."

        # Загружаем данные в PostgreSQL
        success = load_to_postgres(data_df, pg_creds, YOUR_LAST_NAME)
        
        if success:
            print("Скрипт успешно завершен")
        else:
            print("Скрипт завершен с ошибками")
            sys.exit(1)
            
    except AssertionError as e:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА] Проверка не пройдена: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()