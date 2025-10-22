# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, exc

DATA_XLS_FILE = 'DADA2_clean_ASV_counts_ITS2_Roots.xls'
YOUR_LAST_NAME = 'iamshchikov' 


def get_credentials_from_env():
    """
    Читает учетные данные из переменных среды.
    Прерывает выполнение (assert), если переменные не найдены.
    """
    host = os.environ.get('PG_HOST')
    port = os.environ.get('PG_PORT')
    user = os.environ.get('PG_USER')
    password = os.environ.get('PG_PASS')
    dbname = os.environ.get('PG_DBNAME')
    
    # Проверяем, что все переменные установлены 
    assert host, "Ошибка: Переменная среды 'PG_HOST' не установлена!"
    assert port, "Ошибка: Переменная среды 'PG_PORT' не установлена!"
    assert user, "Ошибка: Переменная среды 'PG_USER' не установлена!"
    assert password, "Ошибка: Переменная среды 'PG_PASS' не установлена!"
    assert dbname, "Ошибка: Переменная среды 'PG_DBNAME' не установлена!"
    
    print("Учетные данные успешно прочитаны.")
    
    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'dbname': dbname
    }


def prepare_data(xls_file):
    """
    Загружает данные из Excel и приводит их к "длинному" формату.
    """
    try:
        # Используем pd.read_excel и указываем xlrd
        df = pd.read_excel(xls_file, index_col=0, engine='xlrd')
        
        df.index.name = 'asv_id'
        df = df.reset_index()

        # "Плавление" (Melt)
        df_melted = df.melt(
            id_vars=['asv_id'], 
            var_name='sample_id', 
            value_name='count'
        )
        
        assert not df_melted.empty, "Ошибка: DataFrame пуст после подготовки."
        
        print(f"Данные подготовлены. Получилось {len(df_melted)} строк.")
        return df_melted
        
    except FileNotFoundError:
        print(f"Ошибка: Файл данных не найден по пути: {xls_file}")
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
        print("ПОЖАЛУЙСТА, ПРОВЕРЬТЕ УЧЕТНЫЕ ДАННЫЕ В ПЕРЕМЕННЫХ СРЕДЫ.")
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
        # Читаем секреты из переменных среды
        pg_creds = get_credentials_from_env()

        # Готовим данные из Excel
        data_df = prepare_data(DATA_XLS_FILE)
        
        assert data_df is not None, "Ошибка: Не удалось подготовить данные."

        # Загружаем данные в PostgreSQL
        # Какая БД будет использована,
        # зависит от того, что установлено в переменной PG_DBNAME
        success = load_to_postgres(data_df, pg_creds, YOUR_LAST_NAME)
        
        if success:
            print("Скрипт успешно завершен")
        else:
            print("Скрипт завершен с ошибками")
            sys.exit(1)
            
    except AssertionError as e:
        # Отлавливаем ошибки assert
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА] Проверка не пройдена: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Произошла непредвиденная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

