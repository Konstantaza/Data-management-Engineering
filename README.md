# Data-management-Engineering

Этот репозиторий содержит основной ETL-пайплайн для извлечения, трансформации и загрузки данных.

## Источник данных

Все исходные "сырые" данные для этого проекта хранятся на Google Drive.

**Ссылка на Google Drive:** [https://drive.google.com/drive/folders/1Azl5tjFB47B9aBVfpBObkG4WG7ohekdS?usp=sharing](https://drive.google.com/drive/folders/1Azl5tjFB47B9aBVfpBObkG4WG7ohekdS?usp=sharing)

---

## Основной ETL-пайплайн

Пакет `etl` представляет собой конвейер для извлечения, трансформации и загрузки набора геномных данных.

### Структура пакета `etl/`

```
Data-management-Engineering/
│
├── etl/
│   ├── __init__.py
│   ├── extract.py     # Extract from GDrive
│   ├── load.py        # Load to DB
│   ├── main.py        # The main executable file
│   └── transform.py   # Type conversion
│
├── my_project/        # Working directory
│   ├── data          
│   │   ├── processed
│   │   └── raw
│   ├── notebooks
│   │    └── EDA.ipynb    
│   ├── poetry.lock
│   ├── pyproject.toml
│   │
│   └── archieve
│       ├── api_example
│       │   ├── api_reader.py
│       │   └── processed
│       │       └── jokes.csv
│       └── parse_example
│           ├── data_parser.py
│           └── processed
│               └── population.csv
├── .gitignore
└── README.md
```

### Инструкция по запуску

Для запуска полного ETL-процесса необходимо:

1.  Установить зависимости в `my_project` (используя `poetry install`).
2.  Активировать `conda` окружение (`conda activate my_env`).
3.  Перейти в папку `my_project`, так как в ней находятся библиотеки, данные и файл `creds.db`.
4.  Запустить `main.py` (который лежит снаружи) с помощью `poetry run`.

```bash
# Находясь в корне репозитория, перейдите в my_project
cd my_project

# Запустите ETL-пайплайн (все шаги)
poetry run python ../etl/main.py --step all
```

### Дополнительные примеры
В папке `my_project/archive` находятся дополнительные скрипты, демонстрирующие:
1. `api_example`: Пример работы с API (получение данных о шутках).
2. `parse_example`: Пример парсинга HTML-таблиц с веб-страниц.

