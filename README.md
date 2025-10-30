# Data-management-Engineering

Этот репозиторий содержит основной ETL-пайплайн для извлечения, трансформации и загрузки данных.

## Источник данных

Все исходные "сырые" данные для этого проекта хранятся на Google Drive.

**Ссылка на Google Drive:** [https://drive.google.com/drive/folders/1Azl5tjFB47B9aBVfpBObkG4WG7ohekdS?usp=sharing](https://drive.google.com/drive/folders/1Azl5tjFB47B9aBVfpBObkG4WG7ohekdS?usp=sharing)

---

## Основной ETL-пайплайн

Пакет `etl` представляет собой конвейер для извлечения, трансформации и загрузки набора геномных данных.

### Структура пакета `etl/`

* `etl/__init__.py`: Обозначает `etl` как Python-пакет.
* `etl/extract.py`: **(Extract)** Отвечает за извлечение данных. Скачивает `.xls` файлы из Google Drive, проводит валидацию (проверяет, что файл не пуст), конвертирует их в `.csv` и сохраняет в `my_project/data/raw`.
* `etl/transform.py`: **(Transform)** Отвечает за трансформацию. Читает `.csv` из `my_project/data/raw`, приводит типы данных (`convert_dtypes`), "плавит" (melt) таблицы с подсчетами в длинный формат.
* `etl/load.py`: **(Load)** Отвечает за загрузку. Читает креды из SQLite (`my_project/creds.db`) для подключения к Postgres. Проводит валидацию (проверяет, что DataFrame после трансформации не пуст). Сохраняет полные данные в `my_project/data/processed` в формате `.parquet`. Загружает 100 строк в таблицу Postgres.
* `etl/main.py`: **(Точка входа)** Собирает все модули вместе. Предоставляет CLI-интерфейс (`--step`) для пошагового запуска пайплайна.

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