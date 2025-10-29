# parse_example

**Источник:** Wikipedia — List of countries and dependencies by population  
**Ссылка:** https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population

## Описание
`data_parser.py` скачивает HTML-страницу Википедии, берёт первую таблицу на странице, ограничивает её первыми 100 строками, пытается привести колонку с населением к числовому виду и сохраняет результат в CSV.

Файл-скрипт: `data_parser.py`  
Результат (локально): `parse_example/processed/population.csv`  


## Как запустить
1. Активировать окружение (пример conda):
   ```bash
   conda activate data_eng
   ```
2. Установить зависимости (если ещё не установлены):
   ```bash
   pip install requests pandas beautifulsoup4 lxml
   ```
3. Запустить парсер:
   ```bash
   cd my_project/parse_example
   python3 data_parser.py
   ```
4. Файл CSV появится по пути:
   ```bash
   parse_example/processed/population.csv
   ```
