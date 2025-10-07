# api_example

**API:** JokeAPI — случайные шутки  
**Ссылка:** https://v2.jokeapi.dev/joke/Any?amount=10

## Описание
Простой пример: `api_reader.py` делает HTTP-запрос к API, преобразует результат в `pandas.DataFrame`, печатает первые 10 строк и сохраняет CSV в `api_example/processed/jokes.csv`.

## Как запустить
1. Активировать окружение (пример conda):
   ```bash
   conda activate data_eng
   ```
2. Установить зависимости (если ещё не установлены):
   ```bash
   pip install requests pandas
   ```
   или с poetry:
   ```bash
   poetry add requests pandas
   ```
3. Запустить:
   ```bash
   python3 api_reader.py
   ```
4. Сохраненный .csv файл в папке processed 
