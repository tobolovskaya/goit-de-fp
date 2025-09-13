# Olympic Data Streaming Pipeline

Фінальний проєкт з курсу Data Engineering - Частина 1: Building an End-to-End Streaming Pipeline

## Опис проєкту

Цей проєкт реалізує потоковий pipeline для букмекерської контори, який обробляє дані про олімпійських атлетів для генерації features для ML-моделей. Pipeline читає фізичні дані атлетів з MySQL бази даних, отримує результати змагань через Kafka, об'єднує дані та розраховує статистики для кожного виду спорту.

## Архітектура рішення

```
MySQL (athlete_bio) → Spark → Join ← Kafka (athlete_event_results)
                                ↓
                         Data Processing
                                ↓
                    ┌─────────────────────┐
                    ↓                     ↓
            Kafka Topic              MySQL Table
        (enriched_athlete_data)  (enriched_athlete_stats)
```

## Структура проєкту

```
goit-de-fp/
├── config/
│   └── database_config.py      # Конфігурація БД та Kafka
├── src/
│   ├── data_reader.py          # Читання даних з MySQL та Kafka
│   ├── data_processor.py       # Обробка та трансформація даних
│   ├── data_writer.py          # Запис даних в Kafka та MySQL
│   └── streaming_pipeline.py   # Основний streaming pipeline
├── scripts/
│   ├── setup_mysql_table.sql   # SQL скрипт для створення таблиць
│   ├── download_mysql_connector.sh  # Завантаження JDBC драйвера
│   └── start_kafka.sh          # Запуск Kafka сервісів
├── tests/
│   └── test_data_processor.py  # Unit тести
├── requirements.txt            # Python залежності
├── .env.example               # Приклад конфігурації
├── .env                       # Конфігурація (не в git)
└── main.py                    # Точка входу в програму
```

## Встановлення та налаштування

### 1. Клонування репозиторію
```bash
git clone <repository-url>
cd goit-de-fp
```

### 2. Встановлення Python залежностей
```bash
pip install -r requirements.txt
```

### 3. Завантаження MySQL JDBC драйвера
```bash
chmod +x scripts/download_mysql_connector.sh
./scripts/download_mysql_connector.sh
```

### 4. Налаштування конфігурації
```bash
cp .env.example .env
# Відредагуйте .env файл з вашими credentials
```

### 5. Створення MySQL таблиці
```bash
mysql -h 217.61.57.46 -u your_username -p olympic_dataset < scripts/setup_mysql_table.sql
```

### 6. Запуск Kafka (якщо локально)
```bash
chmod +x scripts/start_kafka.sh
./scripts/start_kafka.sh
```

## Запуск pipeline

```bash
python main.py
```

## Основні компоненти

### 1. DataReader (`src/data_reader.py`)
- Читання даних атлетів з MySQL таблиці `athlete_bio`
- Фільтрація записів з валідними показниками зросту та ваги
- Читання результатів змагань з MySQL таблиці `athlete_event_results`
- Запис даних в Kafka топік
- Читання streaming даних з Kafka

### 2. DataProcessor (`src/data_processor.py`)
- Об'єднання даних з результатами змагань та біологічними даними
- Розрахунок середнього зросту та ваги по групах:
  - Вид спорту
  - Тип медалі (або її відсутність)
  - Стать
  - Країна (country_noc)
- Додавання timestamp розрахунків

### 3. DataWriter (`src/data_writer.py`)
- Запис даних в Kafka топік `enriched_athlete_data`
- Запис даних в MySQL таблицю `enriched_athlete_stats`
- Реалізація `forEachBatch` функції для обробки мікробатчів

### 4. StreamingPipeline (`src/streaming_pipeline.py`)
- Основний клас для управління pipeline
- Ініціалізація Spark сесії з необхідними конфігураціями
- Координація всіх компонентів
- Управління життєвим циклом streaming процесу

## Особливості реалізації

### Фільтрація даних
- Видалення записів з порожніми значеннями зросту та ваги
- Фільтрація нечислових значень
- Видалення записів з нульовими або від'ємними значеннями

### Streaming обробка
- Використання Spark Structured Streaming
- Обробка мікробатчів кожні 30 секунд
- Checkpoint для відновлення після збоїв
- FanOut патерн - запис в два destination одночасно

### Оптимізація
- Кешування статичних даних (athlete_bio)
- Адаптивне виконання запитів Spark
- Індексація MySQL таблиць для швидкого пошуку

## Тестування

Запуск unit тестів:
```bash
python -m pytest tests/ -v
```

## Моніторинг

Pipeline виводить детальні логи про:
- Кількість завантажених записів
- Результати фільтрації
- Статус обробки батчів
- Помилки та винятки

## Вимоги до системи

- Python 3.8+
- Apache Spark 3.5.0
- MySQL 8.0+
- Apache Kafka 2.8+
- Мінімум 4GB RAM для Spark

## Troubleshooting

### Помилки підключення до MySQL
- Перевірте credentials в `.env` файлі
- Переконайтеся, що MySQL JDBC драйвер завантажений
- Перевірте доступність хоста та порту

### Помилки Kafka
- Переконайтеся, що Kafka сервіси запущені
- Перевірте, що топіки створені
- Перевірте конфігурацію bootstrap servers

### Помилки Spark
- Переконайтеся, що достатньо пам'яті
- Перевірте права доступу до checkpoint директорії
- Перевірте наявність JDBC драйвера в classpath

## Автори

Проєкт розроблено в рамках курсу Data Engineering GoIT.