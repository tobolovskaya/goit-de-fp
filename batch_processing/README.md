# Batch Data Lake Pipeline

Частина 2 фінального проєкту з курсу Data Engineering - Building an End-to-End Batch Data Lake

## Опис проєкту

Цей проєкт реалізує multi-hop data lake архітектуру для обробки даних про олімпійських атлетів. Pipeline складається з трьох рівнів обробки даних:

- **Landing Zone**: Завантаження сирих даних з FTP сервера
- **Bronze Layer**: Збереження даних у форматі Parquet
- **Silver Layer**: Очищення та дедублікація даних
- **Gold Layer**: Агреговані аналітичні дані

## Архітектура рішення

```
FTP Server → Landing → Bronze → Silver → Gold
    ↓           ↓        ↓        ↓       ↓
  CSV files   CSV    Parquet  Cleaned  Analytics
                              Parquet   Parquet
```

## Структура проєкту

```
batch_processing/
├── landing_to_bronze.py      # Завантаження з FTP та конвертація в Parquet
├── bronze_to_silver.py       # Очищення тексту та дедублікація
├── silver_to_gold.py         # Агрегація та аналітика
├── project_solution.py       # Airflow DAG для оркестрації
├── test_pipeline_locally.py  # Локальне тестування pipeline
├── config.py                 # Конфігурація проєкту
├── utils.py                  # Допоміжні функції
└── README.md                 # Документація
```

## Етапи обробки даних

### 1. Landing to Bronze (`landing_to_bronze.py`)

**Завдання:**
- Завантаження CSV файлів з FTP сервера
- Конвертація в формат Parquet
- Додавання timestamp завантаження

**Джерела даних:**
- `https://ftp.goit.study/neoversity/athlete_bio.csv`
- `https://ftp.goit.study/neoversity/athlete_event_results.csv`

**Вихід:**
- `data/bronze/athlete_bio/` (Parquet)
- `data/bronze/athlete_event_results/` (Parquet)

### 2. Bronze to Silver (`bronze_to_silver.py`)

**Завдання:**
- Очищення текстових колонок від спеціальних символів
- Видалення дублікатів
- Додавання timestamp обробки

**Функція очищення:**
```python
def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,."\']', '', str(text))
```

**Вихід:**
- `data/silver/athlete_bio/` (Parquet)
- `data/silver/athlete_event_results/` (Parquet)

### 3. Silver to Gold (`silver_to_gold.py`)

**Завдання:**
- Join таблиць за `athlete_id`
- Розрахунок середніх значень `weight` та `height`
- Групування за: `sport`, `medal`, `sex`, `country_noc`
- Додавання timestamp розрахунків

**Вихід:**
- `data/gold/avg_stats/` (Parquet)

### 4. Airflow DAG (`project_solution.py`)

**Завдання:**
- Оркестрація всіх етапів pipeline
- Використання `SparkSubmitOperator`
- Налаштування залежностей між задачами

## Локальне тестування

### Встановлення залежностей

```bash
pip install pyspark requests
```

### Запуск повного pipeline

```bash
python batch_processing/test_pipeline_locally.py
```

### Запуск окремих етапів

```bash
# Етап 1: Landing to Bronze
python batch_processing/landing_to_bronze.py

# Етап 2: Bronze to Silver
python batch_processing/bronze_to_silver.py

# Етап 3: Silver to Gold
python batch_processing/silver_to_gold.py
```

## Розгортання в Airflow

### 1. Підготовка файлів

Скопіюйте всі файли з `batch_processing/` в директорію `dags/` вашого Airflow репозиторію:

```
airflow_sandbox/
└── dags/
    └── batch_processing/
        ├── landing_to_bronze.py
        ├── bronze_to_silver.py
        ├── silver_to_gold.py
        └── project_solution.py
```

### 2. Конфігурація Spark Connection

Переконайтеся, що в Airflow налаштований connection `spark-default`.

### 3. Активація DAG

DAG `batch_data_lake_pipeline` буде автоматично виявлений Airflow і доступний для запуску.

## Особливості реалізації

### Обробка помилок
- Валідація завантажених даних
- Перевірка наявності обов'язкових колонок
- Логування всіх етапів обробки

### Оптимізація Spark
- Adaptive Query Execution
- Coalesce Partitions
- Skew Join Optimization

### Якість даних
- Фільтрація некоректних значень weight/height
- Обробка null значень медалей
- Дедублікація записів

### Моніторинг
- Детальне логування кожного етапу
- Підрахунок кількості записів
- Відображення схеми даних

## Структура даних

### Athlete Bio
- `athlete_id`: Унікальний ідентифікатор атлета
- `height`: Зріст (см)
- `weight`: Вага (кг)
- `sex`: Стать (M/F)

### Athlete Event Results
- `athlete_id`: Унікальний ідентифікатор атлета
- `sport`: Вид спорту
- `medal`: Тип медалі (Gold/Silver/Bronze/null)
- `country_noc`: Код країни

### Gold Layer Output
- `sport`: Вид спорту
- `medal`: Тип медалі або "No Medal"
- `sex`: Стать
- `country_noc`: Код країни
- `avg_weight`: Середня вага
- `avg_height`: Середній зріст
- `timestamp`: Час розрахунку

## Troubleshooting

### Помилки завантаження з FTP
- Перевірте доступність FTP сервера
- Переконайтеся в стабільності інтернет-з'єднання

### Помилки Spark
- Переконайтеся, що достатньо пам'яті
- Перевірте права доступу до директорій

### Помилки Airflow
- Перевірте конфігурацію `spark-default` connection
- Переконайтеся, що файли знаходяться в правильній директорії

## Автори

Проєкт розроблено в рамках курсу Data Engineering GoIT.