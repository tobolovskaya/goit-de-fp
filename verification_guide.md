# Керівництво з верифікації результатів

## Критерії прийняття та оцінювання

### ✅ Код виконується та надає бажаний результат
### ✅ Скриншоти демонструють результат роботи
### ✅ Коментарі позначають кожен етап завдання

---

## Частина 1: Streaming Pipeline

### Як запустити та перевірити результати:

1. **Запуск основного pipeline:**
```bash
python main.py
```

2. **Демонстрація результатів:**
```bash
python demo_results.py
```

### Що показати в скриншотах:

#### 🎯 **Обов'язкові скриншоти:**

1. **База даних MySQL - таблиця `enriched_athlete_stats`:**
   - Запит: `SELECT * FROM enriched_athlete_stats LIMIT 20;`
   - Показує результати агрегації з середніми значеннями

2. **Kafka топік - повідомлення з `enriched_athlete_data`:**
   - Використайте `demo_results.py` для показу даних з Kafka
   - Або Kafka console consumer для перегляду повідомлень

#### 📋 **Етапи з коментарями в коді:**

```python
# Етап 1: Зчитування фізичних показників атлетів з MySQL таблиці
# Етап 2: Фільтрація даних (видалення порожніх та нечислових значень)
# Етап 3.1: Зчитування даних з MySQL таблиці athlete_event_results для запису в Kafka
# Етап 3.2: Запис даних з MySQL в Kafka-топік athlete_event_results
# Етап 3.3: Зчитування streaming даних з Kafka-топіку
# Етап 4: Об'єднання даних за ключем athlete_id
# Етап 5: Розрахунок середніх значень
# Етап 6.а): Запис у вихідний Kafka-топік
# Етап 6.b): Запис у базу даних MySQL
```

---

## Частина 2: Batch Data Lake

### Як запустити та перевірити результати:

1. **Локальне тестування:**
```bash
python batch_processing/test_pipeline_locally.py
```

2. **Окремі етапи:**
```bash
python batch_processing/landing_to_bronze.py
python batch_processing/bronze_to_silver.py
python batch_processing/silver_to_gold.py
```

### Що показати в скриншотах:

#### 🎯 **Обов'язкові скриншоти:**

1. **Структура даних після виконання:**
```
data/
├── landing/
│   ├── athlete_bio.csv
│   └── athlete_event_results.csv
├── bronze/
│   ├── athlete_bio/ (parquet files)
│   └── athlete_event_results/ (parquet files)
├── silver/
│   ├── athlete_bio/ (cleaned parquet files)
│   └── athlete_event_results/ (cleaned parquet files)
└── gold/
    └── avg_stats/ (aggregated parquet files)
```

2. **Вміст gold layer - avg_stats:**
   - Показати результат агрегації з середніми значеннями
   - Групування по sport, medal, sex, country_noc

#### 📋 **Етапи з коментарями в коді:**

```python
# landing_to_bronze.py:
# Етап 1: Завантаження файлу з FTP-сервера в оригінальному форматі CSV
# Етап 2: Читання CSV файлу за допомогою Spark та збереження у форматі Parquet

# bronze_to_silver.py:
# Етап 1: Зчитування таблиці з bronze layer
# Етап 2: Виконання функції чистки тексту для всіх текстових колонок
# Етап 3: Дедублікація рядків

# silver_to_gold.py:
# Етап 1: Зчитування двох таблиць із silver layer
# Етап 2: Виконання об'єднання та деяких перетворень
# Етап 3: Запис таблиці в папку gold

# project_solution.py:
# Airflow DAG який послідовно запускає всі три файли
```

---

## Команди для перевірки результатів

### MySQL запити:
```sql
-- Перевірка кількості записів
SELECT COUNT(*) FROM enriched_athlete_stats;

-- Приклад даних
SELECT sport, medal_status, sex, country_noc, avg_height, avg_weight, timestamp 
FROM enriched_athlete_stats 
LIMIT 10;

-- Статистика по спортам
SELECT sport, COUNT(*) as count 
FROM enriched_athlete_stats 
GROUP BY sport 
ORDER BY count DESC;
```

### Kafka команди:
```bash
# Перегляд повідомлень в топіку
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic enriched_athlete_data --from-beginning

# Або використайте demo_results.py для структурованого виводу
```

### Spark команди для перевірки Parquet файлів:
```python
# Читання gold layer результатів
df = spark.read.parquet("data/gold/avg_stats")
df.show(20, truncate=False)
df.printSchema()
print(f"Total records: {df.count()}")
```

---

## Чек-лист для здачі проєкту

### Частина 1 (Streaming):
- [ ] Код запускається без помилок
- [ ] Скриншот таблиці MySQL з результатами
- [ ] Скриншот даних з Kafka топіку
- [ ] Всі етапи позначені коментарями
- [ ] forEachBatch функція працює

### Частина 2 (Batch):
- [ ] Всі три файли виконуються успішно
- [ ] Структура data lake створена
- [ ] Скриншот gold layer результатів
- [ ] Airflow DAG створений
- [ ] Всі етапи позначені коментарями

### Загальне:
- [ ] README.md з інструкціями
- [ ] Код добре структурований
- [ ] Обробка помилок реалізована
- [ ] Логування детальне та зрозуміле