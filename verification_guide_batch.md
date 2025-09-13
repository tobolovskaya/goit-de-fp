# Керівництво з верифікації результатів - Частина 2: Batch Data Lake

## Критерії прийняття та оцінювання

### ✅ Код виконується та надає бажаний результат
### ✅ Скриншоти демонструють результат роботи коду (скриншоти таблиць виведення та скриншот графу відпрацьованого DAGу)

---

## Як запустити та перевірити результати:

### 1. **Локальне тестування повного pipeline:**
```bash
python batch_processing/test_pipeline_locally.py
```

### 2. **Запуск окремих етапів:**
```bash
# Етап 1: Landing to Bronze
python batch_processing/landing_to_bronze.py

# Етап 2: Bronze to Silver  
python batch_processing/bronze_to_silver.py

# Етап 3: Silver to Gold
python batch_processing/silver_to_gold.py
```

### 3. **Демонстрація результатів:**
```bash
python batch_processing/demo_results.py
```

---

## 🎯 **Обов'язкові скриншоти для здачі:**

### **1. Скриншот структури data lake після виконання:**
Показати результат команди або виводу програми:
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

### **2. Скриншот результатів gold layer:**
Показати вивід таблиці з агрегованими даними:
- Колонки: sport, medal, sex, country_noc, avg_weight, avg_height, timestamp
- Приклад записів з розрахованими середніми значеннями

### **3. Скриншот відпрацьованого Airflow DAG:**
- Граф DAG з трьома задачами: `landing_to_bronze` → `bronze_to_silver` → `silver_to_gold`
- Статус виконання (зелені галочки)
- Часові мітки виконання

---

## 📋 **Етапи з коментарями в коді:**

### **landing_to_bronze.py:**
```python
# Етап 1: Завантаження файлу з FTP-сервера в оригінальному форматі CSV
# Етап 2: Читання CSV файлу за допомогою Spark та збереження у форматі Parquet
```

### **bronze_to_silver.py:**
```python
# Етап 1: Зчитування таблиці з bronze layer
# Етап 2: Виконання функції чистки тексту для всіх текстових колонок  
# Етап 3: Дедублікація рядків
```

### **silver_to_gold.py:**
```python
# Етап 1: Зчитування двох таблиць із silver layer
# Етап 2: Виконання об'єднання та деяких перетворень
# Етап 3: Запис таблиці в папку gold
```

### **project_solution.py:**
```python
# Task 1: Landing to Bronze - Завантаження з FTP та конвертація в Parquet
# Task 2: Bronze to Silver - Очищення тексту та дедублікація  
# Task 3: Silver to Gold - Агрегація та розрахунок статистик
# Define task dependencies - Послідовне виконання всіх трьох файлів
```

---

## 🔍 **Команди для перевірки результатів:**

### **Перевірка структури файлів:**
```bash
ls -la data/
ls -la data/bronze/
ls -la data/silver/
ls -la data/gold/
```

### **Перевірка Parquet файлів через Spark:**
```python
# Читання gold layer результатів
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("VerifyResults").getOrCreate()

# Перевірка gold layer
df = spark.read.parquet("data/gold/avg_stats")
df.show(20, truncate=False)
df.printSchema()
print(f"Total records: {df.count()}")

# Перевірка агрегації
df.groupBy("sport").count().show()
df.groupBy("medal").count().show()
```

---

## 📊 **Очікувані результати:**

### **Gold Layer Structure:**
- **sport**: Вид спорту (наприклад, "Swimming", "Athletics")
- **medal**: Тип медалі ("Gold", "Silver", "Bronze", "No Medal") 
- **sex**: Стать ("M", "F")
- **country_noc**: Код країни (наприклад, "USA", "GBR")
- **avg_weight**: Середня вага (decimal)
- **avg_height**: Середній зріст (decimal)
- **timestamp**: Час виконання розрахунків

### **Приклад запису:**
```
+----------+------+---+-----------+----------+----------+-------------------+
|sport     |medal |sex|country_noc|avg_weight|avg_height|timestamp          |
+----------+------+---+-----------+----------+----------+-------------------+
|Swimming  |Gold  |M  |USA        |75.50     |180.25    |2024-01-15 10:30:00|
|Athletics |Silver|F  |GBR        |58.75     |165.80    |2024-01-15 10:30:00|
+----------+------+---+-----------+----------+----------+-------------------+
```

---

## 🚀 **Розгортання в Airflow:**

### **1. Підготовка файлів:**
```bash
# Скопіювати всі файли в Airflow репозиторій
cp -r batch_processing/ /path/to/airflow_sandbox/dags/
```

### **2. Структура в Airflow:**
```
airflow_sandbox/
└── dags/
    └── batch_processing/
        ├── landing_to_bronze.py
        ├── bronze_to_silver.py  
        ├── silver_to_gold.py
        └── project_solution.py
```

### **3. Активація DAG:**
- DAG ID: `batch_data_lake_pipeline`
- Переконайтеся, що connection `spark-default` налаштований
- Запустіть DAG та зробіть скриншот графу виконання

---

## ✅ **Чек-лист для здачі проєкту:**

- [ ] Код запускається без помилок локально
- [ ] Всі три файли виконуються успішно  
- [ ] Створена структура data lake (landing/bronze/silver/gold)
- [ ] Скриншот структури директорій
- [ ] Скриншот gold layer результатів з агрегованими даними
- [ ] Airflow DAG створений та працює
- [ ] Скриншот графу відпрацьованого DAG в Airflow
- [ ] Всі етапи позначені коментарями в коді
- [ ] Демонстрація результатів працює

---

## 🎯 **Підсумок:**

Проєкт демонструє повний multi-hop data lake pipeline:
1. **Landing**: Завантаження сирих CSV даних з FTP
2. **Bronze**: Конвертація в Parquet формат  
3. **Silver**: Очищення та дедублікація
4. **Gold**: Агрегація та аналітичні розрахунки
5. **Orchestration**: Автоматизація через Airflow DAG