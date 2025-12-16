# Анализ медицинских данных COVID-19 рентгеновских снимков

## Описание проекта

Проект представляет собой комплексную обработку и анализ медицинских данных рентгеновских снимков пациентов с COVID-19 и другими заболеваниями дыхательной системы. Включает очистку данных, заполнение пропусков, анализ аномалий и создание визуализаций.

## Что делает код

### Основные функции:
1. **Предобработка данных**: очистка дубликатов, обработка пропусков, унификация диагнозов
2. **Заполнение недостающих данных**:
   - ML-предсказание возраста (Random Forest)
   - Извлечение пола из текстовых клинических заметок
   - Заполнение PCR-тестов на основе диагнозов
3. **Анализ качества данных**: выявление аномалий, проверка логических противоречий
4. **SQL-аналитика**: комплексный анализ медицинских показателей
5. **Визуализация**: создание графиков и диаграмм для анализа

### Технологический стек:
- **PySpark**: обработка больших объемов данных
- **Python**: анализ и визуализация
- **SQL**: аналитические запросы
- **Machine Learning**: предсказание пропущенных значений

## Как запустить

### Предварительные требования:
1. **Python 3.8+** (рекомендуется 3.11)
2. **Java 8/11/17/21**
3. **Hadoop для Windows** (для PySpark на Windows)

### Установка на Windows:

#### 1. Установите Hadoop для Windows:
```bash
# Скачайте winutils для Hadoop 3.3.6:
# https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.6/bin

# Создайте папку и скопируйте файлы:
mkdir C:\hadoop
# Копируйте winutils.exe и hadoop.dll в C:\hadoop\bin\
```

#### 2. Установите Python зависимости:
```bash
# Создайте виртуальное окружение
python -m venv covid_env

# Активируйте окружение
# Windows:
covid_env\Scripts\activate
# Linux/Mac:
source covid_env/bin/activate

# Установите зависимости
pip install -r requirements.txt
```

#### 3. Установите переменные окружения:
```bash
# Windows (в командной строке или системных настройках):
setx HADOOP_HOME "C:\hadoop"
setx PATH "%HADOOP_HOME%\bin;%PATH%"
setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
setx PYSPARK_PYTHON "python"
```

#### 4. Запустите Jupyter Notebook:
```bash
jupyter notebook DZ1.ipynb
```

## Структура проекта

```
project/
│
├── DZ1.ipynb                    # Основной Jupyter Notebook с кодом
├── Report_Kozyrev.pdf           # Отчет по анализу (PDF формат)
├── requirements.txt             # Список Python зависимостей
├── README.md                    # Этот файл
│
├── metadata.csv                 # Исходные медицинские данные
```

## Структура данных

Обрабатываемый датасет содержит медицинские данные рентгеновских снимков:
- **Основные поля**: patientid, filename, finding, age, sex
- **Клинические данные**: RT_PCR_positive, survival, intubated, went_icu
- **Диагностические данные**: pneumonia_etiology, bacterial_subtype, viral_subtype
- **Технические данные**: view (проекция), offset (дни от начала болезни)

## SQL-аналитика

Проект включает комплексные SQL-запросы для медицинского анализа:

### Ключевые SQL-запросы:

#### 1. **Базовая статистика по диагнозам**
```sql
SELECT 
    main_diagnosis AS diagnosis,
    COUNT(*) AS patient_count,
    ROUND(AVG(age_filled), 1) AS avg_age,
    ROUND(AVG(CASE WHEN survival = True THEN 1 ELSE 0 END) * 100, 2) AS survival_rate,
    ROUND(AVG(CASE WHEN went_icu = True THEN 1 ELSE 0 END) * 100, 2) AS icu_rate
FROM covid_analysis
WHERE main_diagnosis IS NOT NULL
GROUP BY main_diagnosis
ORDER BY patient_count DESC
```

#### 2. **Анализ по полу и возрасту**
```sql
SELECT 
    sex AS gender,
    CASE 
        WHEN age_filled < 30 THEN 'Молодые (<30)'
        WHEN age_filled BETWEEN 30 AND 50 THEN 'Взрослые (30-50)'
        WHEN age_filled BETWEEN 51 AND 70 THEN 'Пожилые (51-70)'
        WHEN age_filled > 70 THEN 'Старшие (>70)'
        ELSE 'Не указан'
    END AS age_group,
    COUNT(*) AS count,
    ROUND(AVG(CASE WHEN survival = True THEN 1 ELSE 0 END) * 100, 2) AS survival_rate
FROM covid_analysis
WHERE sex IS NOT NULL AND age_filled IS NOT NULL
GROUP BY sex, 
    CASE 
        WHEN age_filled < 30 THEN 'Молодые (<30)'
        WHEN age_filled BETWEEN 30 AND 50 THEN 'Взрослые (30-50)'
        WHEN age_filled BETWEEN 51 AND 70 THEN 'Пожилые (51-70)'
        WHEN age_filled > 70 THEN 'Старшие (>70)'
        ELSE 'Не указан'
    END
ORDER BY gender, age_group
```

#### 3. **Топ-3 самых взрослых пациентов по диагнозам**
```sql
WITH RankedPatients AS (
    SELECT 
        patientid,
        main_diagnosis AS diagnosis,
        sex AS gender,
        age_filled AS age,
        survival AS survived,
        went_icu AS in_icu,
        intubated AS intubated,
        ROW_NUMBER() OVER (
            PARTITION BY main_diagnosis 
            ORDER BY age_filled DESC
        ) AS age_rank
    FROM covid_analysis
    WHERE main_diagnosis IS NOT NULL AND age_filled IS NOT NULL
)
SELECT 
    diagnosis,
    age_rank AS rank,
    patientid AS patient_id,
    gender,
    age,
    CASE WHEN survived THEN 'Yes' ELSE 'No' END AS survived,
    CASE WHEN in_icu THEN 'Yes' ELSE 'No' END AS in_icu
FROM RankedPatients
WHERE age_rank <= 3
ORDER BY diagnosis, age_rank
```

#### 4. **Распределение выживаемости по временным периодам**
```sql
SELECT 
    CASE 
        WHEN offset IS NULL THEN 'Не указано'
        WHEN offset = 0 THEN 'День 0'
        WHEN offset BETWEEN 1 AND 7 THEN '1-7 дней'
        WHEN offset BETWEEN 8 AND 14 THEN '8-14 дней'
        WHEN offset BETWEEN 15 AND 30 THEN '15-30 дней'
        WHEN offset BETWEEN 31 AND 90 THEN '1-3 месяца'
        ELSE '>3 месяцев'
    END AS period,
    COUNT(*) as scan_count,
    ROUND(AVG(CASE WHEN survival = True THEN 1 ELSE 0 END) * 100, 2) as survival_rate,
    ROUND(AVG(CASE WHEN went_icu = True THEN 1 ELSE 0 END) * 100, 2) as icu_rate,
    ROUND(AVG(CASE WHEN intubated = True THEN 1 ELSE 0 END) * 100, 2) as intubation_rate
FROM covid_analysis
GROUP BY 
    CASE 
        WHEN offset IS NULL THEN 'Не указано'
        WHEN offset = 0 THEN 'День 0'
        WHEN offset BETWEEN 1 AND 7 THEN '1-7 дней'
        WHEN offset BETWEEN 8 AND 14 THEN '8-14 дней'
        WHEN offset BETWEEN 15 AND 30 THEN '15-30 дней'
        WHEN offset BETWEEN 31 AND 90 THEN '1-3 месяца'
        ELSE '>3 месяцев'
    END
ORDER BY period
```

#### 5. **Анализ вирусных подтипов и PCR-тестов**
```sql
SELECT 
    viral_subtype,
    RT_PCR_positive,
    COUNT(*) as count,
    ROUND(AVG(age_filled), 1) as avg_age,
    ROUND(AVG(CASE WHEN survival = True THEN 1 ELSE 0 END) * 100, 2) as survival_rate,
    ROUND(AVG(CASE WHEN went_icu = True THEN 1 ELSE 0 END) * 100, 2) as icu_rate
FROM covid_analysis
WHERE viral_subtype IS NOT NULL AND viral_subtype != 'unknown'
GROUP BY viral_subtype, RT_PCR_positive
ORDER BY viral_subtype, count DESC
```

#### 6. **Корреляция между возрастом и тяжестью заболевания**
```sql
SELECT 
    CASE 
        WHEN age_filled < 30 THEN 'Молодые (<30)'
        WHEN age_filled BETWEEN 30 AND 50 THEN 'Взрослые (30-50)'
        WHEN age_filled BETWEEN 51 AND 70 THEN 'Пожилые (51-70)'
        WHEN age_filled > 70 THEN 'Старшие (>70)'
        ELSE 'Не указан'
    END AS age_group,
    COUNT(*) as total_patients,
    SUM(CASE WHEN went_icu = True THEN 1 ELSE 0 END) as icu_patients,
    SUM(CASE WHEN intubated = True THEN 1 ELSE 0 END) as intubated_patients,
    SUM(CASE WHEN survival = True THEN 1 ELSE 0 END) as survived_patients,
    ROUND(SUM(CASE WHEN went_icu = True THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as icu_percentage,
    ROUND(SUM(CASE WHEN intubated = True THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as intubated_percentage,
    ROUND(SUM(CASE WHEN survival = True THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as survival_percentage
FROM covid_analysis
WHERE age_filled IS NOT NULL
GROUP BY 
    CASE 
        WHEN age_filled < 30 THEN 'Молодые (<30)'
        WHEN age_filled BETWEEN 30 AND 50 THEN 'Взрослые (30-50)'
        WHEN age_filled BETWEEN 51 AND 70 THEN 'Пожилые (51-70)'
        WHEN age_filled > 70 THEN 'Старшие (>70)'
        ELSE 'Не указан'
    END
ORDER BY age_group
```

## Особенности реализации

### Ключевые функции:
- **Интеллектуальное заполнение пропусков**: использование ML и логических правил
- **Гибкая обработка аномалий**: безопасное исправление некорректных значений
- **Комплексная визуализация**: 8+ типов графиков для анализа
- **SQL-аналитика**: готовые аналитические запросы для исследований

### Особенности работы с PySpark на Windows:
- Настройка Hadoop через winutils
- Оптимизация памяти для локального выполнения
- Обработка ошибок совместимости

## Результаты

Проект включает:
1. **Очищенный датасет** с заполненными пропусками
2. **Аналитический отчет** с выводами
3. **Готовые визуализации** для презентации результатов
4. **SQL-запросы** для дальнейшего анализа

## Примечания

1. Для работы на Windows обязательна установка Hadoop winutils
2. Требуется Java для работы PySpark
3. При запуске проверьте правильность путей в настройках окружения
4. Для больших объемов данных может потребоваться увеличение памяти PySpark

## Поддержка

Для вопросов и проблем с запуском:
1. Проверьте настройки переменных окружения
2. Убедитесь в наличии всех файлов Hadoop
3. Проверьте версии Java и Python

---

*Проект выполнен в рамках учебного задания по анализу медицинских данных.*