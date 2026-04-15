# 📊Marketing project

<br>

## 🛠 Технологічний стек
**Оркестрація:** Airflow — керування пайплайнами.

**Трансформація:** DBT — перетворення даних та тестування.

**Сховище:** DuckDB — аналітична база даних.

**Джерела:** MinIO, MySQL, JSON.

<br>


## 🏗 Архітектура

**Raw Layer:** Сирі дані завантажуються в DuckDB з джерел (MySQL, MinIO, JSON).

**Staging Layer:** Очищення даних, приведення типів, фільтрація дублікатів.

**Mart/Fact Layer:** Розрахунок фінальних показників.

<br>

## 📂 Структура проєкту
dags/ — пайплайни Airflow для завантаження даних (Python).

dbt/models/ — SQL-моделі трансформації (raw, staging, mart).

dbt/seeds/ — статичні довідники (CSV).

data/ — локальне сховище для JSON-файлів та бази DuckDB.

docker-compose.override.yml — конфігурація додаткових сервісів (MinIO, MySQL).

---

<img width="1236" height="844" alt="conversions" src="https://github.com/user-attachments/assets/54343e73-2170-4989-8c0d-999a27dab811" />
<img width="1174" height="844" alt="lead_cost_platforms" src="https://github.com/user-attachments/assets/b49cfe44-e4cc-422d-881d-3297f05fbabf" />
<img width="1749" height="844" alt="coordinators_workload" src="https://github.com/user-attachments/assets/8f23361f-61e6-4b98-a1ee-af483621424a" />
<img width="1555" height="844" alt="sales_kpi" src="https://github.com/user-attachments/assets/dd2b4250-1abc-4588-a330-0328ac570912" />


