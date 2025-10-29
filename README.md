# PySpark-Real-World-Scenarios


This repository contains five hands-on PySpark scenarios implemented on Databricks to help you understand and master real-world data engineering workflows.
Each notebook demonstrates a key PySpark concept, from flattening complex JSON data to implementing Slowly Changing Dimensions dynamically.

⚙️ Scenarios Overview
1️⃣ Flattening JSON Data Dynamically

File: 1_json_flattening.py
Description:
This scenario demonstrates how to read complex nested JSON data, flatten it dynamically, and explode arrays for detailed analysis.
You’ll learn how to:

Load multi-line JSON data into a PySpark DataFrame.

Select and explode nested columns using explode() and selectExpr().

Prepare structured tabular data from semi-structured JSON files.

Key Concepts: read.json, explode, nested fields, data flattening.

2️⃣ Handling Nulls and Data Cleaning

File: 2_null_handling.py
Description:
Learn how to handle missing, null, and corrupted data effectively.
This notebook covers:

Dropping nulls using dropna().

Replacing nulls with default values using fillna().

Identifying and filtering incomplete data for quality checks.

Key Concepts: dropna, fillna, filter, when, coalesce.

3️⃣ Drop Duplicates and Columns Optimization

File: 3_drop_duplicates_columns.py
Description:
Data duplication is a common issue in big data. This notebook shows how to:

Remove duplicate records using dropDuplicates().

Drop unnecessary or temporary columns to optimize storage.

Ensure a clean and unique dataset before analysis or merging.

Key Concepts: dropDuplicates, drop, distinct, schema optimization.

4️⃣ Performance Optimization in PySpark

File: 4_pyspark_optimization.py
Description:
This scenario focuses on PySpark performance tuning — from caching to optimizing joins.
You’ll explore:

Adaptive Query Execution (AQE).

Broadcast joins and partitioning strategies.

Using persist() and cache() effectively.

Handling driver OOM issues and Spark SQL Hints.

Key Concepts: broadcast, repartition, cache, persist, spark.conf, AQE.

5️⃣ Slowly Changing Dimensions (SCD Type-2)

File: 5_scd_type2_dynamic.py
Description:
A real-world enterprise scenario demonstrating dynamic Slowly Changing Dimensions (SCD Type-2).
You’ll learn to:

Create source and dimension tables dynamically in Databricks.

Detect changes in records and mark expired ones.

Use SQL-based MERGE for upsert operations.

Track history of each record with start_time, end_time, and isActive flags.

Key Concepts: MERGE INTO, Delta Tables, row_number, window, timestamps.

🧩 Tech Stack

Platform: Databricks

Language: PySpark (Python)

Storage: Delta Lake / JSON

SQL Engine: Spark SQL

🚀 How to Use

Import the notebooks into your Databricks Workspace.

Attach to a cluster (preferably runtime 14.3+ with Delta enabled).

Run each scenario in order to understand step-by-step transformations.

Explore intermediate outputs using display(df) for visual inspection.

📘 Learning Outcomes

By the end of these exercises, you’ll be able to:
✅ Handle complex JSON and semi-structured data.
✅ Clean, optimize, and prepare large-scale datasets.
✅ Improve PySpark performance with tuning techniques.
✅ Implement SCD Type-2 logic dynamically using Databricks Delta.
