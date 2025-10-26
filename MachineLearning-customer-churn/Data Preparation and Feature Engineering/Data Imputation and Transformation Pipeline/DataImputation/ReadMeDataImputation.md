# Data Imputation and Transformation Pipeline
This module is part of the Customer Churn Prediction project and focuses on handling missing data and applying robust data transformation steps essential for clean machine learning workflows.

## Overview
#### Data Imputation and Transformation Pipeline provides Python scripts and helper functions for:

- Automated detection and imputation of missing values
- Type-aware filling (Boolean/string/numeric features)
- Outlier detection and cleaning
- DataFrame utilities for preprocessing in Spark or Pandas pipelines

## Folder Structure
```
Data Preparation and Feature Engineering/
    └── Data Imputation and Transformation Pipeline/
        ├── Data Imputation
        ├── data_imputation_transformed_pipeline.py
        ├── handling_missing_values_for_outliers.py
        ├── README.md
        └── ...
```

## Features
- Flexible Imputation: Support for different feature types (categorical, boolean, numerical)
- Easy Integration: Designed for plug-and-play in end-to-end ML pipelines
- Modular Code: Functions callable from other scripts or main pipeline jobs
- Best Practices: Includes examples and comments for maintainability

## Usage
- Clone or download the repository.
- Place your raw data in the appropriate directory.
- Adjust and execute the main data imputation/transformation scripts:

```
python handle_missing_values_for_outliers.py
```
or integrate functions directly into your own workflows.

- Results (cleaned DataFrames / CSVs / Delta tables) are available for downstream modeling or feature engineering.

## Example
```
python
from handle_missing_values_for_outliers import calculate_missing

df_cleaned = calculate_missing(df_raw)
```

## Requirements
- Python 3.7+
- PySpark or Pandas (as needed)
- Additional requirements listed in requirements.txt (if present)

## Guidance Material
> [Databricks Partner Academy Learning](https://www.databricks.com/learn/partners/partner-courses-and-public-schedule/scalable-machine-learning-apache-spark)


