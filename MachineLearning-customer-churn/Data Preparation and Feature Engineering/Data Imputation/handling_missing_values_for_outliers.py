# Handling Missing Values
"""
To Handle missing values in dataset we need to identify columns with high percentages of missing data and drops those columns. 
Then, we will remove rows with missing values. Numeric columns are imputed with 0, and string columns are imputed with 'N/A'. 
Overall, the code demonstrates a comprehensive approach to handling missing values in the dataset.
"""
# Delete Columns
"""
- Create a DataFrame called missing_df to count the missing values per column in the telco_no_outliers_df dataset.
- The missing_df DataFrame is then transposed for better readability using the TransposeDF function, which allows for easier analysis of missing values.
"""

def calculate_missing(df, show=True):
    missing_df = df.select([
        count(when(
            (col(c) == 'None') |
            (col(c) == 'NULL') |
            (col(c) == '') |
            (col(c).isNull()), c)
        ).alias(c) for c in df.columns
    ])

    def transpose_dataframe(df):
        columns = df.columns
        columns_value = [f"'{c}', {c}" for c in columns]
        stack_expr = ",".join(columns_value)
        stacked = df.selectExpr(f"stack({len(columns)}, {stack_expr}) as (Column, `Number of Missing Values`)")
        return stacked

    transposed = transpose_dataframe(missing_df)

    if show:
        transposed.orderBy(col("Number of Missing Values").desc()).show()

    return transposed

# Now call it
# missing_df = calculate_missing(telco_no_outliers_df)
