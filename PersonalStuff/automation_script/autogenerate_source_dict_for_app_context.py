import os
import pandas as pd

# Read the CSV file into a DataFrame
csv_file = pd.read_csv("cts_ingest_to_raw_config.csv", delimiter=',')

output_directory = "output_transformation_combined/"

if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Open output file for writing
with open(os.path.join(output_directory, "output_for_app_context.py"), 'w') as w:

    for file_names in csv_file['File Name and Extension'].dropna().unique():
        file_name = file_names.split('.')[0].lower()
        if '.' in file_names:
            file_format = file_names.split('.')[-1]
            file_delimiter = ',' if file_format == 'csv' else None  # ',' for CSV files
        for source in csv_file['source_name'].unique():
            source_name = source
            for type_value in csv_file['type'].unique():
                type = type_value
                if type == 'FU':
                    user_input = input("Please enter key column(s) separated by commas: ")
                    key_columns = [col.strip() for col in user_input.split(',')]
                else:
                    print(f"No key_columns required: as the type {type} is not Valid Scenario")

        # Format the ingested template
        if file_format == 'csv':
            template_ingested = f'''
        "Ingested_{source_name}_{file_name}": {{
            "path": "$(paths:mount_point_ingested)/{source_name}/{file_name}/$(options:ingested_sub_path)/",
            "file_type": "{file_format}",
            "sep": "{file_delimiter}",
        }},'''
        else:
            template_ingested = f'''
        "Ingested_{source_name}_{file_name}": {{
            "path": "$(paths:mount_point_ingested)/{source_name}/{file_name}/$(options:ingested_sub_path)/",
            "file_type": "{file_format}",
        }},'''

        # Format the raw template
        if type == "FU":
            template_raw = f'''
        "Raw_{source_name}_{file_name}": {{
            "path": "$(paths:mount_point_raw)/{source_name}/{file_name}/",
            "file_type": "delta",
            "key": {key_columns},
        }},'''
        else:
            template_raw = f'''
        "Raw_{source_name}_{file_name}": {{
            "path": "$(paths:mount_point_raw)/{source_name}/{file_name}/",
            "file_type": "delta",
        }},'''

        template_ingested = template_ingested.replace('(', '{').replace(')', '}')
        template_raw = template_raw.replace('(', '{').replace(')', '}')

        # Write the formatted strings to the output file
        w.writelines(template_ingested)
        w.writelines(template_raw)
