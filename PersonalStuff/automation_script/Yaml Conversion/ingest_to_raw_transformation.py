import pandas as pd
import os
import shutil


class AutomatedIngestRawTransformation:
    repo_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

    output_folder = 'output_transformation/'
    os.makedirs(output_folder, exist_ok=True)

    def __init__(self, source_file):
        self.source_file = source_file
        self.mapping_sheet = None

    def create_automated_transformation(self, output_folder=None):
        # Set default output folder if not provided
        if output_folder is None:
            output_folder = self.output_folder

        # Load source data
        if self.source_file.endswith('.csv'):
            self.mapping_sheet = pd.read_csv(self.source_file)
        elif self.source_file.endswith('.xlsx'):
            self.mapping_sheet = pd.read_excel(self.source_file)
        else:
            raise ValueError("Unsupported file format")

        # Prompt user for input
        type_value = input("Please enter the valid load type for transformation: ")
        source_name = input("Please enter the valid source name for transformation: ")
        self.mapping_sheet['type'] = type_value
        self.mapping_sheet['source_name'] = source_name.upper()
        cols_data = self.mapping_sheet
        cols_data.to_csv('cts_ingest_to_raw_config.csv', index=False)

        for name in cols_data['File Name and Extension'].dropna().unique().tolist():
            column_names = cols_data.loc[cols_data['File Name and Extension'] == name].iloc[:, 1].dropna().tolist()
            print(column_names)
            transformation_string = name.split('.')[0]
            # source_name is for dest_file
            transform_name = "Raw_" + source_name.upper() + '_' + transformation_string.lower()
            require_name = "Ingested_" + source_name.upper() + '_' + transformation_string.lower()

            if type_value == "D":
                write_type = "append"
                write_partition = ["year"]
            elif type_value == "DL":
                write_type = "append"
                write_partition = ["year", "month"]
            elif type_value == "F":
                write_type = "upsert_from_full"
                write_partition = ["status"]
            elif type_value in ('FU', 'FU-J'):
                write_type = "upsert_from_changed"
                write_partition = ["status"]
            else:
                write_type = ""
                write_partition = []

            requires = ',\n\t\t'.join([f'"{require_name}:' + col.strip().lower() + '"' for col in column_names])

            transform_template_start = f"""
@transform(
    name="{transform_name}",
    requires=[
    \t{requires}
    ],
    write_type="{write_type}",
    write_partition={write_partition},
    write_metadata="default_raw",
    owner=\"mail_car_caap_data_enabling@nl.abnamro.com\",   
)
def generic(context: FeatureStore):
    df_dict = context.load_requirements()

    result = df_dict["{require_name}"]
    result = result.dropDuplicates()"""

            transform_template_middle = f"""
    result = result.withColumn("processing_date", psf.lit(context.period_date))"""

            transform_template_end = f"""

    return context.save(result)
          """
            try:
                with open(f"{output_folder}/{transform_name}.py", "w") as w:
                    if type_value in ("DL", "D"):
                        w.write(transform_template_start)
                        w.write(transform_template_middle)
                        w.write(transform_template_end)
                    elif type_value in ("F", "FU", "FU-J"):
                        w.write(transform_template_start)
                        w.write(transform_template_end)
                    else:
                        print(transform_name, "UNKNOWN TYPE", type_value)
            except Exception as e:
                print(transform_name, 'EXCEPTION', e)

        # Combine individual file transformation in source level
        output_transformation_combined = 'output_transformation_combined/'
        shutil.rmtree(output_transformation_combined, True)
        os.makedirs(output_transformation_combined, exist_ok=True)

        files = os.listdir(output_folder)
        for file in files:
            temp = file.split('_')
            if len(temp) > 1:
                file_name = temp[1].lower() + '_raw_transformation.py'
            else:
                file_name = temp[0].lower() + '_raw_transformation.py'

            if not os.path.exists(output_transformation_combined + file_name):
                with open(output_transformation_combined + file_name, 'w') as w:
                    w.write(
                        'from abnamro_caap_featurestore_engine.feature_store import transform, FeatureStore, psf\n\n')

            with open(output_transformation_combined + file_name, 'a') as a:
                with open(output_folder + file, 'r') as r:
                    a.write(r.read())
                    a.write('\n')


    def source_yaml_config(self):
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

# Usage
if __name__ == "__main__":
    mapper = AutomatedIngestRawTransformation('Data_schema.xlsx')
    mapper.create_automated_transformation()
    mapper.source_yaml_config()
