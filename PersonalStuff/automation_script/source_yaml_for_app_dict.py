from pathlib import Path
from uuid import uuid4
import pandas as pd

import os
import shutil


class AutomatedIngestRawTransformation:
    """
    This class is designed to facilitate the creation of a transformation template that ingests raw data.
    This is accomplished by reading a schema file, typically in Excel format, which contains metadata
    about the dataset.
    """

    output_folder = 'output_transformation/'
    os.makedirs(output_folder, exist_ok=True)

    def __init__(self) :
        self.mapping_sheet = None

    def create_ingest_raw_transformation(self, output_folder=None):
        """
        This function generates an automated transformation template by reading a schema file and
         extract the information like ColumnNames and FileName[dataset_name].

        :parameter
        output_folder: The folder where the generated template will be stored.
                       If not specified, it defaults to 'output_transformation/'.
        :return: The function does not have a return statement, Its primary purpose is to perform
        operations and saves the generated template directly to the specified output folder.

        """

        # Set default output folder if not provided
        if output_folder is None:
            output_folder = self.output_folder

        # Load source data from local machine
        path = input("Enter the path of DataSchema file from local machine: ")
        path = path.replace('"', '')
        path = Path(path)
        self.mapping_sheet = pd.read_excel(path)

        # Prompt user for input the SourceName
        source_name = input("Please enter the source name for transformation: ")
        self.mapping_sheet['source_name'] = source_name.upper()
        cols_data = self.mapping_sheet

        for name in cols_data['File Name and Extension'].dropna().unique().tolist():
            column_names = cols_data.loc[cols_data['File Name and Extension'] == name].iloc[:, 1].dropna().tolist()
            print(column_names)
            transformation_string = name.split('.')[0]
            transform_name = "Raw_" + source_name.upper() + '_' + transformation_string.lower()
            print(transform_name)
            require_name = "Ingested_" + source_name.upper() + '_' + transformation_string.lower()
            print(require_name)

            # create a dictionary to represent the type for write_partition and write_type
            dict_mechanism = {"F": ("upsert_from_full", '"status"'),
                              "D": ("append", ["year"]),
                              "DL": ("append", ["year", "month"]),
                              "FU": ("upsert_from_changed", '"status"'),
                              "FU-J": ("upsert_from_changed", '"status"')
                              }
            file_type = input(f"Please enter type from dict_mechanism {', '.join(i for i in dict_mechanism.keys())}: ")
            print(file_type)
            # print(dict_mechanism[file_type][0], dict_mechanism[file_type][1])

            requires = ',\n\t\t'.join([f'"{require_name}:' + col.strip().lower() + '"' for col in column_names])

            transform_template_start = f"""
@transform(
    name="{transform_name}",
    requires=[
    \t{requires},
    ],
    write_type="{dict_mechanism[file_type][0]}",
    write_partition={dict_mechanism[file_type][1]},
    write_metadata="default_raw",
    owner=\"mail_car_caap_data_enabling@nl.abnamro.com\",   
)
def transform_{uuid4().hex}(context: FeatureStore):
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
                    if file_type in ("DL", "D"):
                        w.write(transform_template_start)
                        w.write(transform_template_middle)
                        w.write(transform_template_end)
                    elif file_type in ("F", "FU", "FU-J"):
                        w.write(transform_template_start)
                        w.write(transform_template_end)
                    else:
                        print(transform_name, "UNKNOWN TYPE", file_type)
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
        shutil.rmtree(output_folder)


    def source_yaml_config(self):
        """
        The function is designed to create a directory and write configuration data into a YAML file
        based on extracted information from an attribute called mapping_sheet

        :parameter:
        self: This parameter refers to the instance of the class in which this method is defined.
        It allows access to the instance’s attributes and other methods.
        :return: The function does not have a return statement, Its primary purpose is to perform
        file operations (creating directories and writing to a YAML file) rather than returning a value.

        """

        output_directory = "output_transformation_combined/"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=False)

        yaml_source_config_data = self.mapping_sheet

        # Open output file for writing
        with open(os.path.join(output_directory, "output_for_source_config.yaml"), 'w') as w:

            for file_names in yaml_source_config_data['File Name and Extension'].dropna().unique():
                file_name = file_names.split('.')[0].lower()
                print(file_name)
                # file_type = file_names.split('.')[-1]
                file_type = 'parquet'
                # print(file_type)
                for source in yaml_source_config_data['source_name'].unique():
                    source_name = source
                    print(source_name)

                if file_type:
                    template_ingested = f'''
Ingested_{source_name}_{file_name}:
  file_type: {file_type}
  path: dbfs:/mnt/ingested/{source_name}/{file_name}/$(ingested_suffix_path)
  read_partition: none'''

                    template_raw = f'''
Raw_{source_name}_{file_name}:
  file_type: delta
  path: dbfs:/mnt/raw/{source_name}/{file_name}
  read_partition: none'''

                else:
                    raise ValueError(f"There is a bug in {template_ingested} and {template_raw}, please review the code")

                # Write the formatted strings to the output file
                w.writelines(template_ingested)
                w.writelines(template_raw)


# Usage
if __name__ == "__main__":
    mapper = AutomatedIngestRawTransformation()
    mapper.create_ingest_raw_transformation()
    mapper.source_yaml_config()
