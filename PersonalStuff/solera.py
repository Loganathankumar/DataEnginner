import pandas as pd
import csv
import os
import shutil


class DataMapper:
    repo_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))

    output_folder = 'output_transformation/'
    os.makedirs(output_folder, exist_ok=True)

    def __init__(self, source_file):
        self.source_file = source_file
        self.mapping_sheet = None

    def create_automated_transformation(self, output_folder=None):
        # Load source data
        global transform_name
        if self.source_file.endswith('.csv'):
            self.mapping_sheet = pd.read_csv(self.source_file)
        elif self.source_file.endswith('.xlsx'):
            self.mapping_sheet = pd.read_excel(self.source_file)
        else:
            raise ValueError("Unsupported file format")

        # Prompt user for input
        type_value = input("Please enter the valid load type for transformation: ")
        self.mapping_sheet['type'] = type_value
        req_cols_data = self.mapping_sheet
        req_cols_data.to_csv('ingest_to_raw_config.csv', index=False)

        column_names = req_cols_data.iloc[:, 1].dropna().tolist()
        for transformation_name in sorted(set(req_cols_data.iloc[:, -2].dropna())):
            transformation_string = transformation_name.split('.')[0]
            dest_file, dest_name = transformation_string.split('_', 1)
            dest_name = dest_name.lower()

            transform_name = "Raw_" + dest_file + '_' + dest_name
            require_name = "Ingested_" + dest_file + '_' + dest_name

            if type == "D":
                write_type = "append"
                write_partition = ["year"]
            elif type == "DL":
                write_type = "append"
                write_partition = ["year", "month"]
            elif type == "F":
                write_type = "upsert_from_full"
                write_partition = ["status"]
            elif type in ('FU', 'FU-J'):
                write_type = "upsert_from_changed"
                write_partition = ["status"]
            else:
                write_type = ""
                write_partition = []

            requires = ',\n'.join([f'"{require_name}:' + col.strip().lower() + '"' for col in column_names])

            transform_template_start = f'''
        @transform(
            name="{transform_name}",
            requires=[
                {requires}
            ],
            write_type="{write_type}",
            write_partition={write_partition},
            write_metadata="default_raw",
            owner=mail_car_caap_data_enabling@nl.abnamro.com,    
        )
        def generic(context: FeatureStore):
            df_dict = context.load_requirements()
        
            result = df_dict["{require_name}"]
            result = result.dropDuplicates()'''
            transform_template_middle = f'''
            result = result.withColumn("processing_date", psf.lit(context.period_date))'''
            transform_template_end = f'''
        
            return context.save(result)
          '''
        try:
            with open(f"{output_folder}/{transform_name}.py", "w") as w:
                if type in ("DL", "D"):
                    w.write(transform_template_start)
                    w.write(transform_template_middle)
                    w.write(transform_template_end)
                elif type in ("F", "FU", "FU-J"):
                    w.write(transform_template_start)
                    w.write(transform_template_end)
                else:
                    print(transform_name, "UNKNOWN TYPE", type)
        except Exception as e:
            print(transform_name, 'EXCEPTION', e)

        # Combine individual file transformation in source level
        output_transformation_combined = 'output_transformation_combined/'
        shutil.rmtree(output_transformation_combined, True)
        os.makedirs(output_transformation_combined, exist_ok=True)

        a = os.listdir(output_folder)
        for i in a:
            temp = i.split('_')
            print(temp)
            file_name = temp[1].lower() + '_raw_transformation.py'

            if not os.path.exists(output_transformation_combined + file_name):
                with open(output_transformation_combined + file_name, 'w') as w:
                    w.write(
                        'from abnamro_caap_featurestore_engine.feature_store import transform, FeatureStore, psf\n\n')

            with open(output_transformation_combined + file_name, 'a') as a:
                with open(output_folder + i, 'r') as r:
                    a.write(r.read())
                    a.write('\n')


# # Usage
mapper = DataMapper('Data_schema.xlsx')
mapping_df = mapper.create_automated_transformation()

