import os
import re
import pandas as pd

class ConfigTableUpdater:

    def __init__(self):
        # Initialize with empty input and output directory; these will be passed to execute
        self.csv_file = None
        self.output_directory = None

    def update_config_tables(self):
        data_optimize_to_append = []
        onboarded_data_to_append = []

        # Loop through the data and construct paths for both tables
        for delta_path1 in self.csv_file['source_name'].unique():
            for delta_path2 in self.csv_file['File Name and Extension'].dropna().unique():
                delta_path2_name = delta_path2.split('.')[0].lower()
                delta_path_source = f"/mnt/raw/{delta_path1.upper()}/{delta_path2_name}"

                # Append Data for the optimize_delta_table_config.csv
                data_optimize_to_append.append({
                    'DeltaTablePath': delta_path_source,
                    # PartitionFilterCondition and ZorderColumns can be given manually
                    'PartitionFilterCondition': "",
                    'ZorderColumns': ""
                })

                # Append Data for the datasets_onboarded_to_ingested.csv
                source_name = delta_path1.lower()
                file_name = re.split(r'[.,]', delta_path2 )[0]
                ingested_base_location = f'{source_name.upper()}/{file_name.lower()}'
                onboarded_data_to_append.append({
                    'source_name': source_name,
                    'file_name': file_name,
                    'ingested_base_location': ingested_base_location,
                    'copy_switch': 'dial_all_cols'
                })

        # Save to optimize_delta_table_config.csv
        optimize_df = pd.DataFrame(data_optimize_to_append)
        optimize_output_path = os.path.join(self.output_directory, "optimize_delta_table_config.csv")
        optimize_df.to_csv(optimize_output_path, mode='a', header=not os.path.exists(optimize_output_path),
                           index=False)

        # Save to datasets_onboarded_to_ingested.csv
        onboarded_df = pd.DataFrame(onboarded_data_to_append)
        onboarded_output_path = os.path.join(self.output_directory, "datasets_onboarded_to_ingested.csv")
        onboarded_df.to_csv(onboarded_output_path, mode='a', header=not os.path.exists(onboarded_output_path),
                            index=False)

    def execute(self, input_csv, output_dir):
        # Load the input CSV and set the output directory
        self.csv_file = pd.read_csv(input_csv, delimiter=',')
        self.output_directory = output_dir

        # Create output directory if it does not exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        # Call the function
        self.update_config_tables()


config_updater = ConfigTableUpdater()
# Execute the function by passing the parameters directly
config_updater.execute("cts_ingest_to_raw_config.csv", "output_transformation_combined/")