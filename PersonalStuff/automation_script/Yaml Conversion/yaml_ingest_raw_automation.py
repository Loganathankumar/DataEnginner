import yaml

with open("ingest_raw.yml", 'r') as file:
    config = yaml.safe_load(file)

    source_file = config['automated_ingest_raw_transformation']['source_file']
    output_folder = config['automated_ingest_raw_transformation']['output_folder']