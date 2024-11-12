import os
import yaml

class PythonToYAMLConverter:

    def __init__(self, python_file, output_folder = 'yaml_output/'):

        self.python_file = python_file
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)

    def convert_python_to_yaml(self):
        # Read the Python file
        with open(self.python_file, 'r') as file:
            python_code = file.readlines()

        yaml_content = self.parse_python_to_yaml(python_code)

        # Write the YAML output
        yaml_output_file = os.path.join(self.output_folder, 'yaml_ingest_to_raw_conversion.yaml')
        with open(yaml_output_file, 'w') as yaml_file:
            yaml.dump(yaml_content, yaml_file, default_flow_style=False)

        print(f"YAML file generated at: {yaml_output_file}")


    def parse_python_to_yaml(self, python_code):
        yaml_structure = {}

        for line in python_code:
            line = line.strip()

            if line.startswith('@transform'):
                # Extract the transform decorator details
                transform_name = line.split('(')[1].split(')')[0].strip()
                yaml_structure['transform'] = {
                    'name': transform_name,
                }

            if line.startswith('requires='):
                # Parse the requires argument, handle it as a list of strings
                requires = line.split('requires=')[1].strip().rstrip(',').strip('[]')
                requires_list = [item.strip().strip('"') for item in requires.split(',')]
                yaml_structure['transform']['requires'] = requires_list

            if line.startswith('write_type='):
                # Parse the write_type argument
                write_type = line.split('write_type=')[1].strip().rstrip(',')
                yaml_structure['transform']['write_type'] = write_type.strip('"')

            if line.startswith('write_partition='):
                # Parse the write_partition argument, treat it as a list
                write_partition = line.split('write_partition=')[1].strip().rstrip(',')
                yaml_structure['transform']['write_partition'] = eval(write_partition)

            if line.startswith('owner='):
                # Parse the owner argument
                owner = line.split('owner=')[1].strip().rstrip(',').strip('"')
                yaml_structure['transform']['owner'] = owner

        return yaml_structure

# Usage
if __name__ == "__main__":
    converter = PythonToYAMLConverter('ingest_to_raw_transformation.py')
    converter.convert_python_to_yaml()