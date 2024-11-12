import pandas as pd
import os
from datetime import datetime
from pyspark.sql import SparkSession


class FeatureDataProcessor:
    """
    It reads input_file (.csv) contains Feature values(FEATURE*)
    and Feature score values(FEATURE_SCORE* value from 1 to 5),
    """

    def __init__(self, input_file):
        self.input_file = input_file
        self.data = None

    def read_data(self):
        """
        Reads the .csv file and validate the columns
        """
        try:
            self.data = pd.read_csv(self.input_file)
            self.validate_columns()
        except Exception as e:
            raise ValueError(f"Error at reading the data: {e}")

    def validate_columns(self):
        """
        Validates the structure of the input data.
        """
        required_Columns = ['REF_ID', 'payment_date', 'ORIG', 'BENEF',
                            'FEATURE1', 'FEATURE1_Score', 'FEATURE2', 'FEATURE2_Score',
                            'FEATURE3', 'FEATURE3_Score', 'FEATURE4', 'FEATURE4_Score',
                            'FEATURE5', 'FEATURE5_Score', 'total_score']

        for column in required_Columns:
            if column not in self.data.columns:
                raise ValueError(f"The Required Columns are missing: {column}")
        return

    def calculate_total_score(self):
        """
        Calculate TOTAL_Score based on FEATURE*_Score columns.
        """
        try:
            score_columns = [f'FEATURE{i}_Score' for i in range(1, 6)]

            missing_columns = [col for col in score_columns if col not in self.data.columns]
            if missing_columns:
                raise ValueError(f"Missing columns: {', '.join(missing_columns)}")
            # Calculate total score
            self.data['TotalScoreOfAllFeaturescore'] = self.data[score_columns].sum(axis=1)
            # print(self.data['TotalScoreOfAllFeaturescore'])

        except Exception as e:
            raise ValueError(f"Error Calculating total score: {e}")

    def add_month(self):
        """
        Extract month from PAYMENT_DATE and add it to the dataframe.
        """
        try:
            self.data['payment_date'] = pd.to_datetime(self.data['payment_date'],
                                                       format='%d-%m-%Y', infer_datetime_format=True)
            self.data['Month'] = self.data['payment_date'].dt.month
            # print(self.data.columns)

        except Exception as e:
            raise ValueError(f"An error occurred while extracting month column from payment_date: {e}")

        return ''

    def generate_alerts(self):
        """Generates alerts based on the filtered data."""
        try:
            filtered_data = self.data[self.data['TotalScoreOfAllFeaturescore'] > 15]
            grouped_data = filtered_data.groupby(['ORIG', 'BENEF'])
            alerts = []
            for (orig, benef), group in grouped_data:
                top_features = self.get_top_features(group)
                alert = self.create_alert(group, top_features, orig)
                alerts.append(alert)
            return alerts
        except Exception as e:
            raise ValueError(f"Error generating alerts: {e}")

    @staticmethod
    def get_top_features(group):
        """Gets the top 3 features based on the score."""
        try:
            score_columns = [f'FEATURE{i}_Score' for i in range(1, 6)]
            feature_columns = [f'FEATURE{i}' for i in range(1, 6)]
            top_features = []
            for feature, score in zip(feature_columns, score_columns):
                top_features.append((feature, group[feature].iloc[0], group[score].max()))
            top_features.sort(key=lambda x: x[2], reverse=True)
            return top_features[:3]
        except Exception as e:
            raise ValueError(f"Error getting top features: {e}")

    @staticmethod
    def create_alert(group, top_features, orig):
        """Creates an alert based on the group and top features."""
        try:
            alert = {
                'REF_ID': group['REF_ID'].iloc[0],
                'ORIG': orig,
                'BENEF': group['BENEF'].iloc[0],
                'top_feat1': top_features[0][0],
                'top_feat1_value': top_features[0][1],
                'top_feat1_score': top_features[0][2],
                'top_feat2': top_features[1][0],
                'top_feat2_value': top_features[1][1],
                'top_feat2_score': top_features[1][2],
                'top_feat3': top_features[2][0],
                'top_feat3_value': top_features[2][1],
                'top_feat3_score': top_features[2][2],
                'TOTAL_Score': group['TotalScoreOfAllFeaturescore'].max(),
                'PAYMENT_DATE': group['payment_date'].min(),
                'MONTH': group['Month'].min(),
                'group': 0,  # Assuming group ID is 0 for connected components
                'ALERT_KEY': orig,
                'alert_top_feat1': top_features[0][0],
                'alert_top_feat1_value': top_features[0][1],
                'alert_top_feat1_score': top_features[0][2],
                'alert_top_feat2': top_features[1][0],
                'alert_top_feat2_value': top_features[1][1],
                'alert_top_feat2_score': top_features[1][2],
                'alert_top_feat3': top_features[2][0],
                'alert_top_feat3_value': top_features[2][1],
                'alert_top_feat3_score': top_features[2][2]
            }
            return alert
        except Exception as e:
            raise ValueError(f"Error creating alert: {e}")

    def run(self):
        """Runs the entire process."""
        try:
            self.read_data()
            self.calculate_total_score()
            self.add_month()
            alerts = self.generate_alerts()
            return alerts
        except Exception as e:
            raise ValueError(f"Error running the process: {e}")


if __name__ == '__main__':
    input_file = 'input_file.csv'
    processor_data = FeatureDataProcessor(input_file)
    alerts = processor_data.run()
    output_file = 'output_payments.csv'
    pd.DataFrame(alerts).to_csv(output_file, index=False)