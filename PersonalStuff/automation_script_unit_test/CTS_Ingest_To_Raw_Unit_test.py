import unittest

from autogenerate_ingest_to_raw_transformation import AutomatedIngestRawTransformation
from pyspark_test import pysparktest

class TestIngestToRawTransformation(PysparkTest, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
