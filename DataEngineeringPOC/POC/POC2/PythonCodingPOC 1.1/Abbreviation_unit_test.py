import unittest
from Abbreviation import AbbreviationForBrandProcessName

class TestAbbreviationForBrandProcessName(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        self.abbreviator = AbbreviationForBrandProcessName()

    def test_remove_special_characters(self):
        self.assertEqual(self.abbreviator.remove_special_characters(
            "Natural Stone, Marble, Slate, Polished Concrete, Honed or Tumbled Surfaces"),
            "Natural Stone Marble Slate Polished Concrete Honed or Tumbled Surfaces")

    def test_remove_noise_words(self):
        self.assertEqual(self.abbreviator.remove_noise_words(
        "Natural Stone Marble Slate Polished Concrete Honed or Tumbled Surfaces"),
        "Natural Stone Marble Slate Polished Concrete Honed Tumbled Surfaces")

    def test_change_plurals_to_singular(self):
        self.assertEqual(self.abbreviator.change_plurals_to_singular(
            "Natural Stone Marble Slate Polished Concrete Honed Tumbled Surfaces"),
            "Natural Stone Marble Slate Polished Concrete Honed Tumbled Surface")

    def test_remove_vowels_from_word(self):
        self.assertEqual(self.abbreviator.remove_vowels_from_word(
            "Natural Stone Marble Slate Polished Concrete Honed Tumbled Surfaces"),
            "Natural Stone Marble Slate Polished Concrete Hnd Tmbld Srfc")

    def test_abbreviate_words_using_diff_techniques_is_vowel(self):
        input_string = "hello world"
        expected_output = "hll wrld"
        self.assertEqual(self.abbreviator.abbreviate_words_using_diff_techniques(input_string), expected_output)

    def test_abbreviate_words_with_repeated_consonants(self):
        input_string = "Letter"
        expected_output = "ltr"
        self.assertEqual(self.abbreviator.abbreviate_words_using_diff_techniques(input_string), expected_output)

    def test_abbreviate_words_using_diff_techniques_with_length_of_string(self):
        long_string = "this is a very long string that needs to be abbreviated"
        self.assertLessEqual(len(self.abbreviator.abbreviate_words_using_diff_techniques(long_string)), 60)


