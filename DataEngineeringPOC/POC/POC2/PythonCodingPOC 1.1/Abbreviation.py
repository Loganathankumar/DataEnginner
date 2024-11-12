import sys
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


class AbbreviationForBrandProcessName:
    """
    This class should be used to abbreviate brand names, based on the requirements provided.
    Except for the standard brand names, all the values we suggest should be no more than
    60 characters. Anything that is more than sixty should be shortened and proposed.
    If value more than 60 char; Follow the below rules!
    """
    # Define Noise words
    def __init__(self):
        self.noise_words = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', "you're",
                            "you've", "you'll", "you'd", 'your', 'yours', 'yourself', 'yourselves', 'he',
                            'him', 'his', 'himself', 'she', "she's", 'her', 'hers', 'herself', 'it', "it's",
                            'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what',
                            'which', 'who', 'whom', 'this', 'that', "that'll", 'these', 'those', 'am', 'is',
                            'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having',
                            'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or',
                            'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about',
                            'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above',
                            'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under',
                            'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why',
                            'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some',
                            'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very',
                            's', 't', 'can', 'will', 'just', 'don', "don't", 'should', "should've", 'now',
                            'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', 'aren', "aren't", 'couldn',
                            "couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn',
                            "hasn't", 'haven', "haven't", 'isn', "isn't", 'ma', 'mightn', "mightn't",
                            'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn',
                            "shouldn't", 'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn',
                            "wouldn't"]

    def remove_special_characters(self, word):
        """
        abbreviated word: This function is used to Remove all the special characters
        from the given brand_name
        """
        word = re.sub(r'[^a-zA-Z0-9\s.]+', '', word)
        return word

    def remove_noise_words(self, word):
        """
        This function is used to Discard or Remove any noise words and return a
        cleaned-up version from the given brand name
        """
        word = word.split()
        cleaned_word = [w for w in word if w.lower() not in self.noise_words]
        return ' '.join(cleaned_word)

    def change_plurals_to_singular(self, word):
        """
        This function is used to change words from plural form and
        return singular from as word
        """
        changed_words = self.remove_noise_words(word)
        if changed_words.endswith('s'):
            return changed_words[:-1]

        return changed_words

    def remove_vowels_from_word(self, word):
        """
        This function is used to Take off the vowels from the words; i.e. ‘a’, ‘e’, ‘i’, ‘o’, ‘u’.
        of last 3 characters from the given brand_name
        """
        sentence = self.change_plurals_to_singular(word)
        sentence = sentence.split()
        last_three_words = sentence[-3:]
        vowels = "aeiouAEIOU"
        modified_last_three = [''.join([char for char in sentence if char not in vowels]) for sentence in last_three_words]
        modified_sentence = ' '.join(sentence[:-3] + modified_last_three)
        return modified_sentence

    def abbreviate_words_using_diff_techniques(self, words):
        try:
            # Split into words
            words = words.split()
            result = []

            # This Function to check if a character is a vowel
            def is_vowel(char):
                return char.lower() in 'aeiou'

            for word in words:
                # If the word has 3 or less letters, do not abbreviate
                if len(word) <= 3:
                    result.append(word)
                    continue

                # Check if the first letter or first two letters are vowels
                if is_vowel(word[0]) or (len(word) > 1 and is_vowel(word[1])):
                    result.append(word)
                    continue

                # Abbreviate repeated consonants and remove vowels
                abbreviated_word = ""
                previous_char = ""
                count = 0

                for char in word:
                    if char.lower() == previous_char.lower():
                        count += 1
                    else:
                        if count > 1:  # If there were repeated consonants, abbreviate them
                            abbreviated_word += previous_char + str(count)
                        elif previous_char != "":
                            abbreviated_word += previous_char

                        previous_char = char
                        count = 1

                # Handle the last character(s)
                if count > 1:
                    abbreviated_word += previous_char + str(count)
                else:
                    abbreviated_word += previous_char

                # Remove vowels from the abbreviation
                abbreviated_word = ''.join([c for c in abbreviated_word if not is_vowel(c)])

                # Limit to maximum of 5 characters
                abbreviated_word = abbreviated_word[:5]

                result.append(abbreviated_word)

            final_result = ' '.join(result)

            # Stop abbreviating if total length is less than or equal to 60
            if len(final_result) <= 60:
                return final_result

            return ' '.join(result)[:60]  # Return only up to the first 60 characters

        except Exception as e:
            return str(e)


# ------------------------------------------------------------------------------------- #
#
# # Create an instance of Abbreviation
# obj = AbbreviationForBrandProcessName()
#
# # Test the Abbreviation function
# brand_name = 'Natural Stone, Marble, Slate, Polished Concrete, Honed or Tumbled Surfaces'
# brand_name = brand_name.replace(',', '')
# removedSpecialCharacters = obj.remove_special_characters(brand_name)
# removedNoiseVoice = obj.remove_noise_words(brand_name)
# changedPluralsToSingular = obj.change_plurals_to_singular(brand_name)
# removedVowelsFromWords = obj.remove_vowels_from_word(brand_name)
# abbreviate_words = obj.abbreviate_words_using_diff_techniques(brand_name)
#
#
# print(removedSpecialCharacters)
# print(removedNoiseVoice)
# print(changedPluralsToSingular)
# print(removedVowelsFromWords)
# print(abbreviate_words)

