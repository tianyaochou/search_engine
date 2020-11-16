import unittest
from search_engine.process import *


class TestFileProcess(unittest.TestCase):
    def test_tokenize_file(self):
        test_file_path = "tests/test.txt"

        def line_filter(line):
            if ":" in line:
                return ""
            else:
                return line

        tokens = tokenize_file(test_file_path, line_filter)
        expected_tokens = {"hello", "world", "doudou"}
        self.assertEqual(len(expected_tokens.difference(tokens)), 0)


if __name__ == "__main__":
    unittest.main()