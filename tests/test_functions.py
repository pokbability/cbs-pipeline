import unittest
import os
from src.functions import _mask_generic_str, _hash_string
from unittest.mock import patch

class TestFunctions(unittest.TestCase):

    def test_mask_generic_str(self):
        self.assertEqual(_mask_generic_str("1234"), "****")  # All characters masked
        self.assertEqual(_mask_generic_str("12345"), "12*45")  # Middle characters masked
        self.assertEqual(_mask_generic_str("12"), "**")  # All characters masked
        self.assertEqual(_mask_generic_str("1"), "*")  # Single character masked
        self.assertEqual(_mask_generic_str(None), None)  # None input

    @patch.dict(os.environ, {'PII_SALT': 'test_salt'})
    def test_hash_string(self):
        hashed_value = _hash_string("test")
        self.assertIsNotNone(hashed_value)  # Ensure it returns a value
        self.assertNotEqual(hashed_value, _hash_string("different"))  # Different inputs should yield different hashes
        self.assertEqual(hashed_value, _hash_string("test"))  # Same input should yield the same hash
        self.assertEqual(_hash_string(None), None)  # None input

if __name__ == '__main__':
    unittest.main()