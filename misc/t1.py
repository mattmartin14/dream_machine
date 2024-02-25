"""
    Testing unit tests
    dont try to run in python notebook
"""

import unittest

def add(a, b):
    return a + b

class TestAddFunction(unittest.TestCase):
    def test_add_positive_numbers(self):
        self.assertEqual(add(3, 4), 7, "Should be 7")

    def test_add_negative_numbers(self):
        self.assertEqual(add(-1, -1), -2, "Should be -2")

    def test_add_zero(self):
        self.assertEqual(add(0, 0), 0, "Should be 0")

    def test_add_positive_and_negative(self):
        self.assertEqual(add(-5, 5), 0, "Should be 0")

    def test_add_with_floats(self):
        self.assertAlmostEqual(add(0.1, 0.2), 0.3, places=1, msg="Should be approximately 0.3")

if __name__ == '__main__':
    unittest.main()
