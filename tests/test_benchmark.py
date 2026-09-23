import unittest

from scripts.benchmark import percentile, run_benchmark


class BenchmarkTests(unittest.TestCase):
    def test_percentiles(self):
        values = list(range(1, 101))
        self.assertEqual(percentile(values, 0.50), 50)
        self.assertEqual(percentile(values, 0.95), 95)

    def test_shared_dataset_is_refused(self):
        with self.assertRaisesRegex(ValueError, "immutable archive"):
            run_benchmark(10, 1, disposable_dataset=False)


if __name__ == "__main__":
    unittest.main()
