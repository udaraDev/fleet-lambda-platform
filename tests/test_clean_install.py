import unittest

from scripts.verify_clean_install import require_pipeline_verification


class CleanInstallEvidenceTests(unittest.TestCase):
    def test_requires_passing_structured_output(self):
        self.assertEqual(
            require_pipeline_verification('{"result":"passed"}')['result'],
            'passed',
        )
        self.assertEqual(
            require_pipeline_verification(
                ' Container tools Creating\n Container tools Created\n'
                '{"result":"passed","checks":["health"]}\n'
            )['checks'],
            ['health'],
        )
        with self.assertRaisesRegex(AssertionError, 'valid JSON'):
            require_pipeline_verification('')
        with self.assertRaisesRegex(AssertionError, 'did not pass'):
            require_pipeline_verification('{"result":"failed"}')


if __name__ == '__main__':
    unittest.main()
