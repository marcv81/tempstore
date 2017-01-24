import tempstore.database as ts_db

import threading
import unittest

import util

DATABASE_DIR = 'database-test'

# Sample SHA-256 hash.
SHA256_TEST = 'e6f96beba7edddcbe06e2b526419ab151300fc271ee13f42eb11ee45f74dd152'

class TestDatabaseParallel(unittest.TestCase):

    def setUp(self):
        self.database = ts_db.Database(DATABASE_DIR)
        self.database.create()

    def tearDown(self):
        self.database.delete()

    # Thread to create a file.
    # Counts the number of attemtps and successes.
    class CreateFileThread(threading.Thread):

        def __init__(
                self, total_counter, success_counter,
                project_name, version_name, file_name):
            threading.Thread.__init__(self)
            self.total_counter = total_counter
            self.success_counter = success_counter
            self.project_name = project_name
            self.version_name = version_name
            self.file_name = file_name

        def run(self):
            self.total_counter.increment()
            database = ts_db.Database(DATABASE_DIR)
            database.create_file(
                self.project_name,
                self.version_name,
                self.file_name,
                SHA256_TEST)
            self.success_counter.increment()

    # Thread to delete the obsolete versions.
    class DeleteVersionsThread(threading.Thread):

        def run(self):
            database = ts_db.Database(DATABASE_DIR)
            database.delete_obsolete_versions()

    # Tests that deleting versions does not interfere with creating files.
    def test_parallel_create_delete(self):
        total_counter = util.Counter()
        success_counter = util.Counter()
        # Try 10 times.
        for i in range(10):
            threads = []
            # Try 100 parallel threads.
            for j in range(50):
                # Creates a file.
                threads.append(self.CreateFileThread(
                    total_counter, success_counter,
                    'Project', 'v' + str(j), 'file' + str(i)))
                # Deletes the obsolete versions.
                threads.append(self.DeleteVersionsThread())
            # Runs the 100 operations in parallel.
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            # Verifies all attempts to create a file were successful.
            self.assertEqual(
                success_counter.value(), total_counter.value())
