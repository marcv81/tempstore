import tempstore.datastore as ts_ds

import io
import os
import threading
import unittest

import util

DATASTORE_DIR = 'datastore-test'

# 10 Mb of test content.
CONTENT_TEST = os.urandom(1 * 1024 * 1024)

class TestDatastoreParallel(unittest.TestCase):

    def setUp(self):
        self.datastore = ts_ds.Datastore(DATASTORE_DIR)
        self.datastore.create()

    def tearDown(self):
        self.datastore.delete()

    # Thread to create a blob, then retrieve and verify its contents.
    # Counts the number of attemtps and successes.
    class CreateRetrieveBlobThread(threading.Thread):

        def __init__(self, total_counter, success_counter):
            threading.Thread.__init__(self)
            self.total_counter = total_counter
            self.success_counter = success_counter

        def run(self):
            self.total_counter.increment()
            datastore = ts_ds.Datastore(DATASTORE_DIR)
            sha256 = datastore.create_blob(io.BytesIO(CONTENT_TEST))
            with datastore.retrieve_blob(sha256) as stream:
                if stream.read(-1) == CONTENT_TEST:
                    self.success_counter.increment()

    # Tests that multiple parallel threads creating and retrieving
    # the same blob do not interfere with each other.
    def test_parallel_create_blob(self):
        total_counter = util.Counter()
        success_counter = util.Counter()
        # Try 100 times.
        for i in range(100):
            threads = []
            # Try 100 parallel threads.
            for j in range(50):
                threads.append(self.CreateRetrieveBlobThread(
                    total_counter, success_counter))
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            # Verifies all attempts were successful.
            self.assertEqual(
                success_counter.value(), total_counter.value())
            # Retrieves and verifies the blob one last time.
            datastore = ts_ds.Datastore(DATASTORE_DIR)
            sha256 = ts_ds.sha256_sum(io.BytesIO(CONTENT_TEST))
            with datastore.retrieve_blob(sha256) as stream:
                self.assertEqual(stream.read(-1), CONTENT_TEST)
