import tempstore.datastore as ts_ds

import io
import os
import unittest

DATASTORE_DIR = 'datastore-test'

# SHA-256 hashes of the string 'foo' and the empty string.
SHA256_FOO = '2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae'
SHA256_EMPTY = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'

# 10 Mb of test content.
CONTENT_TEST1 = os.urandom(1 * 1024 * 1024)
CONTENT_TEST2 = os.urandom(1 * 1024 * 1024)

class TestSHA256Sum(unittest.TestCase):

    def test_sha256_sum(self):

        # Verifies SHA-256 hash of string 'foo'.
        self.assertEqual(
            ts_ds.sha256_sum(io.BytesIO(b'foo')), SHA256_FOO)

        # Verifies SHA-256 hash of empty string.
        self.assertEqual(
            ts_ds.sha256_sum(io.BytesIO(b'')), SHA256_EMPTY)

class TestDatastore(unittest.TestCase):

    def setUp(self):
        self.datastore = ts_ds.Datastore(DATASTORE_DIR)
        self.datastore.create()

    def tearDown(self):
        self.datastore.delete()

    def test_create_blob(self):

        # Creates a blob twice, the SHA-256 hashes match.
        sha256_1a = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST1))
        sha256_1b = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST1))
        self.assertEqual(sha256_1a, sha256_1b)

    def test_retrieve_blob(self):

        # Fails to retrieve blob for an invalid SHA-256 hash.
        with self.assertRaises(ts_ds.DatastoreException) as e:
            self.datastore.retrieve_blob('..')
        self.assertEqual('Invalid SHA-256 hash', str(e.exception))

        # Fails to retrieve blob for a non-existent SHA-256 hash.
        with self.assertRaises(ts_ds.DatastoreException) as e:
            self.datastore.retrieve_blob(SHA256_EMPTY)
        self.assertEqual('Blob not found', str(e.exception))

        # Creates two blobs.
        sha256_1 = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST1))
        sha256_2 = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST2))

        # Retrieves and verifies the first blob.
        with self.datastore.retrieve_blob(sha256_1) as stream:
            self.assertEqual(stream.read(-1), CONTENT_TEST1)

        # Retrieves and verifies the second blob.
        with self.datastore.retrieve_blob(sha256_2) as stream:
            self.assertEqual(stream.read(-1), CONTENT_TEST2)

    def test_delete_unreferenced_blobs(self):

        # Creates two blobs.
        sha256_1 = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST1), 120)
        sha256_2 = self.datastore.create_blob(
            io.BytesIO(CONTENT_TEST2), 120)

        # Deletes the unreferenced blobs.
        sha256s = set([sha256_2, SHA256_EMPTY])
        self.datastore.delete_unreferenced_blobs(sha256s)

        # Fails to retrieve the first blob, it was deleted.
        with self.assertRaises(ts_ds.DatastoreException) as e:
            self.datastore.retrieve_blob(sha256_1)
        self.assertEqual('Blob not found', str(e.exception))

        # Retrieves and verifies the second blob.
        with self.datastore.retrieve_blob(sha256_2) as stream:
            self.assertEqual(stream.read(-1), CONTENT_TEST2)
