import binascii
import hashlib
import os
import os.path
import re
import shutil
import time
import uuid

BUFFER_SIZE = 65536

class DatastoreException(Exception):
    pass

# Checks that a value represents a valid SHA-256 hash.
def validate_sha256(sha256):
    sha256_regex = re.compile('^[0-9a-f]{64}$')
    if not sha256_regex.search(sha256):
        raise DatastoreException('Invalid SHA-256 hash')

# Returns the SHA-256 hash of a stream.
def sha256_sum(stream):
    sha256 = hashlib.sha256()
    for buffer in iter(lambda: stream.read(BUFFER_SIZE), b''):
        sha256.update(buffer)
    return binascii.hexlify(sha256.digest()).decode()

# Filesystem-backed datastore.
class Datastore:

    def __init__(self, data_dir):
        self.data_dir = data_dir

    # Creates or resets the datastore.
    def create(self):
        self.delete()
        os.mkdir(self.data_dir)

    # Deletes the datastore.
    def delete(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    # Creates a blob. Returns its SHA-256 hash.
    # The age in seconds should only be specified when testing.
    def create_blob(self, stream, age=0):
        # Generates the actual and temporary files names.
        sha256 = sha256_sum(stream)
        file_path = os.path.join(self.data_dir, sha256)
        temp_file_path = file_path + '-' + uuid.uuid4().hex
        # Writes the stream to a temporary file.
        stream.seek(0)
        with open(temp_file_path, 'xb') as f:
            for buffer in iter(lambda: stream.read(BUFFER_SIZE), b''):
                f.write(buffer)
            f.flush()
            os.fsync(f.fileno())
        # Fix the temporary file timestamp.
        timestamp = int(time.time()) - age
        os.utime(temp_file_path, (timestamp, timestamp))
        # Replaces the actual file with the temporary file atomically.
        os.replace(temp_file_path, file_path)
        # Returns the SHA-256 hash.
        return sha256

    # Retrieves a blob from its SHA-256 hash. Returns a stream.
    # Raises an exception if the SHA-256 is invalid/unknown.
    def retrieve_blob(self, sha256):
        # Validates the parameter.
        validate_sha256(sha256)
        # Retrieves the blob.
        file_path = os.path.join(self.data_dir, sha256)
        try:
            return open(file_path, 'rb')
        except FileNotFoundError:
            raise DatastoreException('Blob not found')

    # Deletes the unreferenced blobs from the datastore.
    def delete_unreferenced_blobs(self, sha256s):
        now = int(time.time())
        for file_name in os.listdir(self.data_dir):
            file_path = os.path.join(self.data_dir, file_name)
            # Ignores the file if created less than 60 seconds ago,
            # it may not be referenced yet.
            if os.stat(file_path).st_mtime > now - 60:
                continue
            # Deletes the file unless referenced.
            if file_name not in sha256s:
                os.unlink(file_path)
