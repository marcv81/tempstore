import tempstore.database as ts_db
import tempstore.datastore as ts_ds

import datetime
import time

# Combines a database and a datastore to handle projects,
# versions, files, blobs, and their associated metadata.
class Engine:

    def __init__(self, datastore_dir, database_dir, obsolete_age):
        self.datastore = ts_ds.Datastore(datastore_dir)
        self.database = ts_db.Database(database_dir)
        self.obsolete_age = obsolete_age

    # Creates or resets the datastore and database.
    def create(self):
        self.datastore.create()
        self.database.create()

    # Deletes the datastore and database.
    def delete(self):
        self.datastore.delete()
        self.database.delete()

    # Lists all the projects.
    def list_projects(self):
        projects = self.database.retrieve_projects()
        return projects

    # Lists all the versions for a project.
    def list_versions(self, project_name):
        # Retrieves the versions from the database.
        versions = self.database.retrieve_versions(project_name)
        # Formats nicely the date and the time until expiry.
        now = int(time.time())
        for version in versions:
            timestamp = version['timestamp']
            date = datetime.datetime.fromtimestamp(timestamp)
            date = date.strftime('%Y-%m-%d')
            if not version['star']:
                expiry = timestamp + self.obsolete_age - now
                date += ', ' + format_expiry(expiry)
            version['date'] = date
        # Returns the versions
        return versions

    # Lists all the files for a version.
    def list_files(self, project_name, version_name):
        files = self.database.retrieve_files(project_name, version_name)
        return files

    # Uploads a file.
    # The age in seconds should only be specified when testing.
    def upload(self,
            project_name, version_name, file_name, stream, age=0):
        # Writes the stream to a datastore blob.
        sha256 = self.datastore.create_blob(stream, age)
        # Creates a file in the database.
        self.database.create_file(
            project_name, version_name, file_name, sha256, age)

    # Downloads a file.
    def download(self, project_name, version_name, file_name):
        # Retrieves the file SHA-256 hash from the database.
        sha256 = self.database.retrieve_file_sha256(
            project_name, version_name, file_name)
        # Returns a stream from the datastore blob.
        stream = self.datastore.retrieve_blob(sha256)
        return stream

    # Stars a version.
    def star_version(self, project_name, version_name):
        self.database.update_star(project_name, version_name, True)

    # Unstars a version.
    def unstar_version(self, project_name, version_name):
        self.database.update_star(project_name, version_name, False)

    # Cleans up the obsolete database versions
    # and the unreferenced datastore blobs.
    def cleanup(self):
        # Deletes the obsolete versions from the database.
        self.database.delete_obsolete_versions(self.obsolete_age)
        # Retrieves the list of remaining SHA-256 hashes.
        sha256s = self.database.retrieve_sha256s()
        # Deletes the unreferenced blobs from the datastore.
        self.datastore.delete_unreferenced_blobs(sha256s)

# Formats nicely the time until expiry.
def format_expiry(expiry):

    def format_unit(expiry, unit):
        plural = 's' if expiry > 1 else ''
        return str(expiry) + ' ' + unit + plural

    def format_approximate(expiry):
        if expiry >= 60:
            expiry = (expiry + 30) // 60
        else:
            return format_unit(expiry, 'second')
        if expiry >= 60:
            expiry = (expiry + 30) // 60
        else:
            return format_unit(expiry, 'minute')
        if expiry >= 24:
            expiry = (expiry + 12) // 24
        else:
            return format_unit(expiry, 'hour')
        return format_unit(expiry, 'day')

    if expiry <= 0:
        return 'expired'
    else:
        return 'expires in ' + format_approximate(expiry)
