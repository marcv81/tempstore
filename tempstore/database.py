import contextlib
import functools
import os
import re
import shutil
import sqlite3
import time

class DatabaseException(Exception):
    pass

# Checks that a value represents a valid project, version, or file name.
def validate_name(name):
    if name in ('.', '..'):
        raise DatabaseException('Invalid name')
    name_regex = re.compile('^[0-9a-zA-Z_.-]+$')
    if not name_regex.search(name):
        raise DatabaseException('Invalid name')

# Checks that a value represents a valid SHA-256 hash.
def validate_sha256(sha256):
    sha256_regex = re.compile('^[0-9a-f]{64}$')
    if not sha256_regex.search(sha256):
        raise DatabaseException('Invalid SHA-256 hash')

# Checks that a value represents a valid star state.
def validate_star(star):
    if star is not True and star is not False:
        raise DatabaseException('Invalid star state')

# SQLite-backed database to handle projects, versions, and files.
class Database:

    def __init__(self, database_dir):
        self.database_dir = database_dir
        self.database_file = os.path.join(
            self.database_dir, 'packages.db')

    # Creates or resets the database.
    def create(self):
        self.delete()
        os.mkdir(self.database_dir)
        self.create_schema()

    # Deletes the database.
    def delete(self):
        shutil.rmtree(self.database_dir, ignore_errors=True)

    # Initializes the database connection.
    def open(self):
        self.connection = sqlite3.connect(
            self.database_file, isolation_level=None)
        self.cursor = self.connection.cursor()
        self.cursor.execute('PRAGMA foreign_keys=ON')
        self.cursor.execute('PRAGMA journal_mode=WAL')
        self.cursor.execute('PRAGMA busy_timeout=10000')

    # Closes the database connection.
    def close(self):
        self.connection.close()

    # Decorator for the methods working on an open database. Opens the
    # database before use and closes it afterwards, even if the method
    # raised an exception.
    def database_context_manager(method):
        @contextlib.contextmanager
        def context_manager(database):
            database.open()
            yield database
            database.close()
        @functools.wraps(method)
        def wrapper(self, *args, **kwargs):
            with context_manager(self) as database:
                return method(database, *args, **kwargs)
        return wrapper

    # Creates the database schema.
    @database_context_manager
    def create_schema(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS projects(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                CONSTRAINT unique_project UNIQUE (name)
            )''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS versions(
                id INTEGER PRIMARY KEY,
                project_id INTEGER,
                name TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                star BOOLEAN DEFAULT 0,
                FOREIGN KEY(project_id) REFERENCES projects(id),
                CONSTRAINT unique_version UNIQUE (project_id, name)
            )
            ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                version_id INTEGER,
                name TEXT NOT NULL,
                sha256 TEXT NOT NULL,
                FOREIGN KEY(version_id) REFERENCES versions(id)
                    ON DELETE CASCADE,
                CONSTRAINT unique_file UNIQUE (version_id, name)
            )
            ''')

    # Creates a new file.
    # Automatically creates the project and version if required.
    # The age in seconds should only be specified when testing.
    @database_context_manager
    def create_file(
            self, project_name, version_name, file_name,
            sha256, age=0):
        # Validates the parameters.
        validate_name(project_name)
        validate_name(version_name)
        validate_name(file_name)
        validate_sha256(sha256)
        # Initializes the timestamp.
        timestamp = int(time.time()) - age
        # Starts a transaction.
        self.cursor.execute('BEGIN')
        # Creates the project if it does not exist.
        sql = 'INSERT OR IGNORE INTO projects(name) VALUES(?)'
        params = [project_name]
        self.cursor.execute(sql, params)
        # Retrieves the project.
        sql = 'SELECT id FROM projects WHERE name=?'
        params = [project_name]
        rows = list(self.cursor.execute(sql, params))
        assert len(rows) == 1
        project_id = rows[0][0]
        # Creates the version if it does not exist.
        sql = '''
            INSERT OR IGNORE
            INTO versions(project_id, name, timestamp)
            VALUES(?, ?, ?)
            '''
        params = [project_id, version_name, timestamp]
        self.cursor.execute(sql, params)
        # Retrieves the version.
        sql = '''
            SELECT id FROM versions
            WHERE project_id=? AND name=?
            '''
        params = [project_id, version_name]
        rows = list(self.cursor.execute(sql, params))
        assert len(rows) == 1
        version_id = rows[0][0]
        # Creates the file.
        sql = '''
            INSERT INTO files(version_id, name, sha256)
            VALUES(?, ?, ?)
            '''
        params = [version_id, file_name, sha256]
        try:
            self.cursor.execute(sql, params)
            self.cursor.execute('COMMIT')
        except sqlite3.IntegrityError:
            self.cursor.execute('ROLLBACK')
            raise DatabaseException('Unable to create file')

    # Retrieves the SHA-256 hash of a file.
    @database_context_manager
    def retrieve_file_sha256(self, project_name, version_name, file_name):
        # Validates the parameters.
        validate_name(project_name)
        validate_name(version_name)
        validate_name(file_name)
        # Retrieves the file.
        sql = '''
            SELECT files.sha256 FROM projects
            INNER JOIN versions ON projects.id=versions.project_id
            INNER JOIN files ON versions.id=files.version_id
            WHERE projects.name=? AND versions.name=? AND files.name=?
            '''
        params = [project_name, version_name, file_name]
        rows = list(self.cursor.execute(sql, params))
        if len(rows) != 1:
            raise DatabaseException('File not found')
        sha256 = rows[0][0]
        return sha256

    # Retrieves all the projects.
    # The results are in alphabetical order.
    @database_context_manager
    def retrieve_projects(self):
        sql = 'SELECT name FROM projects ORDER BY name ASC'
        rows = list(self.cursor.execute(sql))
        projects = [{
            'name': row[0]} for row in rows]
        return projects

    # Retrieves all the versions (name, date, star) for a project.
    # The results are sorted in reverse chronological order.
    @database_context_manager
    def retrieve_versions(self, project_name):
        # Validates the parameter.
        validate_name(project_name)
        # Starts a transaction.
        self.cursor.execute('BEGIN')
        # Retrieves the project.
        sql = 'SELECT id FROM projects WHERE name=?'
        params = [project_name]
        rows = list(self.cursor.execute(sql, params))
        if len(rows) != 1:
            self.cursor.execute('ROLLBACK')
            raise DatabaseException('Project not found')
        project_id = rows[0][0]
        # Retrieves the versions.
        sql = '''
            SELECT name, timestamp, star FROM versions
            WHERE project_id=? ORDER BY timestamp DESC
            '''
        params = [project_id]
        rows = list(self.cursor.execute(sql, params))
        versions = [{
            'name': row[0],
            'timestamp': row[1],
            'star': row[2]} for row in rows]
        # Commits the transaction.
        self.cursor.execute('COMMIT')
        return versions

    # Retrieves all the files (name, sha256) for a version.
    # The results are sorted in alphabetical order.
    @database_context_manager
    def retrieve_files(self, project_name, version_name):
        # Validates the parameters.
        validate_name(project_name)
        validate_name(version_name)
        # Starts a transaction.
        self.cursor.execute('BEGIN')
        # Retrieves the version.
        sql = '''
            SELECT versions.id FROM versions
            INNER JOIN projects ON projects.id=versions.project_id
            WHERE projects.name=? and versions.name=?
            '''
        params = [project_name, version_name]
        rows = list(self.cursor.execute(sql, params))
        if len(rows) != 1:
            self.cursor.execute('ROLLBACK')
            raise DatabaseException('Version not found')
        version_id = rows[0][0]
        # Retrieves the files.
        sql = '''
            SELECT name, sha256 FROM files
            WHERE version_id=? ORDER BY name ASC
            '''
        params = [version_id]
        rows = list(self.cursor.execute(sql, params))
        files = [{
            'name': row[0],
            'sha256': row[1]} for row in rows]
        # Commits the transaction.
        self.cursor.execute('COMMIT')
        return files

    # Retrieves all the known SHA-256 hashes.
    @database_context_manager
    def retrieve_sha256s(self):
        sql = 'SELECT DISTINCT sha256 FROM files'
        rows = list(self.cursor.execute(sql))
        return [row[0] for row in rows]

    # Star/unstar a version.
    @database_context_manager
    def update_star(self, project_name, version_name, star):
        # Validates the parameters.
        validate_name(project_name)
        validate_name(version_name)
        validate_star(star)
        # Starts a transaction.
        self.cursor.execute('BEGIN IMMEDIATE')
        # Retrieves the version.
        sql = '''
            SELECT versions.id FROM versions
            INNER JOIN projects ON projects.id=versions.project_id
            WHERE projects.name=? and versions.name=?
            '''
        params = [project_name, version_name]
        rows = list(self.cursor.execute(sql, params))
        if len(rows) != 1:
            self.cursor.execute('ROLLBACK')
            raise DatabaseException('Version not found')
        version_id = rows[0][0]
        # Updates the star.
        sql = 'UPDATE versions SET star=? WHERE id=?'
        params = [star, version_id]
        self.cursor.execute(sql, params)
        # Commits the transaction.
        self.cursor.execute('COMMIT')

    # Deletes the obsolete versions (i.e.: with no star
    # and older than the specified age in seconds).
    @database_context_manager
    def delete_obsolete_versions(self, age=0):
        # Initializes the timestamp.
        timestamp = int(time.time()) - age
        # Deletes the versions.
        sql = '''
            DELETE FROM versions
            WHERE star=? AND timestamp<=?
            '''
        params = [False, timestamp]
        self.cursor.execute(sql, params)
