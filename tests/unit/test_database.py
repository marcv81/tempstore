import tempstore.database as ts_db

import unittest

DATABASE_DIR = 'database-test'

# Sample SHA-256 hashes.
SHA256_TEST1 = 'e6f96beba7edddcbe06e2b526419ab151300fc271ee13f42eb11ee45f74dd152'
SHA256_TEST2 = '245a80eeee4c1c2b2cc7e6b921c7a71c36c39a22bbd8ef5613fe414b0c9f74a4'

class TestDatabase(unittest.TestCase):

    def setUp(self):
        self.database = ts_db.Database(DATABASE_DIR)
        self.database.create()

    def tearDown(self):
        self.database.delete()

    def test_create_file(self):

        # Fails to create a file with an invalid project name,
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.create_file(
                'Project?', '1.0', 'fileA', SHA256_TEST1)
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to create a file with an invalid version name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.create_file(
                'ProjectX', '1/2', 'fileA', SHA256_TEST1)
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to create a file with an invalid file name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.create_file(
                'ProjectX', '1.0', '..', SHA256_TEST1)
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to create a file with an invalid SHA-256 hash.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.create_file(
                'ProjectX', '1.0', 'fileA', 'abcd')
        self.assertEqual('Invalid SHA-256 hash', str(e.exception))

        # Succeeds to create a file, new project and version.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)

        # Succeeds to create a file, existing project and version.
        self.database.create_file(
            'ProjectX', '1.0', 'fileB', SHA256_TEST2)

        # Fails to create a duplicate file.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.create_file(
                'ProjectX', '1.0', 'fileA', SHA256_TEST1)
        self.assertEqual('Unable to create file', str(e.exception))

    def test_retrieve_file_sha256(self):

        # Fails to retrieve a file with an invalid project name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_file_sha256(
                'Project!', '1.0', 'fileA')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve a file with an invalid version name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_file_sha256(
                'ProjectX', '', 'fileA')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve a file with an invalid file name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_file_sha256(
                'ProjectX', '1.0', 'file:A')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve a non-existent file.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_file_sha256(
                'ProjectX', '1.0', 'fileA')
        self.assertEqual('File not found', str(e.exception))

        # Creates two files.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)
        self.database.create_file(
            'ProjectX', '1.0', 'fileB', SHA256_TEST2)

        # Retrieves the SHA-256 hash of the first file.
        self.assertEqual(
            self.database.retrieve_file_sha256(
                'ProjectX', '1.0', 'fileA'),
            SHA256_TEST1)

        # Retrieves the SHA-256 hash of the second file.
        self.assertEqual(
            self.database.retrieve_file_sha256(
                'ProjectX', '1.0', 'fileB'),
            SHA256_TEST2)

    def test_retrieve_projects(self):

        # The projects list is initially empty.
        projects = self.database.retrieve_projects()
        projects_names = [project['name'] for project in projects]
        self.assertEqual(projects_names, [])

        # Creates a project.
        self.database.create_file(
            'ProjectY', '1.0', 'fileA', SHA256_TEST1)

        # The projects list contains one project.
        projects = self.database.retrieve_projects()
        projects_names = [project['name'] for project in projects]
        self.assertEqual(projects_names, ['ProjectY'])

        # Creates another project.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)

        # The projects list contains two projects.
        # They are sorted alphabetically.
        projects = self.database.retrieve_projects()
        projects_names = [project['name'] for project in projects]
        self.assertEqual(projects_names, ['ProjectX', 'ProjectY'])

    def test_retrieve_versions(self):

        # Fails to retrieve versions for an invalid project name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_versions('Project?')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve versions for a non-existent project.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_versions('ProjectX')
        self.assertEqual('Project not found', str(e.exception))

        # Creates a version.
        self.database.create_file(
            'ProjectX', '1.1', 'fileA', SHA256_TEST1, 60)

        # The versions list contains one unstarred version.
        versions = self.database.retrieve_versions('ProjectX')
        versions_names = [version['name'] for version in versions]
        versions_stars = [version['star'] for version in versions]
        self.assertEqual(versions_names, ['1.1'])
        self.assertEqual(versions_stars, [False])

        # Creates two other versions.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1, 120)
        self.database.create_file(
            'ProjectX', '1.2', 'fileA', SHA256_TEST1, 0)

        # The versions list contains three unstarred versions.
        # They are sorted in reverse chronological order.
        versions = self.database.retrieve_versions('ProjectX')
        versions_names = [version['name'] for version in versions]
        versions_stars = [version['star'] for version in versions]
        self.assertEqual(versions_names, ['1.2', '1.1', '1.0'])
        self.assertEqual(versions_stars, [False, False, False])

        # Stars the 1.0 version.
        self.database.update_star('ProjectX', '1.0', True)

        # Verifies that the 1.0 version is now starred.
        versions = self.database.retrieve_versions('ProjectX')
        versions_names = [version['name'] for version in versions]
        versions_stars = [version['star'] for version in versions]
        self.assertEqual(versions_names, ['1.2', '1.1', '1.0'])
        self.assertEqual(versions_stars, [False, False, True])

    def test_retrieve_files(self):

        # Fails to retrieve files for an invalid project name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_files('Project$', '1.0')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve files for an invalid version name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_files('ProjectX', '*')
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to retrieve files for a non-existent version.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.retrieve_files('ProjectX', '1.0')
        self.assertEqual('Version not found', str(e.exception))

        # Creates a file.
        self.database.create_file(
            'ProjectX', '1.0', 'fileB', SHA256_TEST2)

        # The files list contains one file and SHA-256 hash.
        files = self.database.retrieve_files('ProjectX', '1.0')
        files_names = [file['name'] for file in files]
        files_sha256s = [file['sha256'] for file in files]
        self.assertEqual(files_names, ['fileB'])
        self.assertEqual(files_sha256s, [SHA256_TEST2])

        # Creates two other files.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)
        self.database.create_file(
            'ProjectX', '1.0', 'fileC', SHA256_TEST1)

        # The files list contains three files and SHA-256 hashes.
        # They are sorted alphabetically.
        files = self.database.retrieve_files('ProjectX', '1.0')
        files_names = [file['name'] for file in files]
        files_sha256s = [file['sha256'] for file in files]
        self.assertEqual(
            files_names,
            ['fileA', 'fileB', 'fileC'])
        self.assertEqual(
            files_sha256s,
            [SHA256_TEST1, SHA256_TEST2, SHA256_TEST1])

    def test_retrieve_sha256s(self):

        # Creates a file.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)

        # The SHA-256 hashes list contains the fist hash.
        sha256s = self.database.retrieve_sha256s()
        self.assertTrue(SHA256_TEST1 in sha256s)
        self.assertFalse(SHA256_TEST2 in sha256s)

        # Creates another file.
        self.database.create_file(
            'ProjectY', '1.1', 'fileA', SHA256_TEST2)

        # The SHA-256 hashes list contains both hashes.
        sha256s = self.database.retrieve_sha256s()
        self.assertTrue(SHA256_TEST1 in sha256s)
        self.assertTrue(SHA256_TEST2 in sha256s)

    def test_update_star(self):

        # Fails to star an invalid project name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.update_star('<Project>', '1.0', True)
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to star an invalid version name.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.update_star('Project', '?', True)
        self.assertEqual('Invalid name', str(e.exception))

        # Fails to set a star to an invalid state.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.update_star('ProjectX', '1.0', 1)
        self.assertEqual('Invalid star state', str(e.exception))

        # Fails to star a non-existent version.
        with self.assertRaises(ts_db.DatabaseException) as e:
            self.database.update_star('ProjectX', '1.0', True)
        self.assertEqual('Version not found', str(e.exception))

        # Creates a file.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1)

        # Succeeds to star/unstar in various circumstances.
        self.database.update_star('ProjectX', '1.0', True)
        self.database.update_star('ProjectX', '1.0', True)
        self.database.update_star('ProjectX', '1.0', False)

    def test_delete_obsolete_versions(self):

        # Creates two projects with two versions per project.
        # 1.0 versions: 60 seconds old, will become obsolete.
        # 2.0 versions: 20 seconds old, won't become obsolete.
        self.database.create_file(
            'ProjectX', '1.0', 'fileA', SHA256_TEST1, 60)
        self.database.create_file(
            'ProjectX', '2.0', 'fileA', SHA256_TEST1, 20)
        self.database.create_file(
            'ProjectY', '1.0', 'fileA', SHA256_TEST1, 60)
        self.database.create_file(
            'ProjectY', '2.0', 'fileA', SHA256_TEST1, 20)

        # Stars the ProjectX versions.
        self.database.update_star('ProjectX', '1.0', True)
        self.database.update_star('ProjectX', '2.0', True)

        # Deletes the unstarred versions older than 40 seconds.
        self.database.delete_obsolete_versions(40)

        # The versions list for ProjectX contains two starred versions.
        versions = self.database.retrieve_versions('ProjectX')
        versions_names = [version['name'] for version in versions]
        versions_stars = [version['star'] for version in versions]
        self.assertEqual(versions_names, ['2.0', '1.0'])
        self.assertEqual(versions_stars, [True, True])

        # The versions list for ProjectY contains one unstarred version.
        versions = self.database.retrieve_versions('ProjectY')
        versions_names = [version['name'] for version in versions]
        versions_stars = [version['star'] for version in versions]
        self.assertEqual(versions_names, ['2.0'])
        self.assertEqual(versions_stars, [False])
