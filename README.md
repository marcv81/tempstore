This project provides a temporary artifacts store. By default the uploaded
files are deleted after 30 days, but an admin can star them to keep them
forever.

# Setup

## Dependencies

    virtualenv -p python3 venv
    source venv/bin/activate
    pip3 install -r requirements.txt

## Self-tests

    python3 -m unittest discover tests/unit
    python3 -m unittest discover tests/parallel

## Bootstrap

Create the datastore and the database.

    python3 start.py --init

# Test usage

## Start the app

    gunicorn start:app

The user interface is at http://localhost:8000.

## Upload files

When the app is running you can upload artifacts with cURL.

    curl -sSf -o /dev/null -F "project=Test" -F "version=123" -F upload=@artifact.tgz http://localhost:8000/upload

## Cleanup

Remove the obsolete versions from the database and the unreferenced blobs
from the datastore.

    python3 start.py --cleanup
