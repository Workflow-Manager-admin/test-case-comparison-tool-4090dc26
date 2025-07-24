#!/usr/bin/env python3
"""SQLite database access for test_case_database

Manages schema and provides insert/retrieve functionality for
test_case_files and test_cases tables.
"""

import sqlite3
from datetime import datetime
from typing import List, Optional, Tuple, Dict

DB_NAME = "test_cases.db"


def _connect():
    """Get a connection to the SQLite database, creating file if needed."""
    conn = sqlite3.connect(DB_NAME)
    return conn


def _init_schema():
    """Create tables if they do not exist."""
    conn = _connect()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_case_files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            upload_date TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_cases (
            tc_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            file_id INTEGER NOT NULL,
            FOREIGN KEY (file_id) REFERENCES test_case_files(file_id) ON DELETE CASCADE
        )
    """)
    conn.commit()
    conn.close()


# PUBLIC_INTERFACE
def initialize_database():
    """Initialize the SQLite schema for test case database.
    Idempotent: safe to call multiple times.
    """
    _init_schema()


# PUBLIC_INTERFACE
def insert_test_case_file(filename: str, upload_date: Optional[str] = None) -> int:
    """Insert a record into test_case_files.

    Args:
        filename: Name of the uploaded file.
        upload_date: Upload date (ISO string). If None, use current UTC.

    Returns:
        The file_id of the inserted row.
    """
    if upload_date is None:
        upload_date = datetime.utcnow().isoformat()
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO test_case_files (filename, upload_date) VALUES (?, ?)",
        (filename, upload_date)
    )
    file_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return file_id


# PUBLIC_INTERFACE
def insert_test_case(name: str, file_id: int) -> int:
    """Insert a record into test_cases.

    Args:
        name: Test case name.
        file_id: file_id referencing test_case_files.

    Returns:
        The tc_id of the inserted row.
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO test_cases (name, file_id) VALUES (?, ?)",
        (name, file_id)
    )
    tc_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return tc_id


# PUBLIC_INTERFACE
def get_test_case_files() -> List[Dict]:
    """Retrieve all test_case_files.

    Returns:
        List of dicts: each dict has keys file_id, filename, upload_date.
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute("SELECT file_id, filename, upload_date FROM test_case_files")
    rows = cursor.fetchall()
    conn.close()
    return [
        {"file_id": row[0], "filename": row[1], "upload_date": row[2]}
        for row in rows
    ]


# PUBLIC_INTERFACE
def get_test_cases_by_file(file_id: int) -> List[Dict]:
    """Retrieve all test_cases for a given file_id.

    Args:
        file_id: The file_id to filter test cases.

    Returns:
        List of dicts: each dict has keys tc_id, name, file_id.
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT tc_id, name, file_id FROM test_cases WHERE file_id=?",
        (file_id,)
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {"tc_id": row[0], "name": row[1], "file_id": row[2]}
        for row in rows
    ]


# PUBLIC_INTERFACE
def get_all_test_cases() -> List[Dict]:
    """Retrieve all test_cases.

    Returns:
        List of dicts: each dict has keys tc_id, name, file_id.
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT tc_id, name, file_id FROM test_cases"
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {"tc_id": row[0], "name": row[1], "file_id": row[2]}
        for row in rows
    ]


# PUBLIC_INTERFACE
def get_test_case_file_by_id(file_id: int) -> Optional[Dict]:
    """Retrieve a single test_case_file by file_id.

    Args:
        file_id: The file_id to look up.

    Returns:
        Dict of file info or None if not found.
    """
    conn = _connect()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT file_id, filename, upload_date FROM test_case_files WHERE file_id=?",
        (file_id,)
    )
    row = cursor.fetchone()
    conn.close()
    if row:
        return {"file_id": row[0], "filename": row[1], "upload_date": row[2]}
    return None


# Initialize schema at import
_init_schema()

if __name__ == "__main__":
    # Simple check/demo
    print("Running DB schema/status check...")
    initialize_database()
    print("Table status (files):", get_test_case_files())
    print("Table status (test_cases):", get_all_test_cases())
