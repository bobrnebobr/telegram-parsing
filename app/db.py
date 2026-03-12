import sqlite3


class RegistryDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.create()

    def create(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ed_number INTEGER,
            file_number INTEGER,
            post_id INTEGER,
            post_date TEXT,
            year TEXT,
            month TEXT,
            doc_type TEXT,
            title TEXT,
            file_name TEXT,
            file_modified TEXT,
            file_size INTEGER,
            file_format TEXT,
            sha256 TEXT,
            relative_path TEXT
        )
        """)

    def insert(self, row):
        self.conn.execute("""
        INSERT INTO records(
            ed_number,file_number,
            post_id,post_date,
            year,month,
            doc_type,title,
            file_name,file_modified,file_size,
            file_format,sha256,relative_path
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, row)
        self.conn.commit()

    def fetch_month(self, year, month):
        cur = self.conn.execute("""
        SELECT
            ed_number,file_number,post_id,post_date,
            doc_type,title,file_name,file_modified,
            file_size,file_format,sha256,relative_path
        FROM records
        WHERE year=? AND month=?
        ORDER BY id
        """, (year, month))
        return cur.fetchall()

    def fetch_year(self, year):
        cur = self.conn.execute("""
        SELECT
            ed_number,file_number,post_id,post_date,
            doc_type,title,file_name,file_modified,
            file_size,file_format,sha256,relative_path
        FROM records
        WHERE year=?
        ORDER BY id
        """, (year,))
        return cur.fetchall()

    def delete_month(self, year, month):
        self.conn.execute("DELETE FROM records WHERE year=? AND month=?", (year, month))
        self.conn.commit()