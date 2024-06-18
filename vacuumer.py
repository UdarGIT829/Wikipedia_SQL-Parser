import sqlite3
from utils import get_connection

def vacuum_database():
    conn = get_connection()
    conn.execute('VACUUM;')
    conn.close()

vacuum_database()