import os
import sqlite3
import logging

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
DB_PATH = os.path.join(os.path.dirname(__file__), 'tracks.db')

def clear_database():
    logging.info(f"Attempting to delete database file at {DB_PATH}")
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logging.info('Database file deleted.')
    else:
        logging.warning('Database file does not exist.')

if __name__ == "__main__":
    clear_database()
