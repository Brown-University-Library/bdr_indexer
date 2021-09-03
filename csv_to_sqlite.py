#!/usr/bin/env python
import sqlite3

def generate_sqlite(csv_path, sqlite_path):
    with open(csv_path, 'rb') as f:
        data = f.read().decode('utf8')
    db = sqlite3.connect(sqlite_path)
    cursor = db.cursor()
    cursor.execute('CREATE TABLE resource_types (pid TEXT NOT NULL UNIQUE, resource_type TEXT NOT NULL)')
    db.commit()
    #make sure the line isn't empty, and skip the first header line
    for line in [line for line in data.split('\n') if line.strip()][1:]:
        pid, resource_type = line.split(',')
        pid = pid.strip()
        resource_type = resource_type.strip()
        if pid and resource_type:
            cursor.execute('INSERT INTO resource_types (pid, resource_type) VALUES (?, ?)', (pid, resource_type))
    db.commit()
    db.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Generate primo resource type sqlite db from csv')
    parser.add_argument('--csv', dest='csv_path', help='CSV filename')
    parser.add_argument('--sqlite', dest='sqlite_path', help='name of sqlite db')
    args = parser.parse_args()

    if args.csv_path and args.sqlite_path:
        generate_sqlite(args.csv_path, args.sqlite_path)
    else:
        print(f'must pass in CSV and Sqlite arguments')
