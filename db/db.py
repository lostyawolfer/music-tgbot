import os
import sqlite3
import datetime


class Music:
    def createdb(self):
        con = sqlite3.connect(os.path.join('db','music.db'))
        cur = con.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS music(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT,
            file_id TEXT
            )
        ''')
        con.commit()

    def add_data(self, video_id, file_id):
        con = sqlite3.connect(os.path.join('db','music.db'))
        cur = con.cursor()
        cur.execute('INSERT INTO music(video_id, file_id) VALUES(?, ?)',
                    (video_id, file_id))
        con.commit()

    def remove_data(self, video_id):
        con = sqlite3.connect(os.path.join('db', 'music.db'))
        cur = con.cursor()
        cur.execute('DELETE FROM music WHERE video_id=?', (video_id,)).fetchone()
        con.commit()

    def get_file_id(self, video_id):
        con = sqlite3.connect(os.path.join('db','music.db'))
        cur = con.cursor()
        value = cur.execute('SELECT file_id FROM music WHERE video_id=?', (video_id,)).fetchone()
        return value[0] if value else None