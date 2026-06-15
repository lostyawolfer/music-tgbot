import os
import sqlite3
import datetime
import time


class Music:
    def createdb(self):
        con = sqlite3.connect(os.path.join('db', 'music.db'))
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
        con = sqlite3.connect(os.path.join('db', 'music.db'))
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
        con = sqlite3.connect(os.path.join('db', 'music.db'))
        cur = con.cursor()
        value = cur.execute('SELECT file_id FROM music WHERE video_id=?', (video_id,)).fetchone()
        return value[0] if value else None


class Analytics:
    def createdb(self):
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS users(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER
                    )
                ''')
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS total_use_count(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    use_count INTEGER
                    )
                ''')
        # A per-user "premium download cooldown" tracker (used when COOKIES_ENABLED=true)
        cur.execute('''
                    CREATE TABLE IF NOT EXISTS premium_rate_limit(
                    user_id INTEGER PRIMARY KEY,
                    last_ts INTEGER
                    )
                ''')

        # Ensure the counter row exists exactly once
        row = cur.execute('SELECT COUNT(*) FROM total_use_count').fetchone()[0]
        if row == 0:
            cur.execute('INSERT INTO total_use_count(use_count) VALUES(?)', (0,))
        con.commit()

    def get_user_count(self):
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        return cur.execute('SELECT COUNT(*) FROM users').fetchone()[0]

    def add_user(self, user_id) -> bool:
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        if not cur.execute('SELECT user_id FROM users WHERE user_id=?', (user_id,)).fetchone():
            cur.execute('INSERT INTO users(user_id) VALUES(?)', (user_id,))
            con.commit()
            return True
        return False

    def get_total_use_count(self):
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        return cur.execute('SELECT use_count FROM total_use_count WHERE id=1').fetchone()[0]

    def increment_use_count(self):
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        cur.execute('UPDATE total_use_count SET use_count=use_count+1 WHERE id=1').fetchone()
        con.commit()

    # -------------------------------
    # Premium rate limit helpers
    # RATE_LIMIT is interpreted as "cooldown seconds between premium downloads per user".
    # If RATE_LIMIT <= 0: disabled.
    # -------------------------------
    def get_premium_wait_seconds(self, user_id: int, cooldown_seconds: int) -> int:
        if cooldown_seconds <= 0:
            return 0
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        row = cur.execute(
            'SELECT last_ts FROM premium_rate_limit WHERE user_id=?',
            (user_id,)
        ).fetchone()
        if not row:
            return 0
        last_ts = int(row[0] or 0)
        now = int(time.time())
        elapsed = now - last_ts
        wait = cooldown_seconds - elapsed
        return int(wait) if wait > 0 else 0

    def mark_premium_download(self, user_id: int):
        con = sqlite3.connect(os.path.join('db', 'analytics.db'))
        cur = con.cursor()
        now = int(time.time())
        cur.execute(
            'INSERT INTO premium_rate_limit(user_id, last_ts) VALUES(?, ?) '
            'ON CONFLICT(user_id) DO UPDATE SET last_ts=excluded.last_ts',
            (user_id, now)
        )
        con.commit()
