import sqlite3
conn=sqlite3.connect('test_stats.db')
c=conn.cursor()
print('tables:', [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")])
print('schema:', list(c.execute("PRAGMA table_info('requests')")))
print('count:', c.execute("SELECT count(*) FROM requests").fetchone())
print('first10:', list(c.execute("SELECT last_sent_at, request_id FROM requests LIMIT 10")))
conn.close()
