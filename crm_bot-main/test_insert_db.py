import sqlite3
conn=sqlite3.connect('test_stats.db')
c=conn.cursor()
try:
    c.execute("INSERT INTO requests (request_id, scheduled_time) VALUES (?,?)", (1,'00:00'))
    conn.commit()
    c.execute('SELECT count(*) FROM requests')
    print('count:', c.fetchone())
    c.execute('SELECT last_sent_at, request_id FROM requests LIMIT 5')
    print('rows:', c.fetchall())
except Exception as e:
    print('err', e)
finally:
    conn.close()
