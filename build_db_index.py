import sqlite3

conn = sqlite3.connect('userdata.db')
c = conn.cursor()


def get_count(username):
    count = 0
    condition = 'select * from userinfo where owener = \'' + str(username) + '\''
    for zero in c.execute(condition):
        count += 1
    return count
