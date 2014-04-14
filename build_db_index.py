import sqlite3
import redis

redis_pool = redis.ConnectionPool(port=6379)
r = redis.Redis(connection_pool=redis_pool)

conn = sqlite3.connect('userdata.db')
c = conn.cursor()


def get_count(username):
    count = 0
    userinfo = []
    condition = 'select * from userinfo where owener = \'' + str(username) + '\''
    for zero in c.execute(condition):
        count += 1
        userinfo.append(zero)
        print zero

    return count, userinfo
