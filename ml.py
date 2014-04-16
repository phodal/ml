import build_db as db
import redis
import pyflann
import h5py
import numpy as np

evttypes = [l.strip() for l in open("evttypes.txt")]
langs = [l.strip() for l in open("languages.txt")]

index_filename = "index.h5"
points_filename = "points.h5"

nevts = len(evttypes)
nlangs = len(langs)
nvector = 1 + 7 + nevts + 1 + 1 + 1 + 1 + nlangs + 1


def get_format(key):
    return "{0}:{1}".format("osrc", key)


def parse_vector(results):
    points = np.zeros(nvector)
    total = int(results[0])

    points[0] = 1.0 / (total + 1)

    # Week means.
    for k, v in results[1].iteritems():
        points[1 + int(k)] = float(v) / total

    # Event types.
    n = 8
    for k, v in results[2]:
        points[n + evttypes.index(k)] = float(v) / total

    # Number of contributions, connections and languages.
    n += nevts
    points[n] = 1.0 / (float(results[3]) + 1)
    points[n + 1] = 1.0 / (float(results[4]) + 1)
    points[n + 2] = 1.0 / (float(results[5]) + 1)
    points[n + 3] = 1.0 / (float(results[6]) + 1)

    # Top languages.
    n += 4
    for k, v in results[7]:
        if k in langs:
            points[n + langs.index(k)] = float(v) / total
        else:
            # Unknown language.
            points[-1] = float(v) / total

    return points


def get_points(usernames):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    pipe = r.pipeline()

    results = get_vector(usernames)
    points = np.zeros([len(usernames), nvector])
    points = parse_vector(results)
    return points


def get_vector(user, pipe=None):
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    no_pipe = False
    if pipe is None:
        pipe = r.pipeline()
        no_pipe = True

    user = user.lower()
    pipe.zscore(get_format("user"), user)
    pipe.hgetall(get_format("user:{0}:day".format(user)))
    pipe.zrevrange(get_format("user:{0}:event".format(user)), 0, -1,
                   withscores=True)
    pipe.zcard(get_format("user:{0}:contribution".format(user)))
    pipe.zcard(get_format("user:{0}:connection".format(user)))
    pipe.zcard(get_format("user:{0}:repo".format(user)))
    pipe.zcard(get_format("user:{0}:lang".format(user)))
    pipe.zrevrange(get_format("user:{0}:lang".format(user)), 0, -1,
                   withscores=True)

    if no_pipe:
        return pipe.execute()


def get_neighbors(name, num=5):
    vector = get_vector(name)

    if any([v is None for v in vector]):
        return []

    vector = parse_vector(vector)

    with h5py.File("points.h5", "r") as f:
        points = f["points"][...]
        usernames = f["names"][...]

    flann = pyflann.FLANN()
    flann.load_index("index.h5", points)

    inds, dists = flann.nn_index(vector, num_neighbors=num + 1)
    inds = inds[0]
    if usernames[inds[0]] == name:
        inds = inds[1:]
    else:
        inds = inds[:-1]
    return list(usernames[inds])