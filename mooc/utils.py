from os.path import join, dirname
import time


def relative_path(*path):

    return join(dirname(dirname(__file__)), *path)


def calc_time(start_time):

    d = time.time() - start_time
    h = int(d / 3600)
    h = f"{h} h " if d > 3600 else ''
    m = int(d % 3600 / 60)
    m = f"{m} m " if d > 60 else ''
    s = int(d % 3600 % 60)
    s = f"{s} s"
    return h + m + s