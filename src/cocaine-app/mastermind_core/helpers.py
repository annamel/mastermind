import contextlib
import copy
import cStringIO
import gzip


def gzip_compress(data, compression_level=1):
    with contextlib.closing(cStringIO.StringIO()) as buf:
        gz = gzip.GzipFile(
            fileobj=buf,
            mode='wb',
            compresslevel=compression_level
        )
        with gz:
            gz.write(data)
        return buf.getvalue()


def encode(s, encoding='utf-8', errors='strict'):
    """Returns an encoded version of string

    NB: s is encoded only if it was a unicode string, otherwise
    it is returned as is.
    """
    if isinstance(s, unicode):
        return s.encode(encoding, errors=errors)
    return s


def merge_dict(dst, src):
    """ Merges two dicts updating 'dst' keys with those from 'src'
    """
    res = copy.deepcopy(dst)
    for k, val in src.iteritems():
        if k not in dst:
            res[k] = val
        else:
            if not isinstance(val, dict):
                res[k] = val
            else:
                res[k] = merge_dict(res[k], src[k])
    return res


def percent(val):
    return '{:.2f}%'.format(val * 100.0)


BYTES_UNITS = ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB')


def convert_bytes(b):
    res = float(b)
    for unit in BYTES_UNITS[:-1]:
        if abs(res) < 1024:
            return '{:.2f} {}'.format(res, unit)
        res = res / 1024
    return '{:.2f} {}'.format(res, BYTES_UNITS[-1])
