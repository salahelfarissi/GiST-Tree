import re

def unpack(lst):
    lst = list(lst)[0].splitlines()
    lst = [" ".join(v[1].split()) for v in enumerate(lst)]
    lst = [[el] for el in lst]
    lst = [sub.split(': ') for subl in lst for sub in subl]

    key = map(lambda a: a[0], filter(lambda b: not b[0].startswith('Total'), lst))
    key = map(lambda v: re.sub('Number Of ', '', v.title()), key)

    value = map(lambda a: a[1], filter(lambda b: not b[1].endswith('bytes'), lst))
    value = map(lambda a: int(a), filter(lambda b: b.isdigit(), value))

    return {k: v for k, v in zip(key, value)}


def field_width(table):
    """Return the maximum length of elements of a list"""
    idx_names = [i[1] for i in table]
    idx_oids = [i[0] for i in table]

    len_names = [len(el) for el in idx_names]
    len_oids = [len(str(el)) for el in idx_oids]

    len1, len2 = max(len_names), max(len_oids) + 3

    return len1, len2  # pack max len values into a tuple
