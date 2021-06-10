def unpack(tuple):
    lst = list(tuple)[0].splitlines()
    lst = [" ".join(v[1].split()) for v in enumerate(lst)]
    lst = [[el] for el in lst]
    lst = [sub.split(': ') for subl in lst for sub in subl]

    key = [i[0] for i in lst]
    value = [i[1] for i in lst]

    new_keys = ['Levels', 'Pages', 'Leaf Pages', 'Tuples', 'Invalid Tuples',
                'Leaf Tuples']

    key = [item.replace(item, new_keys[i]) for i, item in enumerate(new_keys)]
    value = list(map(lambda a: int(a), filter(lambda b: b.isdigit(), value)))

    return {key[i]: value[i] for i in range(len(value))}


def field_width(table):
    """Return the maximum length of elements of a list"""
    idx_names = [i[1] for i in table]
    idx_oids = [i[0] for i in table]

    len_names = [len(el) for el in idx_names]
    len_oids = [len(str(el)) for el in idx_oids]

    len1, len2 = max(len_names), max(len_oids) + 3

    return len1, len2  # pack max len values into a tuple
