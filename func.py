def unpack(tuple):
    lst = list(tuple)[0].splitlines()
    lst = [" ".join(lst[e].split()) for e in range(len(lst))]
    lst = [[el] for el in lst]
    lst = [sub.split(': ') for subl in lst for sub in subl]

    key = [i[0] for i in lst]
    value = [i[1] for i in lst]

    for i in range(len(value)):
        value[i] = value[i].replace(' bytes', '')
        value[i] = int(value[i])

    return {key[i]: value[i] for i in range(len(key))}


def max_len(table):
    """Return the maximum length of elements of a list"""
    idx_names = [i[1] for i in table]
    idx_oids = [i[0] for i in table]

    len_names = [len(el) for el in idx_names]
    len_oids = [len(str(el)) for el in idx_oids]

    len1, len2 = max(len_names), max(len_oids) + 3

    return len1, len2  # pack max len values into a tuple
