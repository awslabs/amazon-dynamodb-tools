import random
import string


def generate():
    pk = ''.join(random.choices(string.ascii_lowercase + string.digits, k=12))
    meta = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
    sk1 = ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))
    sk2 = ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))
    sk3 = ''.join(random.choices(string.ascii_lowercase + string.digits, k=24))
    payload = ''.join(random.choices(string.ascii_lowercase + string.digits, k=200))

    # Generate two items in the same item collection, with diff schemas
    item1 = {'pk': pk, 'sk': sk1, '#meta': meta}
    item2 = {'pk': pk, 'sk': sk2, 'payload': payload}
    item3 = {'pk': pk, 'sk': sk3}

    return [item1, item2, item3]
