import random
import string
from decimal import Decimal


def generate():
    pknum = Decimal(random.randint(1, 1_000_000_000))
    payload = ''.join(random.choices(string.ascii_lowercase + string.digits, k=200))

    item = {'pknum': pknum, 'payload': payload}

    return item
