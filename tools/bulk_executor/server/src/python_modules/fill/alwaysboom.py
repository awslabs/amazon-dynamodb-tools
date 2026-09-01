"""Scratch generator for #332: raises on every call, so it fails during the driver's size peek."""


def generate():
    raise RuntimeError("faker exploded on every call")
