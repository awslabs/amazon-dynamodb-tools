import importlib


def load_transform_module(module_name):
    """
    Dynamically load a transform module.

    Args:
        module_name (str): Name of the transform module

    Returns:
        module: The loaded module

    Raises:
        ImportError: If the transform module cannot be imported
    """
    try:
        return importlib.import_module(f"python_modules.ddb_import.transform.{module_name}")
    except ImportError as e:
        raise ImportError(f"Cannot import transform module '{module_name}': {e}")
