import importlib


def load_transform_module(module_name, transform_package):
    """
    Dynamically load a transform module from the specified package.

    Args:
        module_name (str): Name of the transform module
        transform_package (str): Fully qualified package path (e.g. 'python_modules.load_export.transform')

    Returns:
        module: The loaded module

    Raises:
        ImportError: If the transform module cannot be imported
    """
    try:
        return importlib.import_module(f"{transform_package}.{module_name}")
    except ImportError as e:
        raise ImportError(f"Cannot import transform module '{module_name}' from '{transform_package}': {e}")
