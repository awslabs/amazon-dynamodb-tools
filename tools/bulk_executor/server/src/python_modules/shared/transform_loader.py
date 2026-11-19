import importlib
from .bulk_executor_error import BulkExecutorError


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
        # --transform is user input, so a typo is a sentence rather than a traceback.
        raise BulkExecutorError(
            f"Cannot import transform module '{module_name}' from '{transform_package}': {e}"
        ) from None
