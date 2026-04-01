import importlib


def load_filter_function(filter_module, function_name):
    """
    Dynamically load a filter function from the filter modules. E.g. load_filter_function('example', 'default') to load the default filter function from default.py.
    
    Args:
        filter_module (str): Name of the filter module
        function_name (str): Name of the function to load
        
    Returns:
        callable: The filter function
        
    Raises:
        ImportError: If the filter module cannot be imported
        AttributeError: If the function doesn't exist in the module
    """
    try:
        module = importlib.import_module(f"python_modules.ddb_import.filter.{filter_module}")
        filter_function = getattr(module, function_name)
        return filter_function
    except ImportError as e:
        raise ImportError(f"Cannot import filter module '{filter_module}': {e}")
    except AttributeError as e:
        raise AttributeError(f"Function '{function_name}' not found in filter module '{filter_module}': {e}")
