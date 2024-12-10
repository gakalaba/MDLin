import ctypes
import json
import os
import sys

def _load_library():
    # Potential library paths
    potential_paths = [
        '/users/akalaba/MDLin/src/pythonwrapper/libmdlclient.so'
    ]
    
    # Try each potential path
    for lib_path in potential_paths:
        try:
            print(f"Attempting to load library from: {lib_path}")
            if os.path.exists(lib_path):
                library = ctypes.cdll.LoadLibrary(lib_path)
                
                # Set up function signatures
                library.AsyncAppRequest.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
                library.AsyncAppRequest.restype = ctypes.c_char_p
                
                library.AsyncAppResponse.argtypes = [ctypes.c_char_p]
                library.AsyncAppResponse.restype = ctypes.c_char_p
                
                return library
        except Exception as e:
            # If loading fails, continue to next path
            print(f"Failed to load library from {lib_path}: {e}")
    
    # If no library could be loaded
    raise OSError("Could not load libmdlclient.so from any of the expected locations")

# Load the library at module import time
try:
    _library = _load_library()
except OSError as e:
    print(f"Warning: {e}")
    _library = None

def AppRequest(op_type, key, value=None, old_value=None):
    """
    Perform an operation on a key with optional value and old_value.
    
    :param op_type: Operation type (e.g., 'PUT', 'GET', 'CAS')
    :param key: Key for the operation (string or integer)
    :param value: Value for the operation (optional)
    :param old_value: Old value for CAS operations (optional)
    :return: Request key or result
    """
    if _library is None:
        raise RuntimeError("Library not loaded. Cannot perform AppRequest.")
    
    try:
        # Convert inputs to strings
        key_str = str(key)
        
        # Prepare JSON-encoded parameters
        op_type_json = json.dumps(op_type)
        key_json = json.dumps(key_str)
        value_json = json.dumps(value) if value is not None else None
        old_value_json = json.dumps(old_value) if old_value is not None else None
        
        # Convert to bytes for ctypes
        op_type_bytes = op_type_json.encode('utf-8')
        key_bytes = key_json.encode('utf-8')
        value_bytes = value_json.encode('utf-8') if value_json is not None else None
        old_value_bytes = old_value_json.encode('utf-8') if old_value_json is not None else None
        
        # Call the C function
        result = _library.AsyncAppRequest(
            op_type_bytes,
            key_bytes,
            value_bytes if value_bytes is not None else None,
            old_value_bytes if old_value_bytes is not None else None
        )
        
        # Convert result from bytes to string and parse JSON
        if result:
            result_str = result.decode('utf-8')
            return json.loads(result_str)
        return None
        
    except Exception as e:
        raise Exception(f"Error in AppRequest: {str(e)}")
def AppResponse(key):
    try:
        # Convert key to JSON string
        key_json = json.dumps(key).encode('utf-8')
        
        # Call Go function
        result_ptr = _library.AsyncAppResponse(key_json)
        if not result_ptr:
            return None
            
        # Convert result from bytes to string
        result_str = ctypes.string_at(result_ptr).decode('utf-8')


        result = json.loads(result_str)

        # Parse JSON result
        if result['success'] == False:
            raise Exception("Result returned false")
        
        value_result = result.get('result')
        value_type = value_result.get('Type')
        if value_type == 0:  # StringType
            return value_result.get('String', '')
        elif value_type == 1:  # ListType
            return value_result.get('List', [])
        elif value_type == 2:  # SetType
            # Convert map[string]bool to set
            set_dict = value_result.get('Set', {})
            return {k for k, v in set_dict.items() if v}
        else:
            return None
            
    except Exception as e:
        print(f"Error in appResponse: {str(e)}")
        raise
