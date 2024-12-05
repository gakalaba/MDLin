import ctypes
import json
import os

print("Python: Starting script")

# Get the absolute path to the library
script_dir = os.path.dirname(os.path.abspath(__file__))
lib_path = os.path.join(script_dir, 'libmdlclient.so')

# Load the library
try:
    print("Loading shared library")
    library = ctypes.cdll.LoadLibrary(lib_path)
except OSError as e:
    raise OSError(f"Failed to load library at {lib_path}. Error: {e}")

# Set up function signatures
app_request = library.AsyncAppRequest
app_request.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
app_request.restype = ctypes.c_char_p

app_response = library.AsyncAppResponse
app_response.argtypes = [ctypes.c_char_p]
app_response.restype = ctypes.c_char_p

def AppRequest(op_type, key, oldValue=None, newValue=None):
    try:
        print(f"Python: Starting appRequest with op_type={op_type}, key={key}")
        
        # Convert operation type to JSON
        op_types_json = json.dumps(op_type).encode('utf-8')
        
        # Convert key to string, strip any non-numeric characters, then encode
        # This handles cases like "post:123" -> "123"
        numeric_key = ''.join(c for c in str(key) if c.isdigit())
        if not numeric_key:
            raise ValueError(f"Key '{key}' contains no numeric value")
        keys_json = numeric_key.encode('utf-8')
        
        print(f"Python: Encoded JSON - op_types={op_types_json}, keys={keys_json}")
        
        # Call Go function
        result_ptr = app_request(op_types_json, keys_json)
        print(f"Python: Received result pointer: {result_ptr}")
        
        if not result_ptr:
            return None
            
        # Convert result from bytes to string
        result_str = ctypes.string_at(result_ptr).decode('utf-8')
        print(f"Python: Decoded result string: {result_str}")
        
        # Parse JSON result
        result = json.loads(result_str)
        print(f"Python: Parsed result: {result}")
        
        return result
    except Exception as e:
        print(f"Python: Error in appRequest: {str(e)}")
        raise

def AppResponse(key):
    try:
        # Convert key to JSON string
        key_json = json.dumps(key).encode('utf-8')
        
        # Call Go function
        result_ptr = app_response(key_json)
        if not result_ptr:
            return None
            
        # Convert result from bytes to string
        result_str = ctypes.string_at(result_ptr).decode('utf-8')
        
        # Parse JSON result
        result = json.loads(result_str)
        
        return result
    except Exception as e:
        print(f"Error in appResponse: {str(e)}")
        raise