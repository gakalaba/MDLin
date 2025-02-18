import ctypes
import json
import os

print("Python: Starting script")

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

                library.SyncAppRequest.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
                library.SyncAppRequest.restype = ctypes.c_char_p
                
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

def SyncAppRequest(op_type, key, value=None, old_value=None):
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
        result_ptr = _library.SyncAppRequest(
            op_type_bytes,
            key_bytes,
            value_bytes if value_bytes is not None else None,
            old_value_bytes if old_value_bytes is not None else None
        )
    
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
        raise Exception(f"Error in AppRequest: {str(e)}")

def AppRequest(op_type, key, value=None, old_value=None):
    """
    Perform an operation on a key with optional value and old_value.

    Note: value oldvalue are also used for things like hmset, hmget, 
    might be better to change naming
    
    :param op_type: Operation type (e.g., 'PUT', 'GET', 'CAS')
    :param key: Key for the operation (string or integer)
    :param value: Value for the operation (optional)
    :param old_value: Old value for CAS operations (optional)
    :return: Request key or result
    """
    try:
        # Convert inputs to strings
        key_str = str(key)
        
        # Prepare JSON-encoded parameters
        op_type_json = json.dumps(op_type)
        key_json = json.dumps(key_str)
        value_json = json.dumps(value) if value is not None else None
        old_value_json = json.dumps(old_value) if old_value is not None else None
        
        # print(f"Python: Sending value_json: {value_json}")
        # Call the underlying C function
        result_ptr = async_app_request(
            op_type_json.encode('utf-8'), 
            key_json.encode('utf-8'), 
            value_json.encode('utf-8') if value_json is not None else None,
            old_value_json.encode('utf-8') if old_value_json is not None else None
        )
        print("Received result ptr", result_ptr)
        
        if not result_ptr:
            return None
        
        # Convert result from bytes to string
        result_str = ctypes.string_at(result_ptr).decode('utf-8')
        
        # Parse JSON result
        result = json.loads(result_str)
        
        return result
    except Exception as e:
        print(f"Error in AppRequest: {str(e)}")
        raise

def AppResponse(key):
    try:
        # Convert key to JSON string
        key_json = json.dumps(key).encode('utf-8')
        
        # Call Go function
        result_ptr = async_app_response(key_json)
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

def test_pubsub_operations():
    channel = "test_channel"

    result = AppRequest("SUBSCRIBE", channel)
    response = AppResponse(result)
    assert(response == "OK")

    print("\n")

    result = AppRequest("PUBLISH", channel, "Hello, World!")
    response = AppResponse(result)
    assert(response == "OK")

    result = AppRequest("PUBLISH", channel, "Second message")
    response = AppResponse(result)
    assert(response == "OK")

    result = AppRequest("LISTEN", channel)
    response = AppResponse(result)

    assert(len(response) == 2)
    print(response)
    
    #assert("Hello, World!" in response)
    #assert("Second message" in response)

    result = AppRequest("LISTEN", channel)
    response = AppResponse(result)
    #assert(len(response) == 0)

    result = AppRequest("PUBLISH", channel, "Third message")
    response = AppResponse(result)

    #assert(response == "OK")

    result = AppRequest("LISTEN", channel)
    response = AppResponse(result)

   # assert(len(response) == 1)
    assert("Third message" in response)

def test_operations():
    key = "test_key"
    print("\n--- Starting Test Operations ---")

    # 1. Test PUT and GET with string
    print("\n1. Testing PUT and GET with string...")
    result = SyncAppRequest("PUT", key, "Hello MDLin!")
    response = SyncAppRequest("GET", key)
    # response = AppResponse(result)
    # print(f"PUT request result: {result}")
    
    # result = AppRequest("GET", key)
    # response = AppResponse(result)
    # print(f"GET response: {response}")
    # assert response == "Hello MDLin!", "String PUT and GET failed"

    # # 2. Test PUT with dictionary/set
    # print("\n2. Testing PUT with dictionary...")
    # result = AppRequest("PUT", key, {"item1": True, "item2": True})
    # response = AppResponse(result)
    # print(f"PUT dictionary result: {result}")
    
    # # 3. Test SADD and SCARD for set operations
    # print("\n3. Testing SADD and SCARD...")
    # result = AppRequest("SADD", key, "room1")
    # response = AppResponse(result)
    # print(f"SADD room1 result: {response}")
    
    # result = AppRequest("SADD", key, "room2")
    # response = AppResponse(result)
    # print(f"SADD room2 result: {response}")
    
    # result = AppRequest("SCARD", key)
    # response = AppResponse(result)
    # print(f"SCARD result: {response}")
    # assert response == '2', "Set cardinality is incorrect"

    # # 4. Test HMSET and HMGET for hash operations
    # print("\n4. Testing HMSET and HMGET...")
    # result = AppRequest("HMSET", key, "hello", "world")
    # response = AppResponse(result)
    # print(f"HMSET result: {response}")
    
    # result = AppRequest("HMGET", key, "hello")
    # response = AppResponse(result)
    # print(f"HMGET result: {response}")
    # assert response == "world", "Hash GET failed"

    # # 5. Test INCR operation
    # print("\n5. Testing INCR...")
    # result = AppRequest("PUT", key, "10")
    # result = AppRequest("INCR", key)
    # response = AppResponse(result)
    # print(f"INCR result: {response}")
    # assert response == "11", "INCR operation failed"

    # # 6. Test CAS (Compare And Swap)
    # print("\n6. Testing CAS...")
    # old_value = "Hello MDLin!"
    # new_value = "Updated MDLin!"
    # result = AppRequest("CAS", key, new_value, old_value)
    # response = AppResponse(result)
    # print(f"CAS result: {response}")
    
    # result = AppRequest("GET", key)
    # response = AppResponse(result)
    # print(f"GET after CAS: {response}")
    # assert response == new_value, "CAS operation failed"

    print("\n--- All Tests Completed Successfully! ---")


def main():
    test_pubsub_operations()
    
if __name__ == "__main__":
    main()
