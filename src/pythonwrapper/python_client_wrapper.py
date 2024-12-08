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
async_app_request = library.AsyncAppRequest
async_app_request.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p]
async_app_request.restype = ctypes.c_char_p

async_app_response = library.AsyncAppResponse
async_app_response.argtypes = [ctypes.c_char_p]
async_app_response.restype = ctypes.c_char_p

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
    assert("Hello, World!" in response)
    assert("Second message" in response)

    result = AppRequest("LISTEN", channel)
    response = AppResponse(result)
    assert(len(response) == 0)

    result = AppRequest("PUBLISH", channel, "Third message")
    response = AppResponse(result)

    assert(response == "OK")

    result = AppRequest("LISTEN", channel)
    response = AppResponse(result)

    assert(len(response) == 1)
    assert("Third message" in response)

def test_operations():
    key = "test_key"
    print("\n--- Starting Test Operations ---")

    # 1. Test PUT and GET with string
    print("\n1. Testing PUT and GET with string...")
    result = AppRequest("PUT", key, "Hello MDLin!")
    response = AppResponse(result)
    print(f"PUT request result: {result}")
    
    result = AppRequest("GET", key)
    response = AppResponse(result)
    print(f"GET response: {response}")
    assert response == "Hello MDLin!", "String PUT and GET failed"

    # 2. Test PUT with dictionary/set
    print("\n2. Testing PUT with dictionary...")
    result = AppRequest("PUT", key, {"item1": True, "item2": True})
    response = AppResponse(result)
    print(f"PUT dictionary result: {result}")
    
    # 3. Test SADD and SCARD for set operations
    print("\n3. Testing SADD and SCARD...")
    result = AppRequest("SADD", key, "room1")
    response = AppResponse(result)
    print(f"SADD room1 result: {response}")
    
    result = AppRequest("SADD", key, "room2")
    response = AppResponse(result)
    print(f"SADD room2 result: {response}")
    
    result = AppRequest("SCARD", key)
    response = AppResponse(result)
    print(f"SCARD result: {response}")
    assert response == '2', "Set cardinality is incorrect"

    # 4. Test HMSET and HMGET for hash operations
    print("\n4. Testing HMSET and HMGET...")
    result = AppRequest("HMSET", key, "hello", "world")
    response = AppResponse(result)
    print(f"HMSET result: {response}")
    
    result = AppRequest("HMGET", key, "hello")
    response = AppResponse(result)
    print(f"HMGET result: {response}")
    assert response == "world", "Hash GET failed"

    # 5. Test INCR operation
    print("\n5. Testing INCR...")
    result = AppRequest("PUT", key, "10")
    result = AppRequest("INCR", key)
    response = AppResponse(result)
    print(f"INCR result: {response}")
    assert response == "11", "INCR operation failed"

    # 6. Test CAS (Compare And Swap)
    print("\n6. Testing CAS...")
    old_value = "Hello MDLin!"
    new_value = "Updated MDLin!"
    result = AppRequest("CAS", key, new_value, old_value)
    response = AppResponse(result)
    print(f"CAS result: {response}")
    
    result = AppRequest("GET", key)
    response = AppResponse(result)
    print(f"GET after CAS: {response}")
    assert response == new_value, "CAS operation failed"

    print("\n--- All Tests Completed Successfully! ---")


def main():
    test_operations()
    
if __name__ == "__main__":
    main()
