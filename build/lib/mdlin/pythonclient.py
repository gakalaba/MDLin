import json
from python_client_wrapper import AppRequest, AppResponse

def create_user(username, profile_info):
    """
    Create a user with HMSET
    
    :param username: Unique username
    :param profile_info: Dictionary of user profile information
    :return: User creation result
    """
    key = f"user:{username}"
    result = AppRequest("HMSET", key, profile_info)
    response = AppResponse(result)
    return response

def get_user(username):
    """
    Retrieve user informaation
    
    :param username: Username to retrieve
    :return: User profile information
    """
    key = f"user:{username}"
    fields = list(create_test_user_profile(username).keys())
    result = AppRequest("HMGET", key, fields)
    response = AppResponse(result)
    return response

def send_message(sender, receiver, message):
    """
    Send a message from one user to another
    
    :param sender: Sender's username
    :param receiver: Receiver's username
    :param message: Message content
    :return: Message sending result
    """
    key = f"messages:{sender}:{receiver}"
    result = AppRequest("PUBLISH", key, message)
    response = AppResponse(result)
    return response

def get_messages(sender, receiver):
    """
    Retrieve messages between two users
    
    :param sender: Sender's username
    :param receiver: Receiver's username
    :return: List of messages
    """
    key = f"messages:{sender}:{receiver}"
    result = AppRequest("LISTEN", key)
    response = AppResponse(result)
    return response

def create_test_user_profile(username):
    """
    Generate a test user profile
    
    :param username: Username for the profile
    :return: Dictionary of user profile information
    """
    return {
        "name": f"{username} Doe",
        "age": "25",
        "email": f"{username}@example.com",
        "city": "Tech City"
    }

def run_user_messaging_test():
    """
    Comprehensive test of user and messaging operations
    """
    # Test user creation
    print("\n1. Creating Users")
    users = ["alice", "bob", "charlie"]
    for username in users:
        profile = create_test_user_profile(username)
        result = create_user(username, profile)
        print(f"Created user {username}: {result}")
    
    # Verify user creation
    print("\n2. Retrieving User Profiles")
    for username in users:
        user_profile = get_user(username)
        print(f"{username} profile: {user_profile}")
    
    # Send messages
    print("\n3. Sending Messages")
    messages = [
        ("alice", "bob", "Hey Bob, how are you?"),
        ("bob", "alice", "Hi Alice, I'm good!"),
        ("charlie", "alice", "Alice, let's meet up!")
    ]
    
    for sender, receiver, message in messages:
        result = send_message(sender, receiver, message)
        print(f"Message from {sender} to {receiver}: {result}")
    
    # Retrieve messages
    print("\n4. Retrieving Messages")
    message_pairs = [
        ("alice", "bob"),
        ("bob", "alice"),
        ("charlie", "alice")
    ]
    
    for sender, receiver in message_pairs:
        messages = get_messages(sender, receiver)
        print(f"Messages from {sender} to {receiver}: {messages}")

if __name__ == "__main__":
    run_user_messaging_test()