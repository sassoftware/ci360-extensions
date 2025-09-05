import base64

def decode_base64(encoded_text):
    try:
        # Decode the Base64 text
        decoded_bytes = base64.b64decode(encoded_text)

        # Convert the decoded bytes to a string
        decoded_text = decoded_bytes.decode('utf-8')

        return decoded_text
    except Exception as e:
        print(f"Error occurred: {str(e)}")


def encode_base64(text):
    try:
        # Encode the text as bytes using UTF-8
        text_bytes = text.encode('utf-8')

        # Encode the bytes as Base64
        encoded_bytes = base64.b64encode(text_bytes)

        # Convert the encoded bytes to a string
        encoded_text = encoded_bytes.decode('utf-8')

        return encoded_text
    except Exception as e:
        print(f"Error occurred: {str(e)}")


def read_text_file(file_path):
    try:
        # Open the file in read mode
        with open(file_path, 'r') as file:
            # Read the contents of the file
            file_content = file.read()
            return file_content
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def getKey(file_path):
    content = read_text_file(file_path)
    if content:
        decoded = decode_base64 (content)
        if decoded:
            return decoded.split(",")

