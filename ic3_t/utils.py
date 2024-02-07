import requests

def get_html(url):
    """
    Fetches HTML content from a given URL.

    Args:
    url (str): The URL to fetch HTML from.

    Returns:
    str: HTML content of the URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching HTML from {url}: {e}")
        return None

def save_to_file(data, filename):
    """
    Saves data to a file.

    Args:
    data (str): Data to be saved.
    filename (str): Name of the file to save data to.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            file.write(data)
        print(f"Data saved to {filename} successfully.")
    except IOError as e:
        print(f"Error saving data to {filename}: {e}")