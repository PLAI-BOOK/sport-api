import json
import os

def save_to_json(data, file_path):
    """
    Saves a dictionary to a JSON file.

    Args:
        data (dict): The dictionary to save.
        file_path (str): The path to the JSON file.
    """
    try:
        with open(file_path, 'w') as json_file:
            json.dump(data, json_file, indent=4)
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"Error saving data to JSON: {e}")

def load_from_json(file_path):
    """
    Loads a dictionary from a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict: The loaded dictionary.
    """
    try:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
        print(f"Data successfully loaded from {file_path}")
        return data
    except FileNotFoundError:
        print(f"No file found at {file_path}. Returning an empty dictionary.")
        return {}
    except Exception as e:
        print(f"Error loading data from JSON: {e}")
        return {}

def merge_json_files(file_paths, output_file):
    """
    Merges multiple JSON files into a single JSON file.

    Args:
        file_paths (list): List of JSON file paths to merge.
        output_file (str): Path to save the merged JSON file.
    """
    merged_data = {}

    for file_path in file_paths:
        if os.path.exists(file_path):
            data = load_from_json(file_path)  # Load data using your json_func function
            merged_data.update(data)
            print(f"Loaded and merged data from {file_path}.")
        else:
            print(f"File not found: {file_path}")

    save_to_json(merged_data, output_file)
    print(f"Merged data saved to {output_file}")

    return merged_data