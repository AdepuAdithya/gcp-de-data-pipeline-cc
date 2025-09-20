import json
print("json module imported")

def load_json(file_path):
    with open(file_path) as file:
        data = json.load(file)
        print(f"Loaded JSON from {file_path}: {data}")
        return data