import os
import shutil

paths = [
    '../backend_files/codes_output/',
    '../backend_files/modified/',
    '../backend_files/sl_times/'
]

for base_path in paths:
    if not os.path.exists(base_path):
        print(f"Path not found: {base_path}")
        continue

    for item in os.listdir(base_path):
        item_path = os.path.join(base_path, item)

        if os.path.isdir(item_path):
            shutil.rmtree(item_path, ignore_errors=True)
            print(f"Deleted folder: {item_path}")
        elif os.path.isfile(item_path):
            os.remove(item_path)
            print(f"Deleted file: {item_path}")