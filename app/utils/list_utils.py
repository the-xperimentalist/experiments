
def split_json_list(json_objects, chunk_size=50):
    return [json_objects[i:i + chunk_size] for i in range(0, len(json_objects), chunk_size)]
