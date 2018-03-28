import json


def str_to_json(json_str):
    if isinstance(json_str, (list, tuple)):
        json_str = ' '.join(json_str)
    json_str = json_str.replace('\n', '')
    try:
        json_obj = json.loads(json_str)
    except ValueError:
        json_obj = None

    return json_obj
