import json


class Configurable():
    def __init__(self, fields=None):
        with open('config.json', 'r') as f:
            self.config = json.loads(f.read())  # Loads configuration file

        if isinstance(fields, list):
            new_config = {}
            for field in fields:
                new_config[field] = self.config.get(field, None)
            self.config = new_config

