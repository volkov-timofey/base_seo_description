import logging.config
import json
import os

from client.src.gen_description.prepare_base_data import SEOBaseDescription

log_config_path = os.path.join(os.getenv('BASE_PATH'), os.getenv('path_log_config_files'))

with open(log_config_path, 'r') as f:
    config = json.load(f)

logging.config.dictConfig(config)

logger = logging.getLogger('my_module')

def main():
    description = SEOBaseDescription()
    description.pipeline()


if __name__ == '__main__':
    main()
