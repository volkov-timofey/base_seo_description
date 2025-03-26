import logging.config
from pathlib import Path
import json
import os


from client.src.gen_description.prepare_base_data import SEOBaseDescription

BASE_DIR = Path(__file__).resolve().parent
log_config_path = BASE_DIR / os.getenv('path_log_config_files')

with open(log_config_path, 'r') as f:
    config = json.load(f)

logging.config.dictConfig(config)

logger = logging.getLogger('my_module')

def main():
    description = SEOBaseDescription(BASE_DIR)
    description.pipeline()


if __name__ == '__main__':
    main()
