import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def json_to_parquet(input_csv_filename, list_key_to_remove):

    csv_path = Path(os.getenv('BASE_PATH')) / 'client/data/backup_db' / input_csv_filename

    input_df = pd.read_csv(csv_path)
    input_df = input_df.drop(columns=list_key_to_remove)
    print(input_df.columns)

    output_parquet = csv_path.parent / csv_path.with_suffix('.parquet')
    input_df.to_parquet(output_parquet, engine='pyarrow', compression='snappy')

    print("✅ CSV успешно конвертирован в Parquet")

    print(f"Файл успешно сохранён как {output_parquet}")


json_to_parquet('series.csv', ['_id'])