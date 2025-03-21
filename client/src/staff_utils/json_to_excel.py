import pandas as pd
import json


def json_to_excel(input_json_path, output_excel_path, keys_to_remove):
    with open(input_json_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    cleaned_data = []
    for item in data:
        cleaned_item = {k: v for k, v in item.items() if k not in keys_to_remove}
        cleaned_data.append(cleaned_item)

    df = pd.DataFrame(cleaned_data)
    df.to_excel(output_excel_path, index=False)

    print(f"Файл успешно сохранён как {output_excel_path}")


input_json = '/home/volkov/Загрузки/04_02_gen_tags.items.json'  # Путь к вашему JSON-файлу
output_excel = '/home/volkov/Загрузки/04_02_result.xlsx'  # Путь для сохранения Excel
keys_to_remove = ['params']  # Ключи, которые нужно удалить из словарей

json_to_excel(input_json, output_excel, keys_to_remove)
