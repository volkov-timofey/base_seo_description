import json
import math
import os
import re
import sys
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

import aiohttp

from g4f import Provider
import g4f.client as g4f_client

from dotenv import load_dotenv

import logging

from datetime import datetime

from concurrent.futures import ThreadPoolExecutor, as_completed
from client.src.sima_grpc.api import SimaAPI_GRPC
from client.src.staff_utils.add_data_to_template import save_formated_result_wb

logger = logging.getLogger(__name__)
load_dotenv()


class SEOBaseDescription:
    def __init__(self):
        self.g4f_client = g4f_client.Client()
        self.base_path = os.getenv('BASE_PATH')
        self.max_workers = int(os.getenv('max_workers'))
        self.chunk_size = self.max_workers * 35
        self.min_characters = int(os.getenv('min_characters'))
        self.max_characters = int(os.getenv('max_characters'))
        # self.vc = GenerateTags()
        self.prompts_instructions_sub_description = (
            os.getenv('prompts_instructions_sub_description')
        )
        self.path_base_description = os.getenv('path_base_description')

        self.attributes = None
        self.options_attr = None
        self.series = None

        self.greet_text = f"""
                    Вставьте полный путь до исходного файла,  
                    Либо переместите ваш файл в директорию client/data/input_data c 
                    именем input_data в формате *.csv или *.xlsx, *.xls
                    и нажмите Enter --- >>>  
                """

    def _greeting(self):
        input_path = input(self.greet_text)
        if not os.path.exists(input_path):
            logger.error(f'По данному пути файл не обнаружен: "{input_path}". Попробуйте запустить еще раз.')
            sys.exit(1)

        return input_path

    def get_data_frame(self):
        path_input_file = self._greeting()
        suffix = Path(path_input_file).suffix

        if suffix == '.csv':
            return pd.read_csv(path_input_file, chunksize=self.chunk_size)
        if suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(path_input_file)
            return (df.iloc[i:i + self.chunk_size] for i in range(0, len(df), self.chunk_size))
        logger.error(f'Расширение файла {path_input_file} не соответствует заявленным *.csv или *.xlsx, *.xls')
        sys.exit(1)

    @staticmethod
    def _save_result_to_xlsx(result_list, path_output):
        full_path = Path(os.getenv('BASE_PATH')) / 'client/data/output_data' / path_output
        pd.DataFrame(result_list).to_excel(full_path)
        print(f'Файл {full_path} успешно сохранен')

    def _initial_data_db(self):
        """
        init data database from parquet
        :return:
        """
        filenames = ['attributes.parquet', 'options_attr.parquet', 'series.parquet']
        attributes = ['attributes', 'options_attr', 'series']

        for attr_name, filename in zip(attributes, filenames):
            full_path = Path(self.base_path) / 'client/data/backup_db' / filename
            df = pd.read_parquet(full_path)
            dict_data = dict(zip(df['id'], df['name']))
            setattr(self, attr_name, dict_data)

    @staticmethod
    def _get_base_description_xlsx():
        """
        Get base description for category
        from xlsx table
        :return:
        """
        path_input_file = os.getenv('path_xlsx_description')

        if not os.path.exists(path_input_file):
            logger.error(f'Расширение файла {path_input_file} не соответствует заявленным *.csv или *.xlsx, *.xls')
            sys.exit(1)

        df = pd.read_excel(path_input_file)
        category_items = df.iloc[:,0]
        input_data = '\n'.join([f'{i}. {cat}' for i, cat in enumerate(category_items, start=1)])
        print(input_data)
        index_ = int(input(f'Введите номер категории из списка --> '))
        return df.iloc[index_-1, 1]


    def _get_base_description(self):
        """
        Get base description for category
        :return:
        """
        greet = "Вставьте имя файла базового описания, без расширения: -> "

        file_name = f'{input(greet)}.txt'

        full_path_base_description = Path(self.path_base_description) / file_name

        try:
            with open(full_path_base_description, 'r') as f:
                base_description = f.read()
            logger.info(base_description)
            return base_description

        except FileNotFoundError:
            print(
                f'Такой файл {file_name} '
                f'в папке {self.path_base_description} не найден'
            )
            print('Попробуйте снова')
            return self._get_base_description()


    def _get_vendor_codes(self, list_vendor_codes=None):
        """
        Initial list_tag
        :return:
        """
        if list_vendor_codes:
            generator_vendor_codes = list_vendor_codes
        else:
            generator_vendor_codes = self.get_data_frame()
        return generator_vendor_codes

    def _get_description(self, instruction, client=None, count=0, max_count=4):

        try:
            self.g4f_client = g4f_client.Client()
            if client:
                self.g4f_client = client


            response = self.g4f_client.chat.completions.create(
                model="",
                messages=[
                    {
                        "role": "user",
                        "content": instruction
                    }
                ],
                provider=Provider.Blackbox
            )

            content = response.choices[0].message.content
            logger.info(f'content - {content}')

            return content.strip()

        except aiohttp.client_exceptions.ClientResponseError as e:
            logger.error(
                f'Ошибка на клиенте чата на этапе генерации '
                f'тегов для seo \n {instruction} \n {e}'
            )
            while count < max_count:
                logger.warning(f'{count} попытка чата из {max_count}, включено ожидание 10 секунд')
                time.sleep(10)
                self.g4f_client = g4f_client.Client()
                count += 1
                return self._get_description(instruction)
            logger.error('Вторая попытка чата, завершилась ошибкой')
            raise ValueError(
                'Отсутствие валидного ответа чата, на этапе вопросов в части seo'
            )
        except ValueError as e:
            if 'Invalid image format' not in str(e):
                raise ValueError(f'Invalid image format {e}')

    def sub_response(self, instruction, client=None, count=1, max_count=4):

        description_text = self._get_description(instruction, client)
        logger.info(f'Полученное описание -{description_text}-')

        new_description_text = description_text
        try:
            if '```json' in description_text:  # есть ли внутри
                matches = re.findall(r'```json(.*?)```', description_text, re.DOTALL)
                new_description_text = matches.pop() if matches else ''
            dict_object = json.loads(new_description_text)
            logger.info(f'new_description_text - {new_description_text}, dict_object - {dict_object}')
            text_description = dict_object.get('description')

            return text_description
        except (ValueError, TypeError, json.decoder.JSONDecodeError):
            logger.warning(f'Не смог распознать ответ модели tags - {description_text}, new_description_text - {new_description_text}')
            if count > max_count:
                logger.warning(f'Количество попыток превысило допустимый уровень {max_count}')
                return []
            self.g4f_client = g4f_client.Client()
            count += 1
            logger.warning(f'Попытка № {count}')
            return self.sub_response(instruction, client)

    @staticmethod
    def get_list_instructions():
        path_instructions = os.getenv('prompts_instructions_sub_description')

        list_attribute_instruction = [
            'stage_1.txt', 'validate_instruction.txt'
        ]

        list_instructions = list()
        for name_file in list_attribute_instruction:
            full_path = Path(path_instructions) / name_file
            with open(full_path) as instruction:
                list_instructions.append(instruction.read())

        return list_instructions

    @staticmethod
    def _prepare_instruction(template_instruction: str, min_count_char, max_count_char, current_description='', title='', attributes=''):
        instruction = template_instruction.format(
            title=title,
            attributes=attributes,
            previous_description=current_description,
            min_count_char=min_count_char,
            max_count_char=max_count_char
        )

        return instruction

    def _join_descriptions(self, uniq_sub_description):
        full_description = self._base_description.format(gen_sub_description=uniq_sub_description)

        return full_description

    def process_vendor_code(self, vendor_code, count=1, max_count=4):
        product = SimaAPI_GRPC(vendor_code, self.attributes, self.options_attr, self.series)
        dict_product_data = product.pipeline()
        sort_inf_dict = {
            key: dict_product_data[key]
            for key in dict_product_data
            if key in ('title', 'attributes')
        }

        if not sort_inf_dict:
            logger.critical(f"Данные с GRPC - {sort_inf_dict}")

        list_instruction = self.get_list_instructions()

        client = g4f_client.Client()
        gen_sub_description = ''
        final_min_count_char, final_max_count_char = self.min_characters, self.max_characters

        for i, raw_instruction in enumerate(list_instruction):
            title = sort_inf_dict.get('title')
            attributes = sort_inf_dict.get('attributes')
            min_count_char, max_count_char = final_min_count_char, final_max_count_char

            if i == len(list_instruction) - 1:
                instruction = self._prepare_instruction(
                    raw_instruction, min_count_char, max_count_char, gen_sub_description, title, attributes
                )
                full_uniq_description = self.sub_response(instruction, client)

                while not (len(full_uniq_description) <= final_max_count_char):
                    max_count_char = max(math.ceil(max_count_char * 0.98), min_count_char)

                    logger.info(f'Повторная проработка описания - {len(full_uniq_description)}')

                    instruction = self._prepare_instruction(
                        raw_instruction, min_count_char, max_count_char, full_uniq_description, title, attributes
                    )
                    full_uniq_description = self.sub_response(instruction, client)

                logger.info(f'Финальная длина текста составляет - {len(full_uniq_description)}')

                full_description = self._join_descriptions(full_uniq_description)

                return {
                    'vendor_code': vendor_code,
                    **sort_inf_dict,
                    'text_description': full_description
                }

            if i % 2 == 0:
                instruction = self._prepare_instruction(
                    raw_instruction, min_count_char, max_count_char, gen_sub_description, title, attributes
                )
                gen_sub_description = self.sub_response(instruction, client)
                logger.info(f'Инструкция - {instruction},\nДлина - {len(gen_sub_description)}')

    def process_threaded(self, list_vendor_code):
        result_list = list()
        failed_list = list()

        with ThreadPoolExecutor(self.max_workers) as executor:
            future_to_vendor = {
                executor.submit(self.process_vendor_code, vc): vc
                for vc in list_vendor_code
            }
            for i, future in tqdm(enumerate(as_completed(future_to_vendor)), total=len(future_to_vendor)):
                try:
                    result = future.result()
                    result_list.append(result)
                except Exception as e:
                    logger.error(f"Артикул отправлен в массив на повторную генерацию {future_to_vendor[future]}: {e}")
                    failed_list.append({'vendor_code': future_to_vendor[future]})

        return result_list, failed_list

    def pipeline_chunk(self, chunk, num_chunk):
        if isinstance(chunk, list):
            list_vendor_code = chunk
        else:
            list_vendor_code = list(chunk.iloc[:, 0])

        result_list, failed_list = self.process_threaded(list_vendor_code)
        logger.info(f'Статистика по чанке № {num_chunk} {len(result_list)} из {len(list_vendor_code)}')

        logger.info(f'Результаты - {result_list}')

        if result_list:
            save_formated_result_wb(
                result_list,
                f"success_wb_{datetime.now().strftime('%d-%m-%Y')} - {num_chunk}.xlsx"
            )

        if failed_list:
            self._save_result_to_xlsx(
                failed_list,
                f"failed_{datetime.now().strftime('%d-%m-%Y')} - {num_chunk}.xlsx"
            )

    def pipeline(self, list_vendor_codes=None):
        self._initial_data_db() # инициализация данных DB
        self._base_description = self._get_base_description_xlsx()
        generator_vendor_codes = self._get_vendor_codes(list_vendor_codes)

        if isinstance(generator_vendor_codes, list):
            self.pipeline_chunk(generator_vendor_codes, 0)
            logger.info(f'Список в количестве {len(generator_vendor_codes)} - обработан')
            return

        for i, chunk in enumerate(generator_vendor_codes, start=1):
            self.pipeline_chunk(chunk, i)
            logger.info(f'{i} чанка прошла, в количестве {len(chunk)}')


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger.setLevel(logging.WARNING)
    list_vendor_codes_single = [9867538, 4847257]
    list_vendor_codes_combo = [10070997, 10057139, 10153829, 10652252, 9688259]
    list_photo_frame = [4336891]
    description = SEOBaseDescription()
    description.pipeline(list_vendor_codes_combo)