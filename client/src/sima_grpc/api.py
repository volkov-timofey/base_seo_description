"""
This module using data from https://gitlab.sima-land.ru/sl/it/dev-dep/dev/proto/product
"""
import json
import logging
import os
from collections import defaultdict
from string import punctuation
import grpc

from dotenv import load_dotenv
from google.protobuf.json_format import MessageToJson
from client.src.sima_grpc.protobuf_grpc import product_pb2_grpc, product_pb2

load_dotenv()

logger = logging.getLogger(__name__)

class SimaAPI_GRPC:
    def __init__(self, vendor_code, attributes, options_attr, series):
        self.vendor_code = vendor_code
        self.metadata = [
            (os.getenv('grpc_api_metadata1'), os.getenv('grpc_api_metadata2'))
        ]
        self.product_information = {
            'permission_photo': 1600,
            'base_url_photo': os.getenv('base_url_photo')
        }
        self.russian_punctuation = ''.join([punctuation, '«»'])
        self.attributes = attributes
        self.options_attr = options_attr
        self.series = series
        #self.db = SysManagerDB()

    def _clean_title(self, title):
        if not isinstance(title, str):
            print(f'{title} не является str')
            return ''

        return ' '.join([
            word.strip(self.russian_punctuation)
            for word in title.split()
        ])

    def _get_item_information(self, item: dict):
        """
        Get information about product
        :return:
        """
        important_keys = [
            'id', 'sid', 'name', 'boxDepth', 'boxHeight', 'boxWidth', 'depth',
            'height', 'width', 'weight', 'attributes'
        ]

        important_product_information = {
            key: value
            for key, value in item.items()
            if key in important_keys
        }

        self.product_information = {
            **important_product_information,
            **self.product_information
        }
        logger.info(f'Извлечена информация о товаре: {important_product_information.get("name")} получена')

    def _request_grpc_base(self):
        server_address = os.getenv('grpc_api')

        with grpc.insecure_channel(server_address) as channel:

            stub = product_pb2_grpc.ProductStub(channel)

            metadata = self.metadata

            request = product_pb2.BySidsRequest(sids=[self.vendor_code])

            try:
                response = stub.ViewBySids(request, metadata=metadata)
                response_json = MessageToJson(response, ensure_ascii=False)
                dict_response = json.loads(response_json)

                if not dict_response['defaultItems']:
                    logger.error(f'Данные по артикулу {self.vendor_code} не обнаружены и перемещен в БД')

                item = dict_response['defaultItems']['items'][0]
                logger.info(f'Информация об артикуле {self.vendor_code} получена')
                return item
            except grpc.RpcError as e:
                print(f"gRPC error: {e.code()} - {e.details()}")
            except ValueError:
                logger.error(f'Ошибка в полученных данных: dict_response - {dict_response}, '
                             f'defaultItems - {dict_response.get("defaultItems")}')


    def _get_attribute_information(self, attribute: dict) -> tuple:
        black_list_attributes = (
            'xml-generator-id', 'ШК WB', 'Запрет реализации по сегментам',
            'Склад', 'РосЗакуп', 'tnved'
        )

        key = self.attributes.get(attribute.get('attrId'), '')
        if key in black_list_attributes:
            return None, None

        if not attribute.get('valueType'):
            value = attribute.get('intValue')
            return key, value

        key_value_attr = f'{attribute.get("valueType").lower()}Value'

        if attribute.get('valueType') == 'NUMRANGE':
            values = attribute.get(key_value_attr)['int']
            return key, list(set(values))

        if attribute.get('valueType') == 'OPTION':
            id_option = attribute.get(key_value_attr)
            value = self.options_attr.get(id_option, '')
            return key, value

        if attribute.get('valueType') == 'BOOL':
            value = attribute.get(key_value_attr)
            value = 'Да' if value else 'Нет'
            return key, value

        value = attribute.get(key_value_attr)
        return key, value
        
    @staticmethod
    def _preprocessing_topic_holiday(topic_holiday: list | str):
        if isinstance(topic_holiday, str):
            return topic_holiday

        sort_main_topics = [
            topic
            for topic in topic_holiday
            if topic != 'Универсальная тематика'
        ]

        logger.info(f'ТЕМАТИКИ - {sort_main_topics}')

        if not sort_main_topics:
            return 'Универсальная тематика'

        return sort_main_topics

    def _get_attributes_information(self, list_attributes: list) -> dict:

        result_dict = defaultdict(list)

        for attribute in list_attributes:
            key, value = self._get_attribute_information(attribute)
            if not key:
                continue
            if isinstance(value, list):
                result_dict[key].extend(value)
            else:
                result_dict[key].append(value)

        # Преобразование одноэлементных списков
        for attr in result_dict:
            if len(result_dict[attr]) == 1:
                result_dict[attr] = result_dict[attr][0]

        logger.info(f'полученные атрибуты {result_dict}')

        # обработка тематики праздника
        if target := result_dict.get('Тематика праздника'):
            result_dict['Тематика праздника'] = self._preprocessing_topic_holiday(target)

        return dict(result_dict)

    def _get_series_item(self, list_series: list):
        logger.info(f'list_series - {list_series}')

        list_series = [
            obj for series in list_series
            if (obj := self.series.get(series)) is not None
        ]

        if not isinstance(list_series, list) or not list_series:
            return []

        return list_series
        """
        [
            self.db.get_object(series, 'series')
            for series in list_series
        ]
        """

    def pipeline(self) -> dict:
        dict_item = self._request_grpc_base()
        self._get_item_information(dict_item)
        title = self._clean_title(self.product_information.get('name'))
        logger.info(dict_item['attributes'])
        dict_item['attributes'] = self._get_attributes_information(dict_item['attributes'])
        dict_item['attributes'] = {
                **dict_item['attributes'],
                'height': dict_item.get('height', ''),
                'depth': dict_item.get('depth', ''),
                'width': dict_item.get('width', ''),
                'series': self._get_series_item(dict_item.get('seriesIds', []))
            }

        data_for_marketplace = {
            'vendor_code': self.product_information.get('sid'),
            'title': title,
            'attributes': dict_item.get('attributes')
        }
        logger.info(f'dict_item - {dict_item} \n'
                    f'data_for_marketplace - {data_for_marketplace}')
        return data_for_marketplace


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger.setLevel(logging.INFO)

    product = SimaAPI_GRPC(1620255)
    product.pipeline()