import json

import grpc
from google.protobuf.json_format import MessageToJson

import product_pb2
import product_pb2_grpc


def run(request):
    # Адрес сервера
    server_address = 'stage-svc.sima-land.ru:8080'

    # Создаем канал
    with grpc.insecure_channel(server_address) as channel:
        # Создаем клиент (stub)
        stub = product_pb2_grpc.ProductStub(channel)

        # Формируем метаинформацию
        metadata = [("grpc-service", "product-959")]

        # Пример запроса к методу ViewBySids


        try:
            # Передаем запрос с метаинформацией
            response = stub.ViewBySids(request, metadata=metadata)
            response_json = MessageToJson(response, ensure_ascii=False)
            dict_response = json.loads(response_json)
            items = dict_response['defaultItems']['items']
            print("Response from server:", items)
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()} - {e.details()}")


def get_attributes(key, request):
    # Адрес сервера
    server_address = 'stage-svc.sima-land.ru:8080'

    # Создаем канал
    with grpc.insecure_channel(server_address) as channel:
        # Создаем клиент (stub)
        stub = product_pb2_grpc.ProductStub(channel)

        # Формируем метаинформацию
        metadata = [("grpc-service", "product-959")]

        # Пример запроса к методу ViewBySids
        map_request = {
            'sid': stub.ViewBySids(request, metadata=metadata)

        }

        try:
            # Передаем запрос с метаинформацией
            response = map_request.get(key)
            response_json = MessageToJson(response, ensure_ascii=False)
            dict_response = json.loads(response_json)
            items = dict_response['defaultItems']['items']
            print("Response from server:", response_json)
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()} - {e.details()}")

if __name__ == "__main__":
    request = product_pb2.BySidsRequest(sids=[2894950])
    get_attributes('sid', request)
