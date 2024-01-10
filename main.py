import asyncio
import json
import os.path

from aio_pika.abc import AbstractQueue
from pydantic import ValidationError

from config.entities import RequestQuery, ResponseQuery, RequestUnbagQuery
from pointcloud_handlers import get_axis_swap, get_axiswise_rot, get_conv_ply_xyz, get_height_color, get_unbag
from broker.receive import create_request_queue
from config.log import logger
from broker.send import send_response_query

handlers = {
    "axis_swap": get_axis_swap,
    "axiswise_rot": get_axiswise_rot,
    "conv_ply_xyz": get_conv_ply_xyz,
    "height_color": get_height_color,
    "unbag": get_unbag
}


async def on_message(message):
    async with message.process():
        message_id = message.message_id
        decoded_message_body = message.body.decode()
        data = json.loads(decoded_message_body)
        try:
            result = await processing(data)
            if result:
                send_message = await send_response_query(result[0], message_id)
                if send_message:
                    await message.reject(requeue=True)
            else:
                await message.reject()
        except ValidationError as e:
            logger.error(str(e))
            await message.reject()


async def get_operations(request_query: RequestQuery | RequestUnbagQuery, filename: str):
    return await loop.run_in_executor(None, handlers.get(request_query.operation_type), request_query, filename)


async def processing(data: dict) -> bool | tuple[ResponseQuery]:
    request_data = data.get("Входящий request")
    filename = data["Ожидаемый response"]["file"].split("/")[-1]
    if request_data["operation_type"] == "unbag":
        try:
            request_query = RequestUnbagQuery(**request_data)
        except ValidationError as e:
            logger.error(str(e))
            return False
        print("Запрос принят: ", request_query.operation_type)
        if not os.path.isdir(request_query.output_path):
            os.makedirs(request_query.output_path)
        task = asyncio.gather(get_operations(request_query, filename))
    else:
        try:
            request_query = RequestQuery(**request_data)
        except ValidationError as e:
            logger.error(str(e))
            return False
        print("Запрос принят: ", request_query.operation_type)
        if not os.path.isdir(request_query.output_path):
            os.makedirs(request_query.output_path)
        task = asyncio.gather(get_operations(request_query, filename))
    try:
        result = await task
        print("Результат совпал с ожидаемым response: ", result[0])
        return result
    except Exception as e:
        logger.error(str(e))
        return False


async def main():
    request_queue: AbstractQueue = await create_request_queue()
    await request_queue.consume(on_message)

if __name__ == '__main__':
    loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
    asyncio.ensure_future(main())
    loop.run_forever()
