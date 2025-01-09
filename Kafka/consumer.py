from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from fastapi import HTTPException
from config import KAFKA_BROKER, TOPIC_NAME, GROUP_ID, AUTO_OFFSET_RESET
import logging
import asyncio
import json

logger = logging.getLogger(__name__)
consumer = None

async def get_consumer():
    global consumer
    if not consumer:
        try:
            consumer = AIOKafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset=AUTO_OFFSET_RESET
            )
            await consumer.start()
            logger.info("Kafka Consumer started.")
        except KafkaError as e:
            logger.error(f"Failed to start Kafka Consumer: {str(e)}")
            raise HTTPException(status_code=500, detail="Kafka Consumer initialization failed.")
    return consumer

# async def consume_messages(process_message):
#     try:
#         consumer = await get_consumer()
#         async for msg in consumer:
#             message = msg.value.decode("utf-8")
#             logger.info(f"Consumed message: {message}")
#             process_message(message)
#     except KafkaError as e:
#         logger.error(f"Failed to consume messages: {str(e)}")
#         raise HTTPException(status_code=500, detail="Failed to consume messages.")

################################################################################################
async def consume_json_messages(process_message):
    try:
        consumer = await get_consumer()
        async for msg in consumer:
            value = json.loads(msg.value.decode("utf-8"))  # Deserialize JSON
            logger.info(f"Consumed: {value} from partition {msg.partition}")
            process_message(value)

    except KafkaError as e:
        logger.error(f"Failed to consume messages: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to consume messages.")