from fastapi import FastAPI, BackgroundTasks, HTTPException
from Kafka.producer import  produce_json_messages
from Kafka.consumer import  consume_json_messages
from config import TOPIC_NAME
from logger import setup_logger
from model.inputvalidation import KafkaMessage

app = FastAPI()
logger = setup_logger()

# @app.post("/produce")
# async def produce_endpoint(message: str):
#     """
#     Produce a message to Kafka.
#     """
#     await produce_message(TOPIC_NAME, message)
#     return {"status": "success", "message": message}
######################################################################
# Producer endpoint: Send JSON messages to Kafka
@app.post("/produce")
async def produce_message(message: KafkaMessage):
    try:
        await produce_json_messages(TOPIC_NAME, message)
        return {"status": "success", "message": message}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to send message: {e}")

# @app.get("/consume")
# async def consume_endpoint(background_tasks: BackgroundTasks):
#     """
#     Start consuming messages from Kafka.
#     """
#     def process_message(msg):
#         logger.info(f"Processed message: {msg}")

#     background_tasks.add_task(consume_messages, process_message)
#     return {"status": "success", "message": "Consuming messages in the background."}
#######################################################################################
@app.get("/consume")
async def consume_endpoint(background_tasks: BackgroundTasks):
    """
    Start consuming messages from Kafka.
    """
    def process_message(msg):
        logger.info(f"Processed message: {msg}")

    background_tasks.add_task(consume_json_messages, process_message)
    return {"status": "success", "message": "Consuming messages in the background."}

@app.on_event("startup")
async def startup_event():
    logger.info("Application is starting.")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application is shutting down.")
