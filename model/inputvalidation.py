from pydantic import BaseModel

# Pydantic model for input validation
class KafkaMessage(BaseModel):
    key: str
    value: dict