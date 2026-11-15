"""Pydantic schema for a sensor reading: the contract between producer and consumer.

Ranges are deliberately wide (physically plausible) so that injected anomalies
still pass validation and reach the model. Only garbage goes to the DLQ.
"""

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class SensorReading(BaseModel):
    model_config = ConfigDict(extra="forbid")  # unknown fields are rejected

    sensor_id: str = Field(min_length=1, max_length=50)
    timestamp: AwareDatetime  # timezone required, avoids ambiguous local times
    temperature: float = Field(ge=-50, le=150, allow_inf_nan=False)  # degrees C
    humidity: float = Field(ge=0, le=100, allow_inf_nan=False)  # percent
    vibration: float = Field(ge=0, le=100, allow_inf_nan=False)  # mm/s
