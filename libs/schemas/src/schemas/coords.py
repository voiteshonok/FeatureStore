from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_core import InitErrorDetails, ValidationError


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CoordinateMixin(StrictModel):
    lat: float | None = Field(default=None, ge=-90, le=90)
    lon: float | None = Field(default=None, ge=-180, le=180)

    @model_validator(mode="after")
    def lat_lon_together(self) -> Self:
        if (self.lat is None) == (self.lon is None):
            return self
        missing = "lon" if self.lat is not None else "lat"
        raise ValidationError.from_exception_data(
            self.__class__.__name__,
            [
                InitErrorDetails(
                    type="missing",
                    loc=(missing,),
                    input=None,
                )
            ],
        )
