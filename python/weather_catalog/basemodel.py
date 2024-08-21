# from loguru import logger
import datetime
from enum import Enum
from typing import Any, Self, Union

import pydantic
from pydantic.fields import FieldInfo
from pydantic.main import PydanticUndefined

from airflow.models.param import Param, ParamsDict


class BaseModel(pydantic.BaseModel):
    """Base Class for the Weather Catalog library"""

    class Config:
        arbitrary_types_allowed = True
        orm_mode = True

    @classmethod
    def from_params(cls, params: ParamsDict) -> "Self":
        """Create an instance of a pydantic model
        from a set of airflow parameters.

        Args:
            params (ParamsDict): Airflow parameters

        Returns:
            Self: A new instance of the model
        """

        unflattened_params = cls._unflatten_params(params)
        try:
            return cls(**unflattened_params)
        except pydantic.ValidationError as e:
            raise ValueError(f"Error parsing params: {e}")

    @classmethod
    def _unflatten_params(cls, params: ParamsDict) -> dict:
        """create required nested dictionaries from airflow parameters

        moves from flat:
        {
            "foo": "bar",
            "baz.qux": "quux"
            "baz.quz": "quuz"
        }

        to nested:
        {
            "foo": "bar",
            "baz": {
                "qux": "quux"
                "quz": "quuz"
            }
        }

        Args:
            params (ParamsDict): Airflow parameters

        Returns:
            dict: Nested dictionary of parameters
        """
        unflattened_params = {}  # type: ignore
        for key, value in params.items():
            if "." in key:
                subkeys = key.split(".")
                current_dict = unflattened_params
                for subkey in subkeys[:-1]:
                    if subkey not in current_dict:
                        current_dict[subkey] = {}
                    current_dict = current_dict[subkey]

                current_dict[subkeys[-1]] = value
            else:
                unflattened_params[key] = value
        return unflattened_params

    @classmethod
    def create_params(cls) -> dict[str, Union[Param, dict]]:
        """Create a dictionary of airflow parameters from a pydantic model
        used in generating the airflow UI.

        Args:
            cls (type): The pydantic model class

        Returns:
            dict: A dictionary of airflow parameters
        """
        field_info: dict[str, FieldInfo] = cls.model_fields
        return cls._create_params_dict(field_info)

    @classmethod
    def _create_params_dict(
        cls, field_info: dict[str, FieldInfo]
    ) -> dict[str, Union[Param, dict]]:
        """Create a dictionary of airflow parameters from a pydantic model
        used in generating the airflow UI.

        this is recursively called in a two function loop
        between the functions _create_param and _create_params_dict

        nested models are flattened into a single level dictionary
        where sub-values are delimited by a period

        ```python
        e.g.
        class BarClass(BaseModel):
            qux: str
            quz: int

        class Foo(BaseModel):
            bar: int
            baz: BarClass

        if Foo is flattened, the resulting dictionary will look like:
        {
            "bar": Param...
            "baz.qux": Param...
            "baz.quz": Param...
        }
        ```


        Args:
            field_info (dict): A dictionary of pydantic field information

        Returns:
            dict: A dictionary of airflow parameters
        """
        nested_params_dicts = {
            field_name: cls._create_param(field_info)
            for field_name, field_info in field_info.items()
        }
        return cls._flatten_params_dict(nested_params_dicts)

    @classmethod
    def _flatten_params_dict(
        cls, params_dict: dict[str, Union[Param, dict]]
    ) -> dict[str, Union[Param, dict]]:
        """flatten a nested dictionary of airflow parameters

        Args:
            params_dict (dict[str, Union[Param, dict]]): _description_

        Returns:
            dict[str, Union[Param, dict]]: _description_
        """

        if not isinstance(params_dict, dict):
            return params_dict

        if not any(isinstance(value, dict) for value in params_dict.values()):
            return params_dict

        flattened_params_dict = {}
        for key, value in params_dict.items():
            if isinstance(value, dict):
                flattened_subdict = cls._flatten_params_dict(value)

                flattened_subdict = {
                    f"{key}.{subkey}": subvalue
                    for subkey, subvalue in flattened_subdict.items()
                }

                flattened_params_dict.update(flattened_subdict)

            else:
                flattened_params_dict[key] = value
        return flattened_params_dict

    @classmethod
    def _create_param(cls, field_info: FieldInfo) -> Union[Param, dict]:
        """Create an airflow parameter from a pydantic field

        if the value of the field is a pydantic model, we recursively call
        _create_params_dict to create a nested dictionary of airflow parameters

        Args:
            field_info (FieldInfo): A pydantic field information

        Returns:
            Union[Param, dict]: An airflow parameter or a nested dictionary of airflow parameters
        """

        if issubclass(field_info.annotation, pydantic.BaseModel):  # type: ignore
            composed_class = field_info.annotation
            composed_class_model_fields = composed_class.model_fields  # type: ignore
            return cls._create_params_dict(composed_class_model_fields)

        return Param(
            default=(
                field_info.default
                if not (field_info.default == PydanticUndefined)
                else None
            ),
            **cls._create_param_type_args(field_info.annotation),  # type: ignore
            description=field_info.description,
        )

    @classmethod
    def _create_param_type_args(cls, annotation: type) -> dict[str, Any]:
        """Create a set of args to control the airflow parameter type in the UI

        since, we cannot directly pass in a type to the type kwarg of the airflow
        parameter, we have to do some type checking and then pass in the appropriate
        type to the type kwarg

        Args:
            annotation (type): The pydantic annotation

        Returns:
            dict: A dictionary of args to control the airflow parameter type in the UI
        """
        if annotation == datetime.datetime:
            return {
                "type": "string",
                "format": "date-time",
            }
        elif annotation == datetime.date:
            return {
                "type": "string",
                "format": "date",
            }
        elif issubclass(annotation, Enum):
            return {
                "type": "string",
                "enum": [x.value for x in annotation],
            }
        elif annotation == bool:
            return {
                "type": "boolean",
            }
        elif annotation == int:
            return {
                "type": "integer",
            }
        elif annotation == float:
            return {
                "type": "number",
            }
        elif annotation == str:
            return {
                "type": "string",
            }
        else:
            raise ValueError(f"Unsupported type: {annotation}")
