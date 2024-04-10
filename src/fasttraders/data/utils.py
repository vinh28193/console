from pathlib import Path
from typing import Optional, Type

from .handler import DataHandler


def get_datahandlerclass(datatype: str) -> Type[DataHandler]:
    """
    Get datahandler class.
    Could be done using Resolvers, but since this may be called often and
    resolvers
    are rather expensive, doing this directly should improve performance.
    :param datatype: datatype to use.
    :return: Datahandler class
    """

    if datatype == 'json':
        from .jsondatahandler import JsonDataHandler
        return JsonDataHandler
    else:
        raise ValueError(f"No datahandler for datatype {datatype} available.")


def get_datahandler(
    datadir: Path, data_format: Optional[str] = None,
    data_handler: Optional[DataHandler] = None
) -> DataHandler:
    """
    :param datadir: Folder to save data
    :param data_format: dataformat to use
    :param data_handler: returns this datahandler if it exists or initializes
    a new one
    """

    if not data_handler:
        HandlerClass = get_datahandlerclass(data_format or 'json')
        data_handler = HandlerClass(datadir)
    return data_handler
