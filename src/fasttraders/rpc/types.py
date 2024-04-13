from datetime import datetime
from typing import Any, List, Literal, Optional, TypedDict, Union

from fasttraders.constants import PairWithTimeframe
from fasttraders.enums import RPCMessageType

ProfitLossStr = Literal["profit", "loss"]


class RPCSendMsgBase(TypedDict):
    pass
    # ty1pe: Literal[RPCMessageType]


class RPCStatusMsg(RPCSendMsgBase):
    """Used for Status, Startup and Warning messages"""
    type: Literal[
        RPCMessageType.STATUS, RPCMessageType.STARTUP, RPCMessageType.WARNING]
    status: str


class _AnalyzedDFData(TypedDict):
    key: PairWithTimeframe
    df: Any
    la: datetime


class RPCAnalyzedDFMsg(RPCSendMsgBase):
    """New Analyzed dataframe message"""
    type: Literal[RPCMessageType.ANALYZED_DF]
    data: _AnalyzedDFData


RPCSendMsg = Union[
    RPCStatusMsg,
]
