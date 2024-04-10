# List of pairs with their timeframes
from typing import Tuple, List, Literal, Dict, Any

from fasttraders.enums import CandleType

# DataFrame columns
DEFAULT_DATAFRAME_COLUMNS = ['date', 'open', 'high', 'low', 'close', 'volume']
DEFAULT_TRADES_COLUMNS = ['timestamp', 'id', 'type', 'side', 'price', 'amount', 'cost']
TRADES_DTYPES = {
    'timestamp': 'int64',
    'id': 'str',
    'type': 'str',
    'side': 'str',
    'price': 'float64',
    'amount': 'float64',
    'cost': 'float64',
}
# datetime
DATETIME_PRINT_FORMAT = '%Y-%m-%d %H:%M:%S'

# Typing

PairWithTimeframe = Tuple[str, str, CandleType]
ListPairsWithTimeframes = List[PairWithTimeframe]

TradeList = List[List]

LongShort = Literal['long', 'short']
EntryExit = Literal['entry', 'exit']
BuySell = Literal['buy', 'sell']
MakerTaker = Literal['maker', 'taker']
BidAsk = Literal['bid', 'ask']
OBLiteral = Literal['asks', 'bids']

IntOrInf = float


EntryExecuteMode = Literal['initial', 'pos_adjust', 'replace']