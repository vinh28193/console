# flake8: noqa: F401
import threading
from contextvars import ContextVar
from typing import Final, Optional, Dict, Any

from asgiref.local import Local
from consoles.conf import settings
from consoles.utils.functional import cached_property
from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import StaticPool
from .base import ModelBase
from .key_value_store import KeyStoreKeys, KeyValueStore
from .pairlock_middleware import PairLocks
from .trade_model import LocalTrade, Order, Trade
from .usedb_context import (
    FtNoDBContext, disable_database_use, enable_database_use
)

REQUEST_ID_CTX_KEY: Final[str] = 'request_id'
_request_id_ctx_var: ContextVar[Optional[str]] = ContextVar(REQUEST_ID_CTX_KEY,
                                                            default=None)


def get_request_or_thread_id() -> Optional[str]:
    """
    Helper method to get either async context (for fastapi requests),
    or thread id
    """
    _id = _request_id_ctx_var.get()
    if _id is None:
        # when not in request context - use thread id
        _id = str(threading.current_thread().ident)

    return _id


_SQL_DOCS_URL = 'http://docs.sqlalchemy.org/en/latest/core/engines.html' \
                '#database-urls'


def init_db():
    """
    Initializes this module with the given config,
    registers all known command handlers
    and starts polling for message updates
    :param db_url: Database to use
    :return: None
    """
    kwargs: Dict[str, Any] = {}
    db_url = settings.DATABASE
    if db_url == 'sqlite:///':
        raise Exception(
            f'Bad db-url {db_url}. For in-memory database, please use '
            f'`sqlite://`.')
    if db_url == 'sqlite://':
        kwargs.update({
            'poolclass': StaticPool,
        })
    # Take care of thread ownership
    if db_url.startswith('sqlite://'):
        kwargs.update({
            'connect_args': {'check_same_thread': False},
        })

    try:
        engine = create_engine(db_url, future=True, **kwargs)
    except NoSuchModuleError:
        raise Exception(
            f"Given value for db_url: '{db_url}' "

            f"is no valid database URL! (See {_SQL_DOCS_URL})"
        )

    # https://docs.sqlalchemy.org/en/13/orm/contextual.html#thread-local-scope
    # Scoped sessions proxy requests to the appropriate thread-local session.
    # Since we also use fastAPI, we need to make it aware of the request id, too
    engine = scoped_session(
        sessionmaker(bind=engine, autoflush=False),
        scopefunc=get_request_or_thread_id)
    ModelBase.metadata.create_all(engine)

    check_migrate(engine, decl_base=ModelBase, previous_tables=previous_tables)
