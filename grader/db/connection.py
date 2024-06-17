from functools import wraps

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from geowsm.env import PG_URL

engine = create_async_engine(PG_URL)
Base = declarative_base()
async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


def session_function(f):
    @wraps(f)
    async def inner(*args, **kwargs):
        session = kwargs.get("session", None)
        inner_session = session is None
        if inner_session:
            session = async_session()
            session.begin()

        kwargs["session"] = session
        try:
            result = await f(*args, **kwargs)
            if inner_session:
                await session.commit()
            return result
        except Exception as e:
            if inner_session:
                await session.rollback()
            raise e
        finally:
            if inner_session:
                await session.close()

    return inner
