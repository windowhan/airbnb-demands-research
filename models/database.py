"""DB 연결 및 세션 관리"""

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config.settings import DATA_DIR, DB_PATH
from models.schema import Base


def get_engine(db_path=None):
    """SQLAlchemy 엔진을 생성합니다."""
    path = db_path or DB_PATH
    return create_engine(
        f"sqlite:///{path}",
        echo=False,
        connect_args={"check_same_thread": False},
    )


_engine = None
_SessionFactory = None


def init_db(db_path=None):
    """DB를 초기화합니다. data/ 디렉토리 생성 + 테이블 생성."""
    global _engine, _SessionFactory

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    _engine = get_engine(db_path)
    Base.metadata.create_all(_engine)
    _SessionFactory = sessionmaker(bind=_engine)
    return _engine


def get_session() -> Session:
    """새 세션을 반환합니다."""
    if _SessionFactory is None:
        init_db()
    return _SessionFactory()


@contextmanager
def session_scope():
    """트랜잭션 단위 세션 컨텍스트 매니저."""
    session = get_session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
