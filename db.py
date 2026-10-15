# -*- coding: utf-8 -*-
"""БД: блоки (бригады) и работники. Postgres (Railway) или SQLite локально."""
import os
import datetime as dt
from typing import Optional
from sqlalchemy import (create_engine, Integer, String, Text, DateTime, ForeignKey,
                        Boolean, JSON, inspect, text)
from sqlalchemy.orm import (DeclarativeBase, Mapped, mapped_column, relationship,
                            sessionmaker)

def _clean_db_url(raw: str) -> str:
    """Подстраховка от частой ошибки в панели Railway: в значение переменной
    случайно попадает сам ключ ('DATABASE_URL=') и/или кавычки."""
    if not raw:
        return ""
    raw = raw.strip()
    # убрать случайный префикс 'DATABASE_URL=' (в т.ч. в кавычках)
    for prefix in ("DATABASE_URL=", "database_url="):
        if raw.startswith(prefix):
            raw = raw[len(prefix):].strip()
    # убрать обрамляющие кавычки
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
        raw = raw[1:-1].strip()
    return raw


# Railway даёт DATABASE_URL для Postgres; локально — SQLite-файл.
DATABASE_URL = _clean_db_url(os.environ.get("DATABASE_URL", "")) or "sqlite:///./slam.db"
# SQLAlchemy требует postgresql:// (а не postgres://)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Понятная ошибка, если ссылка на Postgres не раскрылась (пустые логин/хост).
if DATABASE_URL.startswith("postgresql://") and "@" in DATABASE_URL:
    creds = DATABASE_URL.split("://", 1)[1].split("@", 1)[0]
    if creds in ("", ":"):
        raise RuntimeError(
            "DATABASE_URL указывает на пустые логин/пароль — похоже, переменная "
            "на Railway задана неверно. В сервисе бэкенда задайте "
            "DATABASE_URL = ${{Postgres.DATABASE_URL}} (без кавычек и префикса)."
        )

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Block(Base):
    """Бригада. Название произвольное (ставит бригадир). Хранит постоянные данные."""
    __tablename__ = "blocks"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    company: Mapped[str] = mapped_column(String(300), default="")     # подрядная организация (сохраняется)
    object_key: Mapped[str] = mapped_column(String(20), default="sulphide_1")  # sulphide_1|sulphide_2
    task: Mapped[str] = mapped_column(String(20), default="montazh")  # montazh|demontazh
    created_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow)

    workers: Mapped[list["Worker"]] = relationship(
        back_populates="block", cascade="all, delete-orphan", order_by="Worker.order_index")

    def to_dict(self):
        return {
            "id": self.id, "name": self.name, "company": self.company,
            "object_key": self.object_key, "task": self.task,
            "workers": [w.to_dict() for w in self.workers],
        }


class Worker(Base):
    """Работник бригады. ФИО сохраняется."""
    __tablename__ = "workers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    block_id: Mapped[int] = mapped_column(ForeignKey("blocks.id", ondelete="CASCADE"))
    full_name: Mapped[str] = mapped_column(String(300))
    order_index: Mapped[int] = mapped_column(Integer, default=0)

    block: Mapped[Block] = relationship(back_populates="workers")

    def to_dict(self):
        return {"id": self.id, "full_name": self.full_name, "order_index": self.order_index}


class JobRecord(Base):
    """Отправка за бригаду. Хранится в БД, а не только в памяти процесса:
    бригадир сворачивает приложение, телефон выгружает вкладку — вернувшись,
    он должен увидеть ход и итог отправки, а не пустую форму с активной
    кнопкой (и не отправить бригаду второй раз)."""
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    # Без внешнего ключа: задача доживает до конца, даже если бригаду удалили.
    block_id: Mapped[Optional[int]] = mapped_column(Integer, index=True, nullable=True)
    block_name: Mapped[str] = mapped_column(String(200), default="")
    status: Mapped[str] = mapped_column(String(20), default="queued")   # queued|running|done|error
    # server — заполняет движок этого сервера; phone — приложение на телефоне,
    # оно присылает ход отправки само (сервер тогда только хранит и показывает).
    source: Mapped[str] = mapped_column(String(20), default="server")
    # Последний признак жизни. Отправка с телефона, о которой давно ничего не
    # слышно, считается оборвавшейся (телефон разрядился, приложение убили).
    updated_at: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    submit: Mapped[bool] = mapped_column(Boolean, default=True)
    total: Mapped[int] = mapped_column(Integer, default=0)
    done: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[str] = mapped_column(String(40), default="", index=True)  # ISO UTC
    started_at: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    finished_at: Mapped[Optional[str]] = mapped_column(String(40), nullable=True)
    base: Mapped[dict] = mapped_column(JSON, default=dict)
    # Результаты без протокола полей — их читает каждый опрос прогресса.
    results: Mapped[list] = mapped_column(JSON, default=list)
    # Протокол полей по работникам (десятки КБ) — грузится только по запросу.
    fields: Mapped[list] = mapped_column(JSON, default=list, deferred=True)


def init_db():
    Base.metadata.create_all(engine)
    _add_missing_columns()


def _add_missing_columns():
    """create_all создаёт недостающие таблицы, но не колонки. Колонки jobs,
    появившиеся после первого выката таблицы, добавляем сами."""
    cols = {c["name"] for c in inspect(engine).get_columns("jobs")}
    added = {
        "source": "VARCHAR(20) DEFAULT 'server'",
        "updated_at": "VARCHAR(40)",
    }
    with engine.begin() as conn:
        for name, ddl in added.items():
            if name not in cols:
                conn.execute(text(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}"))


def get_session():
    return SessionLocal()
