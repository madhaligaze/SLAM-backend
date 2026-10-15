# -*- coding: utf-8 -*-
"""FastAPI: управление блоками/работниками + запуск автозаполнения SLAM.
API-only сервис (фронтенд деплоится отдельным проектом)."""
import os
from contextlib import asynccontextmanager
from typing import Annotated, List, Literal, Optional

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.orm import Session

from db import init_db, get_session, Block, Worker
from engine import answers as A
from jobs import JobBusy, JobManager

# headless из окружения (на сервере — да). HEADLESS=0 для отладки с окном.
HEADLESS = os.environ.get("HEADLESS", "1") != "0"
# Разрешённые источники для фронтенда (через запятую). По умолчанию — любой.
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",") if o.strip()] or ["*"]
# Сколько бригад можно заполнять одновременно (каждая держит свой chromium).
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "1"))

TaskKey = Literal["montazh", "demontazh"]
ObjectKey = Literal["sulphide_1", "sulphide_2"]


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    jobs.recover()
    yield


app = FastAPI(title="SLAM Auto-Fill API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
jobs = JobManager(headless=HEADLESS, max_concurrent=MAX_CONCURRENT_JOBS)


def db() -> Session:
    s = get_session()
    try:
        yield s
    finally:
        s.close()


# ---------- схемы ----------
class BlockIn(BaseModel):
    name: str = Field(..., max_length=200)
    company: str = Field(default="", max_length=300)
    object_key: ObjectKey = "sulphide_1"
    task: TaskKey = "montazh"


class BlockPatch(BaseModel):
    name: Optional[str] = Field(default=None, max_length=200)
    company: Optional[str] = Field(default=None, max_length=300)
    object_key: Optional[ObjectKey] = None
    task: Optional[TaskKey] = None


class WorkerIn(BaseModel):
    full_name: str = Field(default="", max_length=300)


FullName = Annotated[str, Field(max_length=300)]


class WorkersIn(BaseModel):
    """Весь состав бригады целиком, в порядке на экране."""
    names: List[FullName] = Field(default_factory=list, max_length=100)


class SubmitIn(BaseModel):
    workplace: str = Field(..., max_length=1000)
    submit: bool = True          # «Отправить» в UI = реальная отправка
    # необязательные переопределения (иначе берутся из блока)
    task: Optional[TaskKey] = None
    object_key: Optional[ObjectKey] = None
    company: Optional[str] = Field(default=None, max_length=300)
    # Состав ровно таким, каким его видит бригадир в момент нажатия. Если
    # передан — сохраняется в бригаду и отправляется именно он, а не то,
    # что успело (или не успело) долететь до БД отложенными сохранениями.
    names: Optional[List[FullName]] = Field(default=None, max_length=100)


class PhoneJobIn(BaseModel):
    """Телефон начинает отправку бригады (форму заполняет сам телефон)."""
    id: str = Field(..., min_length=8, max_length=32, pattern=r"^[A-Za-z0-9_-]+$")
    names: List[FullName] = Field(..., min_length=1, max_length=100)
    submit: bool = True
    base: dict = Field(default_factory=dict)     # workplace, task, object_key, company, device


class PhoneJobUpdate(BaseModel):
    status: Literal["running", "done", "error"]
    done: int = Field(..., ge=0)
    results: List[dict] = Field(..., max_length=100)
    fields: Optional[List[list]] = None          # протокол — обычно только в конце
    finished_at: Optional[str] = Field(default=None, max_length=40)


class PreviewIn(BaseModel):
    """Что именно движок напишет в форму — для плашки предпросмотра."""
    full_name: str = Field(default="", max_length=300)
    workplace: str = Field(default="", max_length=1000)
    company: str = Field(default="", max_length=300)
    task: TaskKey = "montazh"
    object_key: ObjectKey = "sulphide_1"


# ---------- служебное ----------
@app.get("/")
def root():
    return {"service": "SLAM Auto-Fill API", "ok": True}


@app.get("/health")
def health():
    return {"status": "ok"}


# ---------- что программа пишет в форму ----------
@app.get("/api/answers")
def answers_template():
    """Шаблон сценария с плейсхолдерами ({{full_name}} и т.п.).

    Фронтенд забирает его один раз и подставляет значения сам — плашка
    обновляется мгновенно, без запроса на каждый набранный символ. Тексты
    берутся из engine/answers.py, то есть ровно те, что уйдут в форму."""
    return A.plan_template()


@app.post("/api/preview")
def preview(data: PreviewIn):
    """Полностью раскрытый сценарий — той же функцией, что и реальный прогон."""
    return {"pages": A.resolve_plan(data.full_name, data.workplace, data.task,
                                    data.object_key, data.company)}


# ---------- блоки ----------
@app.get("/api/blocks")
def list_blocks(s: Session = Depends(db)):
    blocks = s.query(Block).order_by(Block.id).all()
    last = jobs.summaries(b.id for b in blocks)
    out = []
    for b in blocks:
        d = b.to_dict()
        d["last_job"] = last.get(b.id)      # идёт ли отправка / чем кончилась
        out.append(d)
    return out


@app.post("/api/blocks")
def create_block(data: BlockIn, s: Session = Depends(db)):
    name = data.name.strip()
    if not name:
        raise HTTPException(400, "Не указано название бригады")
    b = Block(name=name, company=data.company.strip(),
              object_key=data.object_key, task=data.task)
    s.add(b); s.commit(); s.refresh(b)
    return b.to_dict()


@app.patch("/api/blocks/{block_id}")
def update_block(block_id: int, data: BlockPatch, s: Session = Depends(db)):
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    for f in ("name", "company", "object_key", "task"):
        v = getattr(data, f)
        if v is not None:
            if isinstance(v, str):
                v = v.strip()
            if f == "name" and not v:
                raise HTTPException(400, "Не указано название бригады")
            setattr(b, f, v)
    s.commit(); s.refresh(b)
    return b.to_dict()


@app.delete("/api/blocks/{block_id}")
def delete_block(block_id: int, s: Session = Depends(db)):
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    s.delete(b); s.commit()
    jobs.forget_block(block_id)
    return {"ok": True}


# ---------- работники ----------
def _replace_workers(s: Session, b: Block, names: List[str]):
    """Привести состав бригады к списку names (пустые строки отбрасываются).

    Существующие строки переиспользуются по порядку, лишние удаляются."""
    names = [n.strip() for n in names if n and n.strip()]
    existing = list(b.workers)
    for i, name in enumerate(names):
        if i < len(existing):
            existing[i].full_name = name
            existing[i].order_index = i
        else:
            b.workers.append(Worker(full_name=name, order_index=i))
    for w in existing[len(names):]:
        b.workers.remove(w)                 # delete-orphan удалит строку
    s.commit(); s.refresh(b)


@app.put("/api/blocks/{block_id}/workers")
def set_workers(block_id: int, data: WorkersIn, s: Session = Depends(db)):
    """Сохранить состав целиком одним запросом.

    Раньше каждое ФИО сохранялось своим PATCH с задержкой, а добавление шло
    через POST+PATCH. Стоило выйти из бригады до того, как они долетят, —
    экран показывал старый состав, а правка поверх него затирала новый."""
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    _replace_workers(s, b, data.names)
    return b.to_dict()


@app.post("/api/blocks/{block_id}/workers")
def add_worker(block_id: int, data: WorkerIn, s: Session = Depends(db)):
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    # max+1, а не len(): после удаления работника len() повторяет уже занятый
    # order_index, и порядок в списке начинает «прыгать».
    top = s.query(func.max(Worker.order_index)).filter(Worker.block_id == block_id).scalar()
    w = Worker(block_id=block_id, full_name=data.full_name.strip(),
               order_index=(top + 1) if top is not None else 0)
    s.add(w); s.commit(); s.refresh(b)
    return b.to_dict()


@app.patch("/api/workers/{worker_id}")
def update_worker(worker_id: int, data: WorkerIn, s: Session = Depends(db)):
    w = s.get(Worker, worker_id)
    if not w:
        raise HTTPException(404, "Работник не найден")
    w.full_name = data.full_name.strip()
    s.commit()
    return {"ok": True}


@app.delete("/api/workers/{worker_id}")
def delete_worker(worker_id: int, s: Session = Depends(db)):
    w = s.get(Worker, worker_id)
    if not w:
        raise HTTPException(404, "Работник не найден")
    s.delete(w); s.commit()
    return {"ok": True}


# ---------- запуск автозаполнения ----------
@app.post("/api/blocks/{block_id}/submit")
def submit_block(block_id: int, data: SubmitIn, s: Session = Depends(db)):
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    if jobs.active_for_block(block_id):
        raise HTTPException(409, "По этой бригаде уже идёт отправка")
    if data.names is not None:
        _replace_workers(s, b, data.names)
    workers = []
    for w in b.workers:
        full_name = (w.full_name or "").strip()
        if full_name:
            item = w.to_dict()
            item["full_name"] = full_name
            workers.append(item)
    if not workers:
        raise HTTPException(400, "В блоке нет работников")
    workplace = data.workplace.strip()
    if not workplace:
        raise HTTPException(400, "Не указано наименование рабочего места")

    # переопределения сохраняем в блок (кроме workplace — он не хранится)
    if data.task: b.task = data.task
    if data.object_key: b.object_key = data.object_key
    if data.company is not None: b.company = data.company.strip()
    if not (b.company or "").strip():
        raise HTTPException(400, "Не указана подрядная организация")
    s.commit()

    base = {
        "workplace": workplace,
        "task": b.task,
        "object_key": b.object_key,
        "company": b.company.strip(),
    }
    try:
        job = jobs.start(b.id, b.name, workers, base, submit=data.submit)
    except JobBusy:
        raise HTTPException(409, "По этой бригаде уже идёт отправка")
    return job.to_dict()


@app.post("/api/blocks/{block_id}/phone-jobs")
def phone_job_start(block_id: int, data: PhoneJobIn, s: Session = Depends(db)):
    """Телефон сообщает, что начинает отправку бригады.

    Форму заполняет сам телефон — сервер только хранит ход и итог, чтобы
    их видели все бригадиры, и не даёт отправить бригаду дважды (409)."""
    b = s.get(Block, block_id)
    if not b:
        raise HTTPException(404, "Блок не найден")
    try:
        return jobs.phone_start(b.id, b.name, data.id, [n.strip() for n in data.names],
                                data.base, data.submit)
    except JobBusy:
        raise HTTPException(409, "По этой бригаде уже идёт отправка")


@app.put("/api/phone-jobs/{job_id}")
def phone_job_update(job_id: str, data: PhoneJobUpdate):
    """Ход отправки с телефона: после каждого работника и в конце."""
    job = jobs.phone_update(job_id, data.status, data.done, data.results,
                            data.fields, data.finished_at)
    if job is None:
        raise HTTPException(404, "Отправка с телефона не найдена")
    return job


@app.get("/api/blocks/{block_id}/job")
def block_job(block_id: int):
    """Последняя отправка бригады (идущая или завершённая), либо null.

    По ней клиент, вернувшись из фона или после перезагрузки страницы,
    снова находит задачу и показывает её ход и итог."""
    return jobs.latest_for_block(block_id)


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str):
    """Лёгкий ответ для опроса прогресса: без протокола полей (см. ниже)."""
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(404, "Задача не найдена")
    return job


@app.get("/api/jobs/{job_id}/fields/{index}")
def job_fields(job_id: str, index: int):
    """Протокол одного работника: что реально оказалось в каждом поле формы.

    Запрашивается только когда пользователь раскрыл плашку — иначе опрос
    прогресса возил бы по мобильной сети десятки килобайт каждые 1.5 с."""
    fields = jobs.fields_of(job_id, index)
    if fields is None:
        raise HTTPException(404, "Задача или работник не найдены")
    return {"fields": fields}
