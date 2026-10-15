# -*- coding: utf-8 -*-
"""Менеджер фоновых задач: для блока отправляет по одной форме на каждого
работника, ведёт прогресс и пер-результаты (диагностика).

Один браузер на всю задачу: раньше он поднимался и гасился на каждого
работника, что добавляло по несколько секунд к каждой форме.

Задача не зависит от клиента: бригадир может свернуть приложение или закрыть
вкладку — поток доработает сам. Состояние пишется в БД (таблица jobs), чтобы
вернувшийся клиент мог найти задачу своей бригады и показать ход и итог.
"""
import copy
import datetime as dt
import threading
import uuid
from collections import OrderedDict
from typing import Dict, Iterable, List, Optional

from db import JobRecord, get_session
from engine.slam_filler import SlamFiller, WorkerData
from engine import answers as A

# Сколько завершённых задач держать в памяти (иначе процесс растёт бесконечно).
MAX_JOBS = 200
# Сколько последних задач одной бригады хранить в БД.
KEEP_PER_BLOCK = 10
ACTIVE = ("queued", "running")


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds")


def _light(result: dict) -> dict:
    """Результат работника без протокола полей, но со счётчиками по нему."""
    fields = result.get("fields") or []
    out = {k: v for k, v in result.items() if k != "fields"}
    out["fields_total"] = len(fields)
    out["fields_verified"] = sum(1 for f in fields if f.get("verified"))
    return out


def _summary(job: dict) -> dict:
    """Короткая сводка для списка бригад: идёт ли отправка и чем кончилась."""
    results = job.get("results") or []
    return {
        "id": job["id"], "status": job["status"], "submit": job["submit"],
        "total": job["total"], "done": job["done"],
        "sent": sum(1 for r in results if r.get("submitted")),
        "failed": sum(1 for r in results if r.get("status") == "failed"),
        "unconfirmed": sum(1 for r in results if r.get("status") == "unconfirmed"),
        "created_at": job["created_at"], "finished_at": job["finished_at"],
    }


def _record_dict(rec: JobRecord) -> dict:
    return {
        "id": rec.id, "block_id": rec.block_id, "block_name": rec.block_name,
        "status": rec.status, "submit": rec.submit, "total": rec.total,
        "done": rec.done, "created_at": rec.created_at,
        "started_at": rec.started_at, "finished_at": rec.finished_at,
        "base": dict(rec.base or {}), "results": copy.deepcopy(rec.results or []),
    }


class JobBusy(Exception):
    """По бригаде уже идёт отправка — вторая отправила бы формы повторно."""

    def __init__(self, job: "Job"):
        super().__init__(job.id)
        self.job = job


class Job:
    def __init__(self, job_id: str, block_id: Optional[int], block_name: str,
                 workers: List[dict], base: dict, submit: bool):
        self.id = job_id
        self.block_id = block_id
        self.block_name = block_name
        self.submit = submit
        self.base = base                  # workplace, task, object_key, company
        self.status = "queued"            # queued|running|done|error
        self.total = len(workers)
        self.done = 0
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.created_at = _now()
        self.results: List[dict] = [
            {"worker_id": w.get("id"), "full_name": w["full_name"],
             "status": "pending", "submitted": False,
             "steps": [], "errors": [], "warnings": [], "fields": [], "elapsed": 0.0}
            for w in workers
        ]
        self._workers = workers
        self._lock = threading.Lock()

    def snapshot(self) -> dict:
        """Полная копия состояния (с протоколом полей)."""
        with self._lock:
            return {
                "id": self.id, "block_id": self.block_id, "block_name": self.block_name,
                "status": self.status, "submit": self.submit, "total": self.total,
                "done": self.done, "created_at": self.created_at,
                "started_at": self.started_at, "finished_at": self.finished_at,
                "base": dict(self.base), "results": copy.deepcopy(self.results),
            }

    def to_dict(self, with_fields: bool = False):
        """with_fields=False — «лёгкий» ответ для опроса прогресса.

        Протокол полей одного работника весит ~30 КБ (одни только тексты мер
        контроля), и гонять его каждые полторы секунды по мобильной сети
        незачем: вместо него отдаём счётчики, а сам протокол — по запросу
        (GET /api/jobs/{id}/fields/{index}), когда пользователь раскроет плашку.
        """
        d = self.snapshot()
        if not with_fields:
            d["results"] = [_light(r) for r in d["results"]]
        return d

    @property
    def active(self) -> bool:
        return self.status in ACTIVE

    def fields_of(self, index: int) -> Optional[list]:
        with self._lock:
            if 0 <= index < len(self.results):
                return copy.deepcopy(self.results[index].get("fields") or [])
        return None

    def set_meta(self, **values):
        with self._lock:
            for key, value in values.items():
                setattr(self, key, value)

    def update_result(self, index: int, **values):
        with self._lock:
            self.results[index].update(values)

    def append_result(self, item: dict):
        with self._lock:
            self.results.append(item)

    def increment_done(self):
        with self._lock:
            self.done += 1


class JobManager:
    def __init__(self, headless: bool = True, max_concurrent: int = 1,
                 persist: bool = True):
        self._jobs: "OrderedDict[str, Job]" = OrderedDict()
        self._lock = threading.Lock()
        self.headless = headless
        self.persist = persist
        # Каждая задача держит свой chromium (сотни МБ). Без ограничения две
        # бригады, нажавшие «Отправить» одновременно, кладут контейнер.
        # Лишние задачи честно ждут в статусе «в очереди».
        self._slots = threading.Semaphore(max(1, max_concurrent))

    # ---------- хранение ----------

    def _save(self, job: Job, with_fields: bool = False):
        """Записать состояние задачи в БД. Сбой БД не должен ронять отправку:
        в памяти состояние остаётся, клиент его увидит, пока процесс жив."""
        if not self.persist:
            return
        snap = job.snapshot()
        try:
            with get_session() as s:
                rec = s.get(JobRecord, snap["id"]) or JobRecord(id=snap["id"])
                for key in ("block_id", "block_name", "status", "submit", "total", "done",
                            "created_at", "started_at", "finished_at", "base"):
                    setattr(rec, key, snap[key])
                rec.results = [_light(r) for r in snap["results"]]
                # Протокол перезаписываем, только когда он поменялся: обращение
                # к отложенной колонке тянуло бы десятки КБ на каждый шаг.
                if with_fields:
                    rec.fields = [r.get("fields") or [] for r in snap["results"]]
                s.add(rec)
                s.commit()
        except Exception as e:                      # noqa: BLE001
            print(f"[jobs] не удалось сохранить задачу {job.id}: {type(e).__name__}: {e}")

    def _trim(self, block_id: int):
        """Оставить в БД только последние KEEP_PER_BLOCK задач бригады."""
        if not self.persist or block_id is None:
            return
        try:
            with get_session() as s:
                old = (s.query(JobRecord.id)
                       .filter(JobRecord.block_id == block_id,
                               JobRecord.status.notin_(ACTIVE))
                       .order_by(JobRecord.created_at.desc())
                       .offset(KEEP_PER_BLOCK).all())
                if old:
                    s.query(JobRecord).filter(JobRecord.id.in_([r.id for r in old])) \
                        .delete(synchronize_session=False)
                    s.commit()
        except Exception as e:                      # noqa: BLE001
            print(f"[jobs] не удалось почистить историю бригады {block_id}: {e}")

    def recover(self):
        """После перезапуска сервера незавершённые задачи из БД уже никто не
        доделает. Помечаем их честно: кто был в работе — «не подтверждено»
        (форма могла успеть уйти), до кого очередь не дошла — «ошибка»."""
        if not self.persist:
            return
        with get_session() as s:
            for rec in s.query(JobRecord).filter(JobRecord.status.in_(ACTIVE)).all():
                results = copy.deepcopy(rec.results or [])
                for r in results:
                    if r.get("status") == "running":
                        r["status"] = "unconfirmed"
                        r["errors"] = (r.get("errors") or []) + [
                            "Сервер перезапустился во время заполнения этой формы — "
                            "неизвестно, ушла ли она. Проверьте в ответах формы."]
                    elif r.get("status") == "pending":
                        r["status"] = "failed"
                        r["errors"] = (r.get("errors") or []) + [
                            "Не отправлялось: сервер перезапустился раньше, чем дошла очередь."]
                rec.results = results
                rec.status = "error"
                rec.finished_at = rec.finished_at or _now()
            s.commit()

    def forget_block(self, block_id: int):
        """Бригаду удалили — её история больше не нужна (идущая задача доработает)."""
        with self._lock:
            for job in self._jobs.values():
                if job.block_id == block_id and not job.active:
                    job.block_id = None
        if not self.persist:
            return
        with get_session() as s:
            s.query(JobRecord).filter(JobRecord.block_id == block_id,
                                      JobRecord.status.notin_(ACTIVE)) \
                .delete(synchronize_session=False)
            s.commit()

    # ---------- чтение ----------

    def _mem(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def get(self, job_id: str) -> Optional[dict]:
        """Лёгкий словарь задачи: из памяти, а если процесс перезапускался — из БД."""
        job = self._mem(job_id)
        if job:
            return job.to_dict(with_fields=False)
        if not self.persist:
            return None
        with get_session() as s:
            rec = s.get(JobRecord, job_id)
            return _record_dict(rec) if rec else None

    def fields_of(self, job_id: str, index: int) -> Optional[list]:
        """None — нет задачи или работника с таким номером."""
        job = self._mem(job_id)
        if job:
            return job.fields_of(index)
        if not self.persist:
            return None
        with get_session() as s:
            rec = s.get(JobRecord, job_id)
            if not rec or not (0 <= index < len(rec.results or [])):
                return None
            fields = rec.fields or []
            return copy.deepcopy(fields[index]) if index < len(fields) else []

    def active_for_block(self, block_id: int) -> Optional[Job]:
        with self._lock:
            for job in reversed(self._jobs.values()):
                if job.block_id == block_id and job.active:
                    return job
        return None

    def latest_for_block(self, block_id: int) -> Optional[dict]:
        """Последняя задача бригады (идущая или завершённая) — лёгкий словарь."""
        with self._lock:
            mem = [j for j in self._jobs.values() if j.block_id == block_id]
        if mem:
            # Всё, что создано за жизнь процесса, лежит в памяти и новее записей БД.
            return mem[-1].to_dict(with_fields=False)
        if not self.persist:
            return None
        with get_session() as s:
            rec = (s.query(JobRecord).filter(JobRecord.block_id == block_id)
                   .order_by(JobRecord.created_at.desc()).first())
            return _record_dict(rec) if rec else None

    def summaries(self, block_ids: Iterable[int]) -> Dict[int, dict]:
        """Сводка по последней задаче каждой бригады — одним запросом к БД."""
        ids = list(block_ids)
        out: Dict[int, dict] = {}
        if self.persist and ids:
            with get_session() as s:
                recs = (s.query(JobRecord).filter(JobRecord.block_id.in_(ids))
                        .order_by(JobRecord.created_at.desc()).all())
                for rec in recs:
                    if rec.block_id not in out:
                        out[rec.block_id] = _summary(_record_dict(rec))
        with self._lock:
            mem = list(self._jobs.values())
        for job in mem:                      # память свежее БД
            if job.block_id in ids:
                out[job.block_id] = _summary(job.to_dict())
        return out

    # ---------- запуск ----------

    def start(self, block_id: Optional[int], block_name: str, workers: List[dict],
              base: dict, submit: bool) -> Job:
        job_id = uuid.uuid4().hex[:12]
        job = Job(job_id, block_id, block_name, workers, base, submit)
        with self._lock:
            # Проверка и регистрация под одним замком: двойное нажатие
            # «Отправить» не должно запустить две задачи на одну бригаду.
            if block_id is not None:
                for other in self._jobs.values():
                    if other.block_id == block_id and other.active:
                        raise JobBusy(other)
            self._jobs[job_id] = job
            self._evict_locked()
        self._save(job, with_fields=True)
        self._trim(block_id)
        threading.Thread(target=self._run, args=(job,), daemon=True).start()
        return job

    def _evict_locked(self):
        """Выбросить самые старые завершённые задачи сверх лимита."""
        while len(self._jobs) > MAX_JOBS:
            for jid, j in list(self._jobs.items()):
                if j.status in ("done", "error"):
                    del self._jobs[jid]
                    break
            else:
                break        # все оставшиеся ещё выполняются — не трогаем

    def _run(self, job: Job):
        with self._slots:
            self._run_locked(job)

    def _run_locked(self, job: Job):
        job.set_meta(status="running", started_at=_now())
        self._save(job)
        try:
            # sync-Playwright привязан к потоку — поднимаем браузер здесь,
            # внутри рабочего потока, и держим один на всю бригаду.
            with SlamFiller(headless=self.headless) as filler:
                for i, w in enumerate(job._workers):
                    job.update_result(i, status="running")
                    self._save(job)
                    data = WorkerData(
                        full_name=w["full_name"],
                        workplace=job.base["workplace"],
                        task=job.base["task"],
                        object_key=job.base["object_key"],
                        company=job.base["company"],
                    )
                    try:
                        res = filler.fill_one(data, submit=job.submit)
                        job.update_result(
                            i,
                            steps=res.steps,
                            errors=res.errors,
                            warnings=res.warnings,
                            fields=res.fields,
                            submitted=res.submitted,
                            elapsed=res.elapsed,
                            status=res.status,   # ok | unconfirmed | failed
                        )
                    except Exception as e:
                        job.update_result(i, status="failed",
                                          errors=[f"{type(e).__name__}: {e}"])
                    job.increment_done()
                    self._save(job, with_fields=True)
            job.set_meta(status="done")
        except Exception as e:
            job.set_meta(status="error")
            job.append_result({"worker_id": None, "full_name": "—", "status": "failed",
                               "submitted": False, "steps": [], "warnings": [],
                               "fields": [], "elapsed": 0.0,
                               "errors": [f"Сбой браузера: {type(e).__name__}: {e}"]})
            # Работники, до которых не дошли, не должны выглядеть «ожидающими».
            for i, r in enumerate(job.results):
                if r["status"] in ("pending", "running"):
                    job.update_result(i, status="failed",
                                      errors=r["errors"] + ["Задача прервана сбоем браузера."])
        finally:
            job.set_meta(finished_at=_now())
            self._save(job, with_fields=True)

    # ---- предпросмотр сценария (то же, что покажет фронтенд) ----
    @staticmethod
    def preview(full_name: str, workplace: str, task: str, object_key: str,
                company: str) -> list:
        return A.resolve_plan(full_name, workplace, task, object_key, company)
