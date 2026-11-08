"""Загрузка набора по group_id: API организаторов → fallback на data/{group_id}.json → БД."""
from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Literal

import httpx
from pydantic import BaseModel, ValidationError

from backend import db
from backend.config import get_settings

log = logging.getLogger(__name__)
GROUP_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


class TranscriptItem(BaseModel):
    timestamp: str
    speaker: Literal["client", "operator", "bot", "system"]
    text: str


class Record(BaseModel):
    conversation_id: str
    channel: str
    language: str | None = None
    transcript: list[TranscriptItem]
    audio_url: str | None = None


class DataSourceError(RuntimeError):
    pass


def parse_payload(payload: dict | list) -> list[dict]:
    """Принимает ответ API ({isSuccess, data: {records}}), {records}, список записей или одну запись
    ({conversation_id, transcript, …}). Возвращает записи в исходном виде (поля не переписываются), после проверки схемы."""
    if isinstance(payload, dict) and "transcript" in payload and "records" not in payload:
        payload = [payload]                                   # один диалог как объект
    if isinstance(payload, dict):
        if payload.get("isSuccess") is False:
            raise DataSourceError(f"источник вернул ошибку: {payload.get('error')!r}")
        data = payload.get("data", payload)
        records = data.get("records") if isinstance(data, dict) else None
    else:
        records = payload
    if not isinstance(records, list):
        raise DataSourceError("в ответе нет data.records[]")
    try:
        for r in records:
            Record.model_validate(r)
    except ValidationError as e:
        raise DataSourceError(f"запись не соответствует формату: {e.errors()[0]}") from e
    ids = [r["conversation_id"] for r in records]
    if len(ids) != len(set(ids)):
        raise DataSourceError("дубли conversation_id в наборе")
    return records


def fetch_remote(group_id: str, url: str, timeout_s: float, client: httpx.Client | None = None) -> list[dict]:
    own = client is None
    client = client or httpx.Client(timeout=timeout_s)
    try:
        resp = client.get(url, params={"group_id": group_id})
        resp.raise_for_status()
        return parse_payload(resp.json())
    except (httpx.HTTPError, ValueError) as e:
        raise DataSourceError(f"API недоступно: {e}") from e
    finally:
        if own:
            client.close()


def load_local(group_id: str, data_dir: Path) -> list[dict]:
    path = data_dir / f"{group_id.lower()}.json"
    if not path.exists():
        path = data_dir / f"{group_id}.json"
    if not path.exists():
        raise DataSourceError(f"нет локального файла {path}")
    return parse_payload(json.loads(path.read_text(encoding="utf-8")))


def import_group(conn: sqlite3.Connection, group_id: str, prefer: Literal["remote", "local"] = "remote",
                 client: httpx.Client | None = None) -> dict:
    """Загружает набор и сохраняет в БД. remote: API, при ошибке — локальный файл."""
    if not GROUP_ID_RE.match(group_id):
        raise DataSourceError(f"некорректный group_id {group_id!r}")
    s = get_settings()
    errors = []
    if prefer == "remote" and s.data_api_url:
        try:
            records = fetch_remote(group_id, s.data_api_url, s.data_api_timeout_s, client)
            return db.save_dataset(conn, group_id, "remote", records)
        except DataSourceError as e:
            log.warning("group_id=%s: %s; пробую локальный файл", group_id, e)
            errors.append(str(e))
    elif prefer == "remote":
        errors.append("DATA_API_URL не задан")
    try:
        records = load_local(group_id, s.data_dir)
    except DataSourceError as e:
        raise DataSourceError("; ".join(errors + [str(e)])) from e
    result = db.save_dataset(conn, group_id, "local", records)
    result["warnings"] = errors
    return result


def payload_group_id(payload: dict | list) -> str | None:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict) and data.get("dataset_group_id"):
            return str(data["dataset_group_id"])
        if payload.get("dataset_group_id"):
            return str(payload["dataset_group_id"])
    return None


def save_raw(group_id: str, content: bytes, data_dir: Path | None = None, source: str = "upload") -> Path:
    """Сохраняет полученный набор как есть: data/uploads/<group>_<источник>_<время>.json; если data/<group>.json
    ещё нет — кладёт копию туда (её подхватит импорт с prefer=local и evaluate_group.py)."""
    data_dir = data_dir or get_settings().data_dir
    folder = data_dir / "uploads"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{group_id}_{source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path.write_bytes(content)
    local = data_dir / f"{group_id}.json"
    if not local.exists() and not (data_dir / f"{group_id.lower()}.json").exists():
        local.write_bytes(content)
    return path


def import_upload(conn: sqlite3.Connection, payload: dict | list, group_id: str | None = None,
                  save: bool = True, data_dir: Path | None = None) -> dict:
    """Ручная загрузка JSON (ответ API, {records}, список записей или один диалог). group_id — параметр,
    иначе dataset_group_id из тела, иначе UPLOAD. Исходный JSON сохраняется в data/uploads/."""
    records = parse_payload(payload)
    gid = group_id or payload_group_id(payload) or "UPLOAD"
    if not GROUP_ID_RE.match(gid):
        raise DataSourceError(f"некорректный group_id {gid!r} (латиница, цифры, _ и -, до 64 знаков)")
    result = db.save_dataset(conn, gid, "upload", records)
    if save:
        result["saved_to"] = str(save_raw(gid, json.dumps(payload, ensure_ascii=False).encode("utf-8"), data_dir))
    return result


def import_url(conn: sqlite3.Connection, url: str, group_id: str | None = None, timeout_s: float | None = None,
               client: httpx.Client | None = None, save: bool = True, data_dir: Path | None = None) -> dict:
    """Загрузка набора по готовой GET-ссылке организаторов (адрес с group_id внутри). Ответ сохраняется байт в байт
    в data/uploads/ и в data/<group_id>.json, записи — в БД."""
    if not url.lower().startswith(("http://", "https://")):
        raise DataSourceError("ссылка должна начинаться с http:// или https://")
    own = client is None
    client = client or httpx.Client(timeout=timeout_s or get_settings().data_api_timeout_s)
    try:
        resp = client.get(url)
    except httpx.HTTPError as e:
        raise DataSourceError(f"API недоступно: {e}") from e
    finally:
        if own:
            client.close()
    if resp.status_code >= 400:
        raise DataSourceError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    try:
        payload = json.loads(resp.content.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as e:
        raise DataSourceError(f"ответ не JSON: {e}") from e
    records = parse_payload(payload)
    m = re.search(r"[?&]group_id=([A-Za-z0-9_-]+)", url)
    gid = group_id or payload_group_id(payload) or (m.group(1) if m else None) or "URL"
    if not GROUP_ID_RE.match(gid):
        raise DataSourceError(f"некорректный group_id {gid!r}")
    result = db.save_dataset(conn, gid, "remote", records)
    result["url"] = url
    if save:
        result["saved_to"] = str(save_raw(gid, resp.content, data_dir, source="url"))
    return result
