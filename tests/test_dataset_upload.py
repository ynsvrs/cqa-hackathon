"""Загрузка набора файлом JSON (в т. ч. одного диалога) и по готовой GET-ссылке; исходный JSON сохраняется."""
import json

import httpx
import pytest
from fastapi.testclient import TestClient

from backend import data_source, db
from backend.main import create_app


@pytest.fixture
def app_client(tmp_path):
    app = create_app(db_path=tmp_path / "t.db", data_dir=tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    with TestClient(app) as c:
        yield c, tmp_path


def test_upload_single_dialog_and_group_param(app_client, edge_records):
    client, tmp = app_client
    rec = edge_records["edge-ru-001"]
    r = client.post("/api/datasets/upload?group_id=SWAP", json=rec)                     # один диалог объектом
    assert r.status_code == 201 and r.json()["group_id"] == "SWAP" and r.json()["imported"] == 1
    assert r.json()["source"] == "upload" and r.json()["saved_to"].endswith(".json")
    assert (tmp / "data" / "SWAP.json").exists() and list((tmp / "data" / "uploads").glob("SWAP_upload_*.json"))
    assert client.get("/api/conversations?group_id=SWAP").json()["total"] == 1
    # имя набора из тела, если параметра нет; повторная загрузка не плодит набор
    r = client.post("/api/datasets/upload", json={"data": {"dataset_group_id": "SWAP", "records": [rec, edge_records["edge-ru-002"]]}})
    assert r.status_code == 201 and r.json()["group_id"] == "SWAP" and client.get("/api/conversations?group_id=SWAP").json()["total"] == 2
    r = client.post("/api/datasets/upload?group_id=bad id", json=rec)
    assert r.status_code == 502 and "group_id" in r.json()["error"]["message"]
    r = client.post("/api/datasets/upload", json={"foo": 1})
    assert r.status_code == 502


def test_import_by_url_saves_raw(tmp_path, edge_records, monkeypatch):
    conn = db.connect(":memory:")
    db.init_db(conn)
    body = json.dumps({"isSuccess": True, "data": {"dataset_group_id": "SWAP", "records": list(edge_records.values())[:2]}},
                      ensure_ascii=False).encode("utf-8")
    seen = {}

    def handler(req):
        seen["url"] = str(req.url)
        return httpx.Response(200, content=body)

    monkeypatch.setattr(data_source, "get_settings", lambda: type("S", (), {"data_dir": tmp_path, "data_api_timeout_s": 5})())
    res = data_source.import_url(conn, "https://api.example/datasets?group_id=SWAP&token=t",
                                 client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert seen["url"] == "https://api.example/datasets?group_id=SWAP&token=t"        # ссылка не меняется
    assert res["group_id"] == "SWAP" and res["imported"] == 2 and res["source"] == "remote"
    assert (tmp_path / "SWAP.json").read_bytes() == body
    assert list((tmp_path / "uploads").glob("SWAP_url_*.json"))[0].read_bytes() == body
    with pytest.raises(data_source.DataSourceError, match="HTTP 500"):
        data_source.import_url(conn, "https://api.example/x", client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(500))))
    with pytest.raises(data_source.DataSourceError, match="http"):
        data_source.import_url(conn, "ftp://nope")


def test_import_endpoint_accepts_url(app_client, edge_records, monkeypatch):
    client, tmp = app_client
    body = json.dumps({"data": {"dataset_group_id": "FBroker", "records": list(edge_records.values())[:3]}}, ensure_ascii=False).encode()
    real_client = httpx.Client
    monkeypatch.setattr(data_source.httpx, "Client",
                        lambda **kw: real_client(transport=httpx.MockTransport(lambda r: httpx.Response(200, content=body))))
    r = client.post("/api/datasets/import", json={"url": "https://api.example/datasets?group_id=FBroker"})
    assert r.status_code == 201 and r.json()["group_id"] == "FBroker" and r.json()["imported"] == 3
    assert client.get("/api/conversations?group_id=FBroker").json()["total"] == 3
    r = client.post("/api/datasets/import", json={"prefer": "local"})
    assert r.status_code == 400
