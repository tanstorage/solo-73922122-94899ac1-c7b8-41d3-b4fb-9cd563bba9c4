"""Avro OCF 脱敏下载测试：字段识别、writer 匹配、往返一致性、
失败不交付部分结果、处理期间他进程发布仍用原版本且不占写锁。"""
import io
import json
import sys
import threading
import time
from pathlib import Path

import avro.datafile
import avro.io
import avro.schema
import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app import compat, mask as mask_mod
from app.main import create_app
from app.store import Store

W1 = {
    "type": "record", "name": "Batch", "namespace": "lake",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": ["null", "string"]},
        {"name": "amount", "type": "int"},
    ],
}
# 演进版 writer：amount int->long，新增 note
W2 = {
    "type": "record", "name": "Batch", "namespace": "lake",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": ["null", "string"]},
        {"name": "amount", "type": "long"},
        {"name": "note", "type": ["null", "string"], "default": None},
    ],
}
READER = {
    "type": "record", "name": "Batch", "namespace": "lake",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "customer", "type": ["null", "string"]},
        {"name": "email", "type": ["null", "string"], "default": None},
        {"name": "amount", "type": "long"},
        {"name": "plain", "type": "string", "default": ""},
        {"name": "opt", "type": ["null", "string", "int"], "default": None},
    ],
}
RECS_W1 = [
    {"id": 1, "customer": "alice", "amount": 10},
    {"id": 2, "customer": None, "amount": 20},
    {"id": 3, "customer": "carol", "amount": 30},
]


def j(obj):
    return json.dumps(obj, separators=(",", ":"))


def build_ocf(schema_obj, records, codec="null", header_schema_text=None):
    """构造 OCF 字节；header_schema_text 可覆盖写入文件头的 writer 模式文本。"""
    buf = io.BytesIO()
    w = avro.datafile.DataFileWriter(
        buf, avro.io.DatumWriter(), writers_schema=avro.schema.parse(j(schema_obj)), codec=codec
    )
    if header_schema_text is not None:
        w.set_meta(avro.datafile.SCHEMA_KEY, header_schema_text.encode("utf-8"))
    for r in records:
        w.append(r)
    w.flush()
    return buf.getvalue()


def read_ocf(data):
    return list(avro.datafile.DataFileReader(io.BytesIO(data), avro.io.DatumReader()))


def ocf_header(data):
    r = avro.datafile.DataFileReader(io.BytesIO(data), avro.io.DatumReader())
    return {"schema": json.loads(r.get_meta(avro.datafile.SCHEMA_KEY).decode("utf-8")), "codec": r.codec}


@pytest.fixture()
def st(tmp_path):
    return Store(str(tmp_path / "contracts.db"))


@pytest.fixture()
def client(st):
    return TestClient(create_app(st))


@pytest.fixture()
def link(client):
    """建链路：writer W1 -> 发布 W2（历史 [W1, W2]），reader 含多种字段形态。"""
    assert client.post("/api/links", json={"name": "lake", "writer": W1, "reader": READER}).status_code == 201
    assert client.put("/api/links/lake/writer", json={"schema": W2}).status_code == 200
    return client


def mask_req(client, data, fields, name="lake", delay=0):
    return client.post(
        f"/api/links/{name}/mask",
        files={"file": ("batch.avro", data, "application/avro")},
        data={"fields": json.dumps(fields), "demo_delay_ms": str(delay)},
    )


# ---------------- 可脱敏字段识别 ----------------

def test_sensitive_fields_only_exact_null_string_union():
    reader = {
        "type": "record", "name": "R",
        "fields": [
            {"name": "a", "type": ["null", "string"]},                    # 恰为 ["null","string"]
            {"name": "b", "type": ["string", "null"]},                    # 同一 union，分支顺序不同
            {"name": "c", "type": "string"},                              # 非 union
            {"name": "d", "type": ["null", "string", "int"]},             # 三分支
            {"name": "e", "type": ["null", "int"]},                       # 非 string
            {"name": "f", "type": {"type": "record", "name": "N", "fields": [
                {"name": "inner", "type": ["null", "string"]}]}},         # 嵌套不算顶层
        ],
    }
    assert mask_mod.sensitive_fields(reader) == ["a", "b"]
    assert mask_mod.sensitive_fields({"type": "string"}) == []            # 非 record 无顶层字段
    assert mask_mod.sensitive_fields('["null","string"]') == []


def test_mask_fields_endpoint(link):
    r = link.get("/api/links/lake/mask/fields")
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 2
    # READER 顶层中恰为 ["null","string"] 的只有 customer / email
    assert body["fields"] == ["customer", "email"]
    assert link.get("/api/links/nope/mask/fields").status_code == 404


# ---------------- 成功路径：往返一致 ----------------

def test_mask_roundtrip_uses_committed_reader(link):
    ocf = build_ocf(W1, RECS_W1)  # 用历史 #1 的 writer 写文件
    r = mask_req(link, ocf, ["customer"])
    assert r.status_code == 200, r.text
    assert r.headers["content-type"] == "application/avro"
    assert r.headers["x-contract-version"] == "2"      # 契约版本（已提交快照）
    assert r.headers["x-record-count"] == "3"
    assert r.headers["x-writer-seq"] == "1"            # 文件头 writer 命中历史 #1
    assert r.headers["x-masked-fields"] == "customer"
    assert "attachment" in r.headers["content-disposition"]

    # 输出是以 reader 模式写出的 OCF：codec=null，文件头即当前 reader
    hdr = ocf_header(r.content)
    assert hdr["codec"] == "null"
    assert hdr["schema"] == avro.schema.parse(j(READER)).to_json()

    out = read_ocf(r.content)
    assert len(out) == len(RECS_W1)                    # 条数一致
    # 仅 customer 置 null；其余字段为解码值（含 reader 默认值、int->long 提升）
    assert [rec["customer"] for rec in out] == [None, None, None]
    assert [rec["id"] for rec in out] == [1, 2, 3]     # 顺序一致
    assert [rec["amount"] for rec in out] == [10, 20, 30]
    assert all(rec["email"] is None for rec in out)    # reader 默认 null
    assert all(rec["plain"] == "" for rec in out)
    assert all(rec["opt"] is None for rec in out)


def test_mask_matches_any_published_writer_version(link):
    # 历史 #2（当前 writer）写出的文件同样可脱敏
    recs = [{"id": 7, "customer": "bob", "amount": 99, "note": "n1"}]
    r = mask_req(link, build_ocf(W2, recs), ["customer"])
    assert r.status_code == 200, r.text
    assert r.headers["x-writer-seq"] == "2"
    out = read_ocf(r.content)
    assert out[0]["customer"] is None and out[0]["amount"] == 99


def test_writer_match_ignores_key_order_and_whitespace(link):
    # 同一模式的另一种 JSON 序列化（键序打乱 + 多余空白）仍须匹配历史 #1
    shuffled = """
    {  "fields": [ {"type": "long", "name": "id"},
                   {"name": "customer", "type": ["null", "string"]},
                   {"name": "amount", "type": "int"} ],
       "namespace": "lake",   "name": "Batch", "type": "record" }
    """
    assert json.loads(shuffled) == W1  # 语义相同、文本不同
    r = mask_req(link, build_ocf(W1, RECS_W1, header_schema_text=shuffled), ["customer"])
    assert r.status_code == 200, r.text
    assert r.headers["x-writer-seq"] == "1"
    assert len(read_ocf(r.content)) == 3


def test_mask_multiple_fields_and_zero_records(link):
    ocf = build_ocf(W1, RECS_W1)
    r = mask_req(link, ocf, ["customer", "email"])
    assert r.status_code == 200 and r.headers["x-masked-fields"] == "customer,email"

    empty = build_ocf(W1, [])
    r = mask_req(link, empty, ["customer"])
    assert r.status_code == 200, r.text
    assert r.headers["x-record-count"] == "0"
    assert read_ocf(r.content) == []                   # 空文件也产出合法 OCF


# ---------------- 失败路径：不交付部分结果 ----------------

def test_unregistered_writer_rejected(link):
    stranger = {"type": "record", "name": "Batch", "namespace": "lake",
                "fields": [{"name": "id", "type": "long"}]}  # 从未发布过
    r = mask_req(link, build_ocf(stranger, [{"id": 1}]), ["customer"])
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "writer_not_registered"
    assert r.headers["content-type"].startswith("application/json")  # 无文件内容


def test_non_null_codec_rejected(link):
    ocf = build_ocf(W1, RECS_W1, codec="deflate")
    r = mask_req(link, ocf, ["customer"])
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "unsupported_codec"


def test_truncated_file_rejected(link):
    ocf = build_ocf(W1, RECS_W1)
    for cut in (len(ocf) // 2, len(ocf) - 3):
        r = mask_req(link, ocf[:cut], ["customer"])
        assert r.status_code == 400, cut
        assert r.json()["detail"]["error"] == "invalid_ocf"
        assert r.headers["content-type"].startswith("application/json")


def test_garbage_file_rejected(link):
    r = mask_req(link, b"this is not avro at all", ["customer"])
    assert r.status_code == 400
    assert r.json()["detail"]["error"] == "invalid_ocf"


def test_oversize_file_rejected(link):
    r = mask_req(link, b"\x00" * (mask_mod.MAX_UPLOAD_BYTES + 1), ["customer"])
    assert r.status_code == 413
    assert r.json()["detail"]["error"] == "too_large"


def test_bad_field_selection_rejected(link):
    ocf = build_ocf(W1, RECS_W1)
    # 空选择
    assert mask_req(link, ocf, []).status_code == 400
    # 非恰为 ["null","string"] 的字段
    r = mask_req(link, ocf, ["plain"])
    assert r.status_code == 400 and r.json()["detail"]["error"] == "field_not_maskable"
    # 不存在的字段
    assert mask_req(link, ocf, ["ghost"]).status_code == 400
    # fields 不是 JSON 数组
    r = link.post("/api/links/lake/mask",
                  files={"file": ("b.avro", ocf, "application/avro")},
                  data={"fields": "customer"})
    assert r.status_code == 400
    # 链路不存在
    assert mask_req(link, ocf, ["customer"], name="ghost").status_code == 404


# ---------------- 并发：处理期间他进程发布，本次仍用原版本且不占写锁 ----------------

def test_mask_uses_original_snapshot_during_concurrent_publish(st):
    """脱敏处理期间另一请求发布了新契约：本次响应仍报告原版本、按原 reader
    产出；发布不被阻塞（处理全程不持 SQLite 写锁）。"""
    app = create_app(st)
    c1, c2 = TestClient(app), TestClient(app)
    assert c1.post("/api/links", json={"name": "lake", "writer": W1, "reader": READER}).status_code == 201
    ocf = build_ocf(W1, RECS_W1)

    new_reader = dict(READER, fields=READER["fields"] + [
        {"name": "extra", "type": "int", "default": 0}])
    outcome = {}

    def do_mask():
        t0 = time.monotonic()
        resp = c1.post("/api/links/lake/mask",
                       files={"file": ("b.avro", ocf, "application/avro")},
                       data={"fields": '["customer"]', "demo_delay_ms": "1200"})
        outcome["mask"] = resp
        outcome["mask_end"] = time.monotonic() - t0

    t = threading.Thread(target=do_mask)
    t.start()
    time.sleep(0.3)  # 确保脱敏已取快照并进入处理期
    t0 = time.monotonic()
    pub = c2.put("/api/links/lake/reader", json={"schema": new_reader})
    outcome["pub_elapsed"] = time.monotonic() - t0
    t.join()

    # 发布成功且未被脱敏阻塞（远小于 1200ms 的处理延迟）
    assert pub.status_code == 200, pub.text
    assert outcome["pub_elapsed"] < 1.0
    assert outcome["mask"].status_code == 200, outcome["mask"].text
    # 本次仍使用原版本：契约版本为发布前的 v1，输出文件头是原 reader
    assert outcome["mask"].headers["x-contract-version"] == "1"
    assert ocf_header(outcome["mask"].content)["schema"] == avro.schema.parse(j(READER)).to_json()
    assert "extra" not in read_ocf(outcome["mask"].content)[0]
    # 库中契约已前进到 v2（发布确实生效）
    assert c1.get("/api/links/lake").json()["version"] == 2


def test_mask_does_not_block_or_break_publishing(client):
    """脱敏成功后原发布功能照常可用。"""
    assert client.post("/api/links", json={"name": "lake", "writer": W1, "reader": READER}).status_code == 201
    r = mask_req(client, build_ocf(W1, RECS_W1), ["customer"])
    assert r.status_code == 200
    r = client.put("/api/links/lake/writer", json={"schema": W2})
    assert r.status_code == 200 and r.json()["state"]["version"] == 2
