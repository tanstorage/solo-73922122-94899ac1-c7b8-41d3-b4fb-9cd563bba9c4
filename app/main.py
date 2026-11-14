"""FastAPI 入口：契约发布网页 + REST API + Avro OCF 脱敏下载。

两个进程可共用同一个 SQLite 库文件（DB_PATH 指向共享卷），
兼容检查不持写锁，发布按提交时状态裁决（见 store.py）。
脱敏下载一次性读取契约一致快照后在内存中处理，全程不持 SQLite
写锁；处理期间另一进程发布新版本时，本次仍使用快照中的原版本。
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Any, Literal, Optional

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from . import compat, mask as mask_mod, store

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "data", "contracts.db"))
SERVER_ID = os.environ.get("SERVER_ID", "dev")
MAX_DEMO_DELAY_MS = 10_000
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

_NAME_PATTERN = r"^[A-Za-z0-9_.\-]{1,64}$"


class LinkCreate(BaseModel):
    name: str = Field(pattern=_NAME_PATTERN)
    writer: Any  # Avro 模式：JSON 对象或 JSON 字符串
    reader: Any


class PublishBody(BaseModel):
    schema_: Any = Field(alias="schema")
    demo_delay_ms: int = Field(default=0, ge=0, le=MAX_DEMO_DELAY_MS)

    model_config = {"populate_by_name": True}


class CheckBody(BaseModel):
    end: Literal["writer", "reader"]
    schema_: Any = Field(alias="schema")

    model_config = {"populate_by_name": True}


def _normalize_schema(raw: Any) -> str:
    """校验并规范化模式文本；非法 JSON / 非法 Avro 模式返回 400。"""
    try:
        text = compat.canonical(raw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"模式不是合法 JSON: {exc}")
    try:
        compat.parse_schema(text)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Avro 模式解析失败: {exc}")
    return text


def _reject(result: store.PublishResult, what: str) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "error": "incompatible",
            "stage": result.stage,  # pre-check: 预检拒绝；commit: 提交时裁决拒绝
            "message": what,
            "conflicts": result.conflicts,
            "state": result.state,
        },
    )


def create_app(st: Optional[store.Store] = None) -> FastAPI:
    app = FastAPI(title="Avro 契约注册中心", version="1.0.0")
    st = st or store.Store(DB_PATH)
    app.state.store = st

    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    @app.middleware("http")
    async def _server_header(request: Request, call_next):
        resp = await call_next(request)
        resp.headers["X-Server-Id"] = SERVER_ID
        return resp

    # --------------------------------------------------------------
    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(os.path.join(STATIC_DIR, "index.html"))

    @app.get("/api/info")
    def info():
        return {"server_id": SERVER_ID, "db_path": st.path, "links": len(st.list_links())}

    @app.get("/api/links")
    def list_links():
        return {"server_id": SERVER_ID, "links": st.list_links()}

    @app.post("/api/links", status_code=201)
    def create_link(body: LinkCreate):
        writer_json = _normalize_schema(body.writer)
        reader_json = _normalize_schema(body.reader)
        try:
            result = st.create_link(body.name, writer_json, reader_json, compat.check_compatibility)
        except store.LinkExists:
            raise HTTPException(status_code=409, detail={"error": "exists", "message": f"链路 {body.name!r} 已存在"})
        if not result.ok:
            raise _reject(result, "初建链路不兼容：当前 reader 无法读取初始 writer，链路未创建")
        return {"ok": True, "server_id": SERVER_ID, "state": result.state}

    @app.get("/api/links/{name}")
    def get_link(name: str):
        link = st.get_link(name)
        if link is None:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        return {"server_id": SERVER_ID, **link}

    @app.delete("/api/links/{name}")
    def delete_link(name: str):
        if not st.delete_link(name):
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        return {"ok": True}

    @app.put("/api/links/{name}/writer")
    def publish_writer(name: str, body: PublishBody):
        writer_json = _normalize_schema(body.schema_)
        try:
            result = st.publish_writer(name, writer_json, compat.check_compatibility, body.demo_delay_ms)
        except store.LinkNotFound:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        if not result.ok:
            raise _reject(result, "新 writer 无法被当前 reader 读取，已保留旧契约")
        return {"ok": True, "server_id": SERVER_ID, "state": result.state}

    @app.put("/api/links/{name}/reader")
    def publish_reader(name: str, body: PublishBody):
        reader_json = _normalize_schema(body.schema_)
        try:
            result = st.publish_reader(name, reader_json, compat.check_compatibility, body.demo_delay_ms)
        except store.LinkNotFound:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        if not result.ok:
            raise _reject(result, "新 reader 无法读取全部历史 writer，已保留旧契约")
        return {"ok": True, "server_id": SERVER_ID, "state": result.state}

    @app.post("/api/links/{name}/check")
    def dry_check(name: str, body: CheckBody):
        """试算：只检查不提交（不持写锁）。"""
        schema_json = _normalize_schema(body.schema_)
        link = st.get_link(name)
        if link is None:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        obj = compat.parse_schema(schema_json)
        if body.end == "writer":
            res = compat.check_compatibility(obj, compat.parse_schema(link["reader"]))
            conflicts = [{"writer_seq": None, **c} for c in res.to_list()]
        else:
            conflicts = []
            for h in link["history"]:
                res = compat.check_compatibility(compat.parse_schema(h["schema"]), obj)
                conflicts.extend({"writer_seq": h["seq"], **c} for c in res.to_list())
        return {"ok": not conflicts, "conflicts": conflicts, "server_id": SERVER_ID}

    # --------------------------------------------------------------
    # Avro OCF 脱敏下载
    # --------------------------------------------------------------
    @app.get("/api/links/{name}/mask/fields")
    def mask_fields(name: str):
        """当前 reader 中类型恰为 ["null","string"] 的顶层可脱敏字段。"""
        link = st.get_link(name)
        if link is None:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")
        return {
            "server_id": SERVER_ID,
            "version": link["version"],
            "fields": mask_mod.sensitive_fields(link["reader"]),
        }

    @app.post("/api/links/{name}/mask")
    async def mask_download(
        name: str,
        file: UploadFile = File(...),
        fields: str = Form(...),
        demo_delay_ms: int = Form(0),
    ):
        """脱敏并下载：上传 codec=null 的 OCF（≤2 MiB），把选中字段置 null 后
        以当前已提交 reader 模式写出 OCF 返回。任何失败都不交付部分结果。"""
        # 一次性读取一致快照（只读事务，不占写锁）；此后全程使用这一已提交版本，
        # 即使处理期间另一进程发布了新契约也不受影响。
        snap = st.get_link(name)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"链路 {name!r} 不存在")

        # 选中字段：JSON 字符串数组，且每个都必须是当前 reader 的可脱敏字段
        try:
            selected = json.loads(fields)
        except Exception:
            raise HTTPException(status_code=400, detail={"error": "bad_fields", "message": "fields 须为 JSON 字符串数组"})
        if not isinstance(selected, list) or not all(isinstance(f, str) for f in selected):
            raise HTTPException(status_code=400, detail={"error": "bad_fields", "message": "fields 须为 JSON 字符串数组"})
        selected = list(dict.fromkeys(selected))  # 去重保序
        if not selected:
            raise HTTPException(status_code=400, detail={"error": "no_fields", "message": "未选择任何脱敏字段"})
        eligible = mask_mod.sensitive_fields(snap["reader"])
        bad = [f for f in selected if f not in set(eligible)]
        if bad:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "field_not_maskable",
                    "message": f"字段 {bad} 不是当前 reader 中类型恰为 [\"null\",\"string\"] 的顶层字段",
                    "fields": eligible,
                },
            )

        if not 0 <= demo_delay_ms <= MAX_DEMO_DELAY_MS:
            raise HTTPException(status_code=400, detail={"error": "bad_delay", "message": f"demo_delay_ms 须在 0..{MAX_DEMO_DELAY_MS} 之间"})

        data = await file.read(mask_mod.MAX_UPLOAD_BYTES + 1)
        if len(data) > mask_mod.MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail={"error": "too_large", "message": "文件超过 2 MiB 上限"})

        if demo_delay_ms > 0:
            # 可控演示：放大“处理期间他进程发布契约”的窗口（不持任何 SQLite 锁）
            await asyncio.sleep(demo_delay_ms / 1000.0)

        try:
            out, count, seq = mask_mod.mask_ocf(
                data, compat.canonical(snap["reader"]), snap["history"], selected
            )
        except mask_mod.MaskError as exc:
            raise HTTPException(status_code=exc.status, detail={"error": exc.code, "message": exc.message})

        version = snap["version"]
        headers = {
            "X-Contract-Version": str(version),
            "X-Record-Count": str(count),
            "X-Writer-Seq": str(seq),
            "X-Masked-Fields": ",".join(selected),
            "Content-Disposition": f'attachment; filename="{name}-v{version}-masked.avro"',
        }
        return Response(content=out, media_type="application/avro", headers=headers)

    return app


app = create_app()
