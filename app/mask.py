"""Avro OCF 脱敏：以同一已提交契约为准，把选中的可空字符串顶层字段置 null。

交付给外部分析前的处理流程（全程在内存中完成，任何失败都不产生部分结果）：

  1. 解析 OCF 文件头，要求 codec 为 null（缺省按 null 处理，其余一律拒绝）；
  2. 文件头 writer 模式与某版**已发布** writer 历史做规范化 JSON 匹配
     （对象键序与空白不影响匹配），未登记即拒绝；
  3. 用同一已提交版本的 reader 做 Schema Resolution 解码**全部**记录；
  4. 仅把选中字段置 null，其余字段的解码值、记录顺序与条数保持不变；
  5. 以 reader 模式写出新的 OCF（codec=null）。

调用方（main.py）负责：一次性读取契约快照、校验选中字段、限制上传大小。
"""
from __future__ import annotations

import io
import json
from typing import Any, Dict, List, Optional, Tuple

import avro.datafile
import avro.io
import avro.schema
from avro.schema import RecordSchema, UnionSchema

from . import compat

MAX_UPLOAD_BYTES = 2 * 1024 * 1024  # 2 MiB


class MaskError(Exception):
    """脱敏失败：status 为 HTTP 状态码，code 为机器可读错误码。"""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _canon(obj: Any) -> str:
    """规范化 JSON：对象键排序 + 紧凑分隔符 —— 键序与空白不影响比较结果。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sensitive_fields(reader: Any) -> List[str]:
    """当前 reader 中类型恰为 ["null","string"] 的顶层字段名（按解析后的模式判定）。

    reader 不是 record 时没有顶层字段，返回空列表。
    """
    try:
        schema = compat.parse_schema(reader if isinstance(reader, str) else json.dumps(reader))
    except Exception:
        return []
    if not isinstance(schema, RecordSchema):
        return []
    out: List[str] = []
    for f in schema.fields:
        t = f.type
        if (
            isinstance(t, UnionSchema)
            and len(t.schemas) == 2
            and {b.type for b in t.schemas} == {"null", "string"}
        ):
            out.append(f.name)
    return out


def match_writer_seq(file_schema_obj: Any, history: List[dict]) -> Optional[int]:
    """文件头 writer 与全部已发布历史做规范化匹配，返回命中的 seq（小者优先）。"""
    target = _canon(file_schema_obj)
    for h in history:
        if _canon(h["schema"]) == target:
            return h["seq"]
    return None


def mask_ocf(
    data: bytes, reader_json: str, history: List[dict], fields: List[str]
) -> Tuple[bytes, int, int]:
    """解码 → 选中字段置 null → 以 reader 模式写出。

    返回 (脱敏后的 OCF 字节, 记录数, 文件头 writer 命中的历史 seq)。
    任何步骤失败都抛 MaskError，调用方不得交付部分结果。
    """
    # ---- 1. 文件头：合法性 + codec 必须为 null ----
    try:
        reader_schema = compat.parse_schema(reader_json)
        ocf = avro.datafile.DataFileReader(
            io.BytesIO(data), avro.io.DatumReader(readers_schema=reader_schema)
        )
        codec = ocf.codec  # 缺省 avro.codec 按 "null" 处理
    except Exception as exc:
        raise MaskError(400, "invalid_ocf", f"文件头解析失败（非 Avro OCF 或文件已截断）: {exc}")
    if codec != "null":
        raise MaskError(400, "unsupported_codec", f"仅接受 codec 为 null 的文件，实际为 {codec!r}")

    # ---- 2. 文件头 writer 须匹配某版已发布模式（键序/空白不敏感） ----
    raw_schema = ocf.get_meta(avro.datafile.SCHEMA_KEY)
    try:
        file_schema_obj = json.loads(raw_schema.decode("utf-8"))
    except Exception as exc:
        raise MaskError(400, "invalid_ocf", f"文件头 writer 模式不是合法 JSON: {exc}")
    seq = match_writer_seq(file_schema_obj, history)
    if seq is None:
        raise MaskError(
            400, "writer_not_registered", "文件头 writer 模式未匹配任何已发布版本，拒绝交付"
        )

    # ---- 3. 用同一已提交版本的 reader 解码全文件，仅选中字段置 null ----
    records: List[Dict[str, Any]] = []
    try:
        for datum in ocf:
            for f in fields:
                datum[f] = None
            records.append(datum)
    except Exception as exc:
        raise MaskError(400, "invalid_ocf", f"解码失败（文件截断或内容损坏）: {exc}")

    # ---- 4. 以 reader 模式写出 OCF（codec=null），全部成功才返回 ----
    out = io.BytesIO()
    try:
        writer = avro.datafile.DataFileWriter(
            out, avro.io.DatumWriter(), writers_schema=reader_schema, codec="null"
        )
        for rec in records:
            writer.append(rec)
        writer.flush()
    except Exception as exc:
        raise MaskError(500, "write_failed", f"写出脱敏文件失败: {exc}")
    return out.getvalue(), len(records), seq
