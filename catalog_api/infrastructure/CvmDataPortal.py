import requests
import re

from io import BytesIO
from typing import List, Optional
from zipfile import ZipFile


_KV_LINE = re.compile(r"^\s*(.+?)\s*:\s*(.*)$")


def _collect_attrs(lines: List[str], start_idx: int) -> tuple[dict[str, str], int]:
    """
    Collect key/value pairs until we hit the next 'Campo:' (or EOF).
    """
    attrs: dict[str, str] = {}
    idx = start_idx
    while idx < len(lines) and not lines[idx].startswith("Campo:"):
        m = _KV_LINE.match(lines[idx])
        if m:
            key, value = m.groups()
            attrs[key.strip().lower()] = value.strip()
        idx += 1
    return attrs, idx


def _split_size_prec(
    attrs: dict[str, str],
) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Extract (length, precision, scale) from the parsed attributes.
    """
    raw_len = attrs.get("tamanho")
    raw_prec = attrs.get("precisão")
    raw_scale = attrs.get("scale")

    length = int(raw_len) if raw_len and raw_len not in {"", "-1"} else None
    precision = int(raw_prec) if raw_prec else None
    scale = int(raw_scale) if raw_scale else None

    # Some integer types list their size under 'Precisão'
    if attrs.get("tipo dados", "").lower() in {"int", "tinyint", "bigint", "smallint"}:
        if precision and not length:
            length = precision
            precision = None
            scale = None

    return length, precision, scale


def parse_catalog(text: str) -> List[CatalogColumnHelper]:
    """
    Parse the entire “Campo / Descrição / Domínio …” document.
    """
    columns: list[CatalogColumnHelper] = []
    lines = [ln.rstrip() for ln in text.splitlines()]

    i = 0
    while i < len(lines):
        # ── look for the start of a new column ───────────────────────────────
        if not lines[i].startswith("Campo:"):
            i += 1
            continue

        col_name = lines[i].split(":", 1)[1].strip()
        i += 1

        attrs, i = _collect_attrs(lines, i)

        base_type = attrs.get("tipo dados", "").lower()
        length, precision, scale = _split_size_prec(attrs)
        nullable = not attrs.get("descrição") or not attrs.get("domínio")

        columns.append(
            CatalogColumnHelper(
                name=col_name.lower(),
                type=base_type,
                length=length,
                precision=precision,
                scale=scale,
                nullable=nullable,
                description=attrs.get("descrição") or None,
            )
        )

    return columns


def parse_cvm_zip_schema(url: str) -> SchemaHelper:
    helper = SchemaHelper()
    zip_request = requests.get(url)
    zip_request.raise_for_status()
    zip_file = BytesIO(zip_request.content)
    with ZipFile(zip_file) as zf:
        for name in zf.namelist():
            table_name = name[5:-4].lower()
            with zf.open(name) as f:
                entry_text = f.read().decode("latin1")
                helpers = parse_catalog(entry_text)
                table = CatalogTableHelper(name=table_name, columns=helpers)
                helper.tables.append(table)
    return helper


def parse_cvm_txt_schema(url: str) -> SchemaHelper:
    """
    Parse the CVM schema from a text file.
    """
    helper = SchemaHelper()
    table_name = url.split("/")[-1]
    table_name = table_name.split(".")[0].replace("meta_", "")
    txt_request = requests.get(url)
    txt_request.raise_for_status()
    txt_file = txt_request.content.decode("latin1")
    columns = parse_catalog(txt_file)
    table = CatalogTableHelper(name=table_name, columns=columns)
    helper.tables.append(table)
    return helper


def parse_cvm_schema(url: str) -> SchemaHelper:
    """
    Parse the CVM schema from a zip file.
    """
    if url.endswith(".zip"):
        return parse_cvm_zip_schema(url)
    elif url.endswith(".txt"):
        return parse_cvm_txt_schema(url)
    else:
        raise ValueError("Invalid file format. Only .zip and .txt files are supported.")
