# from __future__ import annotations
#
# import re
# from typing import Any, Dict, List, Mapping
#
# from ..engine import normalize_actions
#
# _JSON_TYPE = {
#     str: "string",
#     int: "integer",
#     float: "number",
#     bool: "boolean",
#     list: "array",
#     dict: "object",
#     type(None): "null",
# }
#
#
# def _split_pointer(p: str) -> List[str]:
#     return [t.replace("~1", "/").replace("~0", "~") for t in p.lstrip("/").split("/")]
#
#
# def _merge(dst: dict, patch: dict) -> None:
#     for key, val in patch.items():
#         if isinstance(dst.get(key), dict) and isinstance(val, dict):
#             _merge(dst[key], val)
#         else:
#             dst[key] = val
#
#
# def build_schema(spec: Any) -> dict:
#     """Build a JSON Schema approximation from a DSL script.
#
#     The algorithm scans all steps that write to paths, infers basic JSON types from
#     literal values and respects replace_root / foreach / $eval nesting.
#     """
#     root: Dict[str, Any] = {
#         "type": "object",
#         "properties": {},
#         "required": [],
#     }
#
#     steps = normalize_actions(spec)
#
#     def _wild(pointer: str) -> str:
#         """Replace any ${...} template segments in pointer with '*' to avoid collisions."""
#         return re.sub(r"\$\{[^}]+}", "*", pointer)
#
#     def _ensure(pointer: str, is_append: bool) -> Dict[str, Any]:
#         """Ensure that the schema tree contains the path described by pointer."""
#         cur = root
#         for tok in _split_pointer(pointer):
#             cur.setdefault("required", [])
#             if tok not in cur["required"]:
#                 cur["required"].append(tok)
#
#             cur = cur.setdefault("properties", {}).setdefault(
#                 tok,
#                 {"type": "object", "properties": {}},
#             )
#
#         if is_append:
#             cur["type"] = "array"
#
#         return cur
#
#     def _scan_value(val: Any) -> None:
#         if isinstance(val, Mapping):
#             if "$eval" in val:
#                 nested = normalize_actions(val["$eval"])
#                 _scan(nested)
#
#             for v in val.values():
#                 _scan_value(v)
#
#         elif isinstance(val, (list, tuple)):
#             for item in val:
#                 _scan_value(item)
#
#     def _scan(items: List[dict]) -> None:
#         for step in items:
#             op = step.get("op")
#
#             if op == "replace_root":
#                 val = step.get("value")
#                 if not (isinstance(val, Mapping) and "$ref" in val):
#                     root["type"] = _JSON_TYPE.get(type(val), "object")
#                 _scan_value(val)
#                 continue
#
#             if "do" in step and isinstance(step["do"], list):
#                 _scan(step["do"])
#
#             if "path" in step:
#                 path_raw = step["path"]
#                 path = _wild(path_raw)
#
#                 is_append = path.endswith("/-")
#                 leaf = _ensure(path[:-2] if is_append else path, is_append)
#
#                 if "value" in step and not isinstance(step["value"], Mapping):
#                     leaf["type"] = _JSON_TYPE.get(type(step["value"]), "object")
#
#                 _scan_value(step.get("value"))
#             else:
#                 _scan_value(step.get("value"))
#
#     _scan(steps)
#     return root
