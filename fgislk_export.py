#!/usr/bin/env python3
"""Экспорт лесоустроительных данных из Публичной лесной карты ФГИС ЛК.

Интерактивно выбирает папку загрузки, регион и центральные лесничества, затем
выгружает центральные и участковые лесничества, кварталы, выделы, подробные
атрибуты выделов и шесть слоёв категорий защитности/ОЗУ. Для выбранных
лесничеств сохраняет точные границы, индивидуальные bbox и общий bbox в EPSG:4326.
Защитные сведения добавляются к выделам пространственным пересечением.
Дополнительно создаётся таблица всех пересечений с площадью и долей выдела.

Папка загрузки: --out или окно выбора папки (запуск без параметров).
Докачка: каждая страница ответа сразу сохраняется в
    <папка>/_cache_fgislk_<регион>_<выбор>/
При обрыве связи скрипт ждёт и повторяет тот же запрос; если окно закрыто или
загрузка прервана, повторный запуск с той же папкой продолжает с последней
сохранённой страницы.
Результат: <папка>/fgislk_<регион>_<выбор>.gpkg и журнал *_log.txt.
Промежуточные данные после записи GPKG сохраняются (удаление — флаг --clean-cache);
повторный запуск с той же папкой пересобирает GPKG из них без новых запросов.
Все площадные слои пишутся как MultiPolygon (совместимо с ArcGIS Pro).
GeoJSON и CSV по слоям — только по флагам --geojson / --csv.

Установка:
    python -m pip install requests geopandas pyogrio shapely

Примеры:
    python fgislk_export.py
    python fgislk_export.py --list-regions
    python fgislk_export.py --list-forestries 26
    python fgislk_export.py --region 26 --forestry 26:4 --out D:\\FGISLK\\essentuki
    python fgislk_export.py --out D:\\FGISLK\\essentuki      (продолжить незавершённую)
    python fgislk_export.py --repair D:\\FGISLK\\fgislk_50_all.gpkg   (исправить готовый GPKG для ArcGIS Pro)
    %run fgislk_export.py                                   (Jupyter)
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import os
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

VERSION = "2.1"
MAP_API = "https://pub.fgislk.gov.ru/map/geo/map_api"
GEO_API = MAP_API + "/geo"
ATTR_API = MAP_API + "/layer/attributesinfo"

NET_WAIT_FIRST = 5          # первая пауза при обрыве связи, с
NET_WAIT_MAX = 120          # максимальная пауза между повторами, с
SERVER_RETRIES = 10         # повторы страницы из 1 записи при ошибке сервера
SETTINGS = Path.home() / ".fgislk_export.json"

LAYERS = {
    "central_forestries": "FOREST_LAYERS:FORESTRY",
    "district_forestries": "FOREST_LAYERS:DISTRICT_FORESTRY",
    "quarters": "FOREST_LAYERS:QUARTER",
    "taxation_pieces": "FOREST_LAYERS:TAXATION_PIECE",
}

PROTECTION_LAYERS = {
    "protective_forest": {
        "layer": "FOREST_LAYERS:PROTECTIVE_FOREST",
        "group": "category", "source": "public",
        "title": "Категории защитных лесов",
    },
    "special_protect_stead": {
        "layer": "FOREST_LAYERS:SPECIAL_PROTECT_STEAD",
        "group": "ozu", "source": "public",
        "title": "Особо защитные участки лесов",
    },
    "protective_forest_subcategory": {
        "layer": "FOREST_LAYERS:PROTECTIVE_FOREST_SUBCATEGORY",
        "group": "subcategory", "source": "public",
        "title": "Ценные леса и леса, выполняющие функции защиты объектов",
    },
    "kzl_management": {
        "layer": "FOREST_LAYERS:KZL_MANAGEMENT",
        "group": "category", "source": "management",
        "title": "Категории защитных лесов — лесоустройство",
    },
    "ozul_management": {
        "layer": "FOREST_LAYERS:OZUL_MANAGEMENT",
        "group": "ozu", "source": "management",
        "title": "Особо защитные участки — лесоустройство",
    },
    "protective_forest_subcategory_management": {
        "layer": "FOREST_LAYERS:PROTECTIVE_FOREST_SUBCATEGORY_MANAGEMENT",
        "group": "subcategory", "source": "management",
        "title": "Подкатегории защитных лесов — лесоустройство",
    },
}

REGIONS = {
 "01":"Республика Адыгея","02":"Республика Башкортостан","03":"Республика Бурятия","04":"Республика Алтай",
 "05":"Республика Дагестан","06":"Республика Ингушетия","07":"Кабардино-Балкарская Республика",
 "08":"Республика Калмыкия","09":"Карачаево-Черкесская Республика","10":"Республика Карелия","11":"Республика Коми",
 "12":"Республика Марий Эл","13":"Республика Мордовия","14":"Республика Саха (Якутия)",
 "15":"Республика Северная Осетия — Алания","16":"Республика Татарстан","17":"Республика Тыва",
 "18":"Удмуртская Республика","19":"Республика Хакасия","20":"Чеченская Республика","21":"Чувашская Республика",
 "22":"Алтайский край","23":"Краснодарский край","24":"Красноярский край","25":"Приморский край",
 "26":"Ставропольский край","27":"Хабаровский край","28":"Амурская область","29":"Архангельская область",
 "30":"Астраханская область","31":"Белгородская область","32":"Брянская область","33":"Владимирская область",
 "34":"Волгоградская область","35":"Вологодская область","36":"Воронежская область","37":"Ивановская область",
 "38":"Иркутская область","39":"Калининградская область","40":"Калужская область","41":"Камчатский край",
 "42":"Кемеровская область — Кузбасс","43":"Кировская область","44":"Костромская область","45":"Курганская область",
 "46":"Курская область","47":"Ленинградская область","48":"Липецкая область","49":"Магаданская область",
 "50":"Московская область","51":"Мурманская область","52":"Нижегородская область","53":"Новгородская область",
 "54":"Новосибирская область","55":"Омская область","56":"Оренбургская область","57":"Орловская область",
 "58":"Пензенская область","59":"Пермский край","60":"Псковская область","61":"Ростовская область",
 "62":"Рязанская область","63":"Самарская область","64":"Саратовская область","65":"Сахалинская область",
 "66":"Свердловская область","67":"Смоленская область","68":"Тамбовская область","69":"Тверская область",
 "70":"Томская область","71":"Тульская область","72":"Тюменская область","73":"Ульяновская область",
 "74":"Челябинская область","75":"Забайкальский край","76":"Ярославская область","77":"Москва",
 "78":"Санкт-Петербург","79":"Еврейская автономная область","82":"Республика Крым",
 "83":"Ненецкий автономный округ","86":"Ханты-Мансийский автономный округ — Югра",
 "87":"Чукотский автономный округ","89":"Ямало-Ненецкий автономный округ","90":"Запорожская область",
 "91":"Республика Крым","92":"Севастополь","93":"Краснодарский край","95":"Чеченская Республика",
}
_thread_local = threading.local()
_STOP = threading.Event()          # установлен — фоновые запросы прекращают ожидание связи
_net_lock = threading.Lock()
_net_last_msg = [0.0]
_net_since = [0.0]           # начало обрыва, о котором выведено сообщение (0 — связь есть)


def natural_key(value: str) -> list[Any]:
    return [int(x) if x.isdigit() else x.lower() for x in re.split(r"(\d+)", value or "")]


def region_code(value: str) -> str:
    first = (value or "").split(":", 1)[0]
    return first.zfill(2) if first.isdigit() else first


def fmt_time(sec: float) -> str:
    sec = int(sec)
    h, m, s = sec // 3600, sec % 3600 // 60, sec % 60
    return f"{h} ч {m:02d} мин" if h else (f"{m} мин {s:02d} с" if m else f"{s} с")


def short(exc: BaseException) -> str:
    text = str(exc).replace("\n", " ")
    return (type(exc).__name__ + ": " + text)[:200]


# ───────────────────────── сеть ─────────────────────────

def make_session(verify_ssl: bool) -> requests.Session:
    s = requests.Session(); s.verify = verify_ssl
    retry = Retry(total=6, connect=2, read=6, backoff_factor=1.0,
                  status_forcelist=(429,500,502,503,504), allowed_methods=frozenset(["GET"]))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter); s.mount("http://", adapter)
    if not verify_ssl:
        requests.packages.urllib3.disable_warnings(  # type: ignore[attr-defined]
            requests.packages.urllib3.exceptions.InsecureRequestWarning)  # type: ignore[attr-defined]
    return s


def worker_session(verify_ssl: bool) -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session(verify_ssl)
    return _thread_local.session


def encoded_cql(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


def is_network_error(exc: BaseException) -> bool:
    """Обрыв связи (повторять тот же запрос), а не отказ сервера (уменьшать страницу)."""
    if isinstance(exc, (requests.exceptions.ConnectTimeout, requests.exceptions.ChunkedEncodingError)):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "Read timed out" not in str(exc)
    return False


def _net_message(text: str, lost: float) -> None:
    with _net_lock:
        now = time.time()
        if now - _net_last_msg[0] >= 30:
            _net_last_msg[0] = now
            if not _net_since[0] or lost < _net_since[0]:
                _net_since[0] = lost
            print(text, flush=True)


def _net_restored() -> None:
    with _net_lock:
        if _net_since[0]:
            print(f"  связь восстановлена (перерыв {fmt_time(time.time()-_net_since[0])})", flush=True)
            _net_since[0] = 0.0; _net_last_msg[0] = 0.0


def fetch_json(session: requests.Session, url: str, params: dict[str,str], timeout: int) -> Any:
    """GET с ожиданием восстановления связи: при обрыве повторяет тот же запрос без ограничения
    числа попыток (пауза 5 → 120 с). Ошибки сервера передаются вызывающей функции."""
    wait = NET_WAIT_FIRST; lost: float | None = None
    while True:
        if _STOP.is_set():
            raise KeyboardInterrupt
        try:
            r = session.get(url, params=params, timeout=timeout); r.raise_for_status()
            data = r.json()
            if lost is not None:
                _net_restored()
            return data
        except requests.RequestException as exc:
            if not is_network_error(exc):
                raise
            if lost is None:
                lost = time.time()
            _net_message(f"  нет связи с сервером ({short(exc)[:90]}); повтор через {wait} с, "
                         f"ожидание {fmt_time(time.time()-lost)}. Скачанное сохранено: окно можно "
                         f"закрыть и позже запустить скрипт с той же папкой.", lost)
            if _STOP.wait(wait):
                raise KeyboardInterrupt
            wait = min(NET_WAIT_MAX, wait * 2)


def normalize_collection(data: Any) -> dict[str,Any]:
    if isinstance(data, list) and (not data or "Данные не обнаружены" in str(data[0])):
        return {"type":"FeatureCollection", "features":[], "numberMatched":0}
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise RuntimeError(f"Неожиданный ответ API: {str(data)[:500]}")
    return data


def fetch_pages(session: requests.Session, layer: str, timeout: int, page_size: int,
                geometry: bool=True, bbox: tuple[float,float,float,float]|None=None,
                cql: str|None=None, quiet: bool=False, ckpt: Path|None=None) -> list[dict[str,Any]]:
    """Постраничная загрузка WFS. Если задан ckpt, каждая полученная страница дописывается
    в этот файл (JSON Lines), и при повторном вызове загрузка продолжается с неё."""
    result: dict[str,dict[str,Any]] = {}; start = 0; previous: set[str] = set()
    current = max(1, page_size); total: int|None = None; fails = 0
    if ckpt is not None and ckpt.exists():
        with ckpt.open(encoding="utf-8") as fh:
            for line in fh:
                try: rec = json.loads(line)
                except ValueError: continue            # недописанная строка при обрыве
                for f in rec.get("features", []):
                    result[str(f.get("properties", {}).get("wfs_id"))] = f
                start = int(rec["next"]); current = int(rec.get("size", current)); total = rec.get("total")
        if result and not quiet:
            print(f"  продолжаю с записи {start} (сохранено {len(result)})")
    stream = ckpt.open("a", encoding="utf-8") if ckpt is not None else None
    try:
        while True:
            if total is not None and start >= total:
                break
            p = {"service":"WFS","version":"2.0.0","request":"GetFeature","typeNames":layer,
                 "outputFormat":"application/json","srsName":"EPSG:4326","count":str(current),
                 "startIndex":str(start),"selectControl":"true","sortBy":"id"}
            if not geometry: p["propertyName"] = "id,label_name,externalid"
            if bbox: p["bbox"] = ",".join(f"{x:.8f}" for x in bbox) + ",EPSG:4326"
            if cql: p["CQL_FILTER"] = encoded_cql(cql)
            try:
                data = normalize_collection(fetch_json(session, GEO_API, p, timeout))
            except (requests.RequestException, ValueError) as exc:
                if current > 1:
                    current = max(1, current // 2)
                    if not quiet: print(f"  сервер отклонил страницу; размер уменьшен до {current}")
                    continue
                fails += 1
                if fails > SERVER_RETRIES:
                    raise
                print(f"  ошибка сервера ({short(exc)[:120]}); повтор {fails}/{SERVER_RETRIES} через 30 с")
                if _STOP.wait(30): raise KeyboardInterrupt
                continue
            fails = 0
            fs = data.get("features", [])
            if not fs:
                break
            ids = set()
            for f in fs:
                fid = str(f.get("id") or f.get("properties", {}).get("id")); ids.add(fid)
                f.setdefault("properties", {})["wfs_id"] = fid; result[fid] = f
            if ids == previous:
                raise RuntimeError("API повторил страницу")
            previous = ids; start += len(fs)
            matched = data.get("numberMatched", data.get("totalFeatures"))
            if isinstance(matched, int): total = matched
            if stream is not None:
                stream.write(json.dumps({"next":start,"size":current,"total":total,"features":fs},
                                        ensure_ascii=False) + "\n")
                stream.flush()
            if not quiet:
                print(f"  получено {len(result)}" + (f" из {matched}" if isinstance(matched, int) else ""))
    finally:
        if stream is not None: stream.close()
    return list(result.values())


def fetch_exact(s:requests.Session, layer:str, eid:str, timeout:int) -> list[dict[str,Any]]:
    return fetch_pages(s,layer,timeout,10,cql=f"externalid = '{eid}'",quiet=True)


def ext(f:dict[str,Any])->str: return str(f.get("properties",{}).get("externalid", ""))
def label(f:dict[str,Any])->str: return str(f.get("properties",{}).get("label_name", ""))
def sort_features(fs:Iterable[dict[str,Any]])->list[dict[str,Any]]:
    return sorted(fs,key=lambda f:(natural_key(ext(f)),natural_key(label(f))))


def iter_points(value:Any)->Iterable[tuple[float,float]]:
    if isinstance(value,list) and len(value)>=2 and all(isinstance(v,(int,float)) for v in value[:2]):
        yield float(value[0]),float(value[1])
    elif isinstance(value,list):
        for item in value: yield from iter_points(item)


def feature_bbox(f:dict[str,Any])->tuple[float,float,float,float]:
    geom=f.get("geometry"); pts=list(iter_points(geom.get("coordinates"))) if geom else []
    if pts:
        xs,ys=zip(*pts); return min(xs),min(ys),max(xs),max(ys)
    b=f.get("bbox")
    if b and len(b)>=4: return tuple(map(float,b[:4]))  # type: ignore[return-value]
    raise ValueError(f"Нет geometry/bbox: {f.get('id')}")


def padded(b:tuple[float,float,float,float],pad:float=.00002)->tuple[float,float,float,float]:
    return b[0]-pad,b[1]-pad,b[2]+pad,b[3]+pad


def union_bbox(features:Iterable[dict[str,Any]])->tuple[float,float,float,float]:
    boxes=[feature_bbox(f) for f in features]
    if not boxes:
        raise ValueError("Нельзя вычислить bbox: список геометрий пуст")
    return min(b[0] for b in boxes),min(b[1] for b in boxes),max(b[2] for b in boxes),max(b[3] for b in boxes)


def bbox_feature(bbox:tuple[float,float,float,float], properties:dict[str,Any], fid:str)->dict[str,Any]:
    """Создаёт GeoJSON-полигон bbox в порядке west,south,east,north."""
    minx,miny,maxx,maxy=bbox
    ring=[[minx,miny],[maxx,miny],[maxx,maxy],[minx,maxy],[minx,miny]]
    props=dict(properties)
    props.update({"min_lon":minx,"min_lat":miny,"max_lon":maxx,"max_lat":maxy,
                  "bbox_wsen":f"{minx:.8f},{miny:.8f},{maxx:.8f},{maxy:.8f}",
                  "bbox_crs":"EPSG:4326"})
    return {"type":"Feature","id":fid,"properties":props,
            "geometry":{"type":"Polygon","coordinates":[ring]},"bbox":[minx,miny,maxx,maxy]}


def make_forestry_bboxes(boundaries:list[dict[str,Any]])->tuple[list[dict[str,Any]],list[dict[str,Any]]]:
    """Возвращает bbox каждого лесничества и общий bbox выбранной территории."""
    grouped:dict[str,list[dict[str,Any]]]={}
    for f in boundaries:
        grouped.setdefault(ext(f),[]).append(f)
    individual=[]
    for forestry_id,features in sorted(grouped.items(),key=lambda x:natural_key(x[0])):
        b=union_bbox(features)
        title=next((label(f) for f in features if label(f)),"")
        individual.append(bbox_feature(
            b,{"forestry_id":forestry_id,"forestry_name":title,"bbox_scope":"individual"},
            f"bbox.{forestry_id}"))
    combined=[]
    if boundaries:
        b=union_bbox(boundaries)
        combined=[bbox_feature(
            b,{"forestry_ids":"; ".join(sorted(grouped,key=natural_key)),
               "forestry_count":len(grouped),"bbox_scope":"combined"},
            "bbox.selected_area")]
    return individual,combined


def fetch_region_central(s:requests.Session, region:str, timeout:int)->list[dict[str,Any]]:
    ids=[f"{int(region)}:{i}" for i in range(1,301)]; result={}
    for pos in range(0,len(ids),150):
        cql="externalid IN ("+",".join(repr(x) for x in ids[pos:pos+150])+")"
        for f in fetch_pages(s,LAYERS["central_forestries"],timeout,500,geometry=False,cql=cql,quiet=True):
            result[str(f.get("id"))]=f
    # Для меню объединяем дубликаты по учётному номеру.
    unique={}
    for f in sort_features(result.values()): unique.setdefault(ext(f),f)
    return list(unique.values())


def fetch_district_catalog(s:requests.Session, central_ids:list[str], timeout:int)->list[dict[str,Any]]:
    ids=[f"{cid}:{j}" for cid in central_ids for j in range(1,201)]; result={}
    for pos in range(0,len(ids),150):
        cql="externalid IN ("+",".join(repr(x) for x in ids[pos:pos+150])+")"
        for f in fetch_pages(s,LAYERS["district_forestries"],timeout,500,geometry=False,cql=cql,quiet=True):
            result[str(f.get("id"))]=f
    unique={}
    for f in sort_features(result.values()): unique.setdefault(ext(f),f)
    return list(unique.values())


# ───────────────────────── меню и папка ─────────────────────────

def print_regions()->None:
    for i,c in enumerate(sorted(REGIONS,key=natural_key),1): print(f"{i:3}. {c} — {REGIONS[c]}")


def choose(prompt:str,n:int,all_option:bool=False)->list[int]:
    while True:
        raw=input(f"{prompt} [1–{n}"+(", 0 — все]: " if all_option else "]: ")).strip()
        if all_option and raw=="0": return list(range(n))
        try:
            values=sorted(set(int(x.strip())-1 for x in raw.split(",")))
            if values and all(0<=x<n for x in values): return values
        except ValueError: pass
        print("Введите номер или номера через запятую"+("; 0 — все" if all_option else "")+".")


def load_settings() -> dict[str,Any]:
    try: return json.loads(SETTINGS.read_text(encoding="utf-8"))
    except (OSError, ValueError): return {}


def save_settings(**kw: Any) -> None:
    data = load_settings(); data.update(kw)
    try: SETTINGS.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except OSError: pass


def ask_folder() -> Path:
    """Окно выбора папки загрузки (Проводник); без графики — ввод пути в консоли."""
    initial = load_settings().get("last_out") or str(Path.cwd())
    if not Path(initial).exists(): initial = str(Path.cwd())
    path = None; gui = True
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk(); root.withdraw()
        try: root.attributes("-topmost", True)
        except Exception: pass
        path = filedialog.askdirectory(title="Папка для загрузки ФГИС ЛК (выберите или создайте)",
                                       initialdir=initial, mustexist=False)
        root.destroy()
    except Exception:
        gui = False
    if not gui:
        raw = input(f"Папка для загрузки [Enter — {initial}]: ").strip().strip('"')
        path = raw or initial
    if not path:
        raise RuntimeError("Папка для загрузки не выбрана")
    out = Path(path).expanduser()
    print(f"Папка загрузки: {out}")
    return out


def selection_tag(code: str, ids: list[str], all_flag: bool) -> str:
    if all_flag:
        return f"{code}_all"
    parts = [i.split(":", 1)[1] if ":" in i else i for i in ids]
    if len(parts) <= 4:
        return f"{code}_" + "-".join(re.sub(r"[^0-9A-Za-z]", "-", p) for p in parts)
    h = hashlib.md5(",".join(sorted(ids)).encode()).hexdigest()[:6]
    return f"{code}_{len(ids)}lesn_{h}"


def cache_dir(out: Path, tag: str) -> Path:
    return out / f"_cache_fgislk_{tag}"


def read_run(path: Path) -> dict[str,Any] | None:
    try: return json.loads((path / "run.json").read_text(encoding="utf-8"))
    except (OSError, ValueError): return None


def unfinished_runs(out: Path) -> list[tuple[Path,dict[str,Any]]]:
    runs = []
    if out.is_dir():
        for d in sorted(out.glob("_cache_fgislk_*")):
            run = read_run(d)
            if run and run.get("status") == "running":
                runs.append((d, run))
    return runs


def describe_run(d: Path, run: dict[str,Any]) -> str:
    sel = ", ".join(f"{x['externalid']} {x.get('label_name','')}".strip() for x in run.get("selected", [])[:4])
    if len(run.get("selected", [])) > 4: sel += f" и ещё {len(run['selected'])-4}"
    nq = len(list((d/"pieces").glob("*.json"))) if (d/"pieces").is_dir() else 0
    return (f"{run.get('region')} — {REGIONS.get(run.get('region',''), '')}: {sel}; "
            f"начата {run.get('started','?')}, кварталов с выделами сохранено {nq}")


def pick_unfinished(out: Path) -> tuple[Path,dict[str,Any]] | None:
    runs = unfinished_runs(out)
    if not runs:
        return None
    print("\nВ папке есть незавершённая загрузка:")
    for i, (d, run) in enumerate(runs, 1):
        print(f"  {i}. {describe_run(d, run)}")
    if len(runs) == 1:
        raw = input("Продолжить её? [Д/н]: ").strip().lower()
        return runs[0] if raw in ("", "д", "да", "y", "yes", "l") else None
    while True:
        raw = input(f"Номер загрузки для продолжения [1–{len(runs)}, 0 — новая загрузка]: ").strip()
        if raw == "0": return None
        if raw.isdigit() and 1 <= int(raw) <= len(runs): return runs[int(raw)-1]


# ───────────────────────── сохранение промежуточных данных ─────────────────────────

def atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


class Store:
    """Промежуточные результаты: один JSON на завершённый запрос (лесничество, квартал, слой)
    и *.part.jsonl для незавершённого постраничного запроса."""

    def __init__(self, root: Path):
        self.root = root; root.mkdir(parents=True, exist_ok=True)
        self.hits = 0; self.fetched = 0

    def path(self, kind: str, key: str) -> Path:
        return self.root / kind / (re.sub(r"[^0-9A-Za-z_.-]", "-", key) + ".json")

    def has(self, kind: str, key: str) -> bool:
        return self.path(kind, key).exists()

    def get(self, kind: str, key: str, producer: Callable[[Path], Any]) -> Any:
        p = self.path(kind, key)
        if p.exists():
            try:
                data = json.loads(p.read_text(encoding="utf-8")); self.hits += 1; return data
            except ValueError:
                p.unlink()
        p.parent.mkdir(parents=True, exist_ok=True)
        part = p.with_name(p.stem + ".part.jsonl")
        data = producer(part)
        atomic_write_text(p, json.dumps(data, ensure_ascii=False))
        if part.exists(): part.unlink()
        self.fetched += 1
        return data


# ───────────────────────── атрибуты выделов ─────────────────────────

def object_id(f:dict[str,Any])->str:
    return str(f.get("id") or f.get("properties",{}).get("wfs_id","")).rsplit(".",1)[-1]


def load_cache(path:Path)->dict[str,dict[str,Any]]:
    """Успешно полученные атрибуты. Записи с ошибкой (пустые, без признака ok) запрашиваются снова."""
    out={}
    if path.exists():
        with path.open(encoding="utf-8") as fh:
            for line in fh:
                try:
                    row=json.loads(line); oid=str(row["wfs_object_id"]); payload=row["payload"]
                except (ValueError,KeyError,TypeError): continue
                if row.get("ok") or payload: out[oid]=payload if isinstance(payload,dict) else {}
    return out


def fetch_attr(f:dict[str,Any],timeout:int,verify:bool)->tuple[str,dict[str,Any]]:
    oid=object_id(f); data=fetch_json(worker_session(verify),ATTR_API,
        {"layer_code":"TAXATION_PIECE","object_id":oid},timeout)
    payload=data.get("payload",{}) if isinstance(data,dict) else {}
    return oid,payload if isinstance(payload,dict) else {}


def enrich_attributes(fs:list[dict[str,Any]],cache_path:Path,timeout:int,verify:bool,workers:int)->int:
    """Подробные атрибуты с докачкой. Возвращает число выделов, для которых атрибуты не получены."""
    cache=load_cache(cache_path); pending=[f for f in fs if object_id(f) not in cache]
    print(f"Подробные атрибуты: сохранено ранее {len(fs)-len(pending)}, запросить {len(pending)}")
    failed: list[dict[str,Any]] = []
    for attempt in (1, 2):
        if not pending: break
        if attempt == 2:
            print(f"  повторный запрос для {len(pending)} выделов с ошибкой")
        failed = []; t0 = time.time()
        pool = ThreadPoolExecutor(max_workers=workers)
        try:
            with cache_path.open("a",encoding="utf-8") as stream:
                futures={pool.submit(fetch_attr,f,timeout,verify):f for f in pending}
                for i,future in enumerate(as_completed(futures),1):
                    f=futures[future]
                    try: oid,payload=future.result()
                    except KeyboardInterrupt: raise
                    except Exception as exc:
                        failed.append(f); print(f"  атрибуты {object_id(f)}: {short(exc)}",file=sys.stderr)
                        continue
                    cache[oid]=payload
                    stream.write(json.dumps({"wfs_object_id":oid,"ok":True,"payload":payload},ensure_ascii=False)+"\n")
                    stream.flush()
                    if i%100==0 or i==len(pending):
                        el=time.time()-t0; eta=el/i*(len(pending)-i)
                        print(f"  атрибуты: {i}/{len(pending)}, прошло {fmt_time(el)}, осталось около {fmt_time(eta)}")
        except BaseException:
            _STOP.set(); pool.shutdown(wait=False, cancel_futures=True); raise
        pool.shutdown(wait=True)
        pending = failed
    for f in fs:
        props=f.setdefault("properties",{}); payload=cache.get(object_id(f),{})
        props["attributes_loaded"]=object_id(f) in cache
        for k,v in payload.items(): props[k if k not in props else "attr_"+k]=v
    if failed:
        print(f"  атрибуты не получены для {len(failed)} выделов; повторный запуск с той же папкой дозапросит их")
    return len(failed)


# ───────────────────────── защитность ─────────────────────────

def protection_value(props:dict[str,Any])->str:
    for key in ("type","label_name","name"):
        value=props.get(key)
        if value not in (None,""): return str(value)
    return str(props.get("externalid", ""))


def polygonal(g: Any) -> Any:
    """Только площадные части пересечения (касания по линии и точке отбрасываются)."""
    from shapely.geometry import Polygon
    from shapely.ops import unary_union
    if g is None or g.is_empty: return Polygon()
    if g.geom_type in ("Polygon","MultiPolygon"): return g
    if g.geom_type == "GeometryCollection":
        parts=[p for p in g.geoms if p.geom_type in ("Polygon","MultiPolygon")]
        return unary_union(parts) if parts else Polygon()
    return Polygon()


def spatial_protection_join(pieces:list[dict[str,Any]], protection:dict[str,list[dict[str,Any]]],
                            min_area_m2:float)->tuple[list[dict[str,Any]],list[dict[str,Any]]]:
    """Добавляет защитные поля в pieces и возвращает геометрию пересечений."""
    try:
        import geopandas as gpd
    except ImportError as exc:
        raise RuntimeError("Для категорий защитности нужны geopandas, shapely и pyogrio") from exc

    defaults={
      "protective_category_public":"","protective_category_management":"","protective_category":"",
      "protective_subcategory_public":"","protective_subcategory_management":"","protective_subcategory":"",
      "ozu_public":"нет","ozu_management":"нет","ozu_present":"нет",
      "ozu_type_public":"","ozu_type_management":"","ozu_type":"",
      "protection_sources":"","protection_intersections":0,"protection_overlap_area_ha":0.0,
    }
    piece_by_id={str(f.get("id")):f for f in pieces}
    accum={pid:{"category_public":set(),"category_management":set(),
                "subcategory_public":set(),"subcategory_management":set(),
                "ozu_public":set(),"ozu_management":set(),"sources":set(),"areas":[]} for pid in piece_by_id}
    links=[]
    if not pieces:
        return pieces,links
    pgdf=gpd.GeoDataFrame.from_features(pieces,crs="EPSG:4326")
    pgdf["piece_wfs_id"]=[str(f.get("id")) for f in pieces]
    pgdf=pgdf[["piece_wfs_id","geometry"]].to_crs(6933)
    pgdf["piece_area_m2"]=pgdf.geometry.area
    try: pgdf.geometry=pgdf.geometry.make_valid()
    except Exception: pgdf.geometry=pgdf.geometry.buffer(0)

    link_props=[]; link_geoms=[]
    for key,features in protection.items():
        if not features: continue
        cfg=PROTECTION_LAYERS[key]
        zgdf=gpd.GeoDataFrame.from_features(features,crs="EPSG:4326")
        zgdf["protection_wfs_id"]=[str(f.get("id")) for f in features]
        zgdf["protection_externalid"]=[ext(f) for f in features]
        zgdf["protection_type"]=[protection_value(f.get("properties",{})) for f in features]
        zgdf["registration_no_management"]=[str(f.get("properties",{}).get("registration_no_management", "")) for f in features]
        zgdf=zgdf[["protection_wfs_id","protection_externalid","protection_type","registration_no_management","geometry"]].to_crs(6933)
        try: zgdf.geometry=zgdf.geometry.make_valid()
        except Exception: zgdf.geometry=zgdf.geometry.buffer(0)
        joined=gpd.sjoin(pgdf,zgdf,predicate="intersects",how="inner")
        if joined.empty: continue
        for _,row in joined.iterrows():
            pgeom=pgdf.loc[row.name,"geometry"]
            zgeom=zgdf.loc[row["index_right"],"geometry"]
            inter=polygonal(pgeom.intersection(zgeom))
            area=float(inter.area)
            if inter.is_empty or area<min_area_m2: continue
            pid=str(row["piece_wfs_id"]); value=str(row["protection_type"])
            group=cfg["group"]; source=cfg["source"]
            accum[pid][f"{group}_{source}"].add(value)
            accum[pid]["sources"].add(key); accum[pid]["areas"].append(area)
            share=100.0*area/float(row["piece_area_m2"]) if row["piece_area_m2"] else 0.0
            link_geoms.append(inter)
            link_props.append({"id":f"{pid}|{row['protection_wfs_id']}|{key}",
              "properties":{"piece_wfs_id":pid,
                "piece_externalid":piece_by_id[pid].get("properties",{}).get("externalid"),
                "piece_label":piece_by_id[pid].get("properties",{}).get("label_name"),
                "source_layer":key,"source_layer_title":cfg["title"],"source_kind":source,
                "protection_group":group,"protection_type":value,
                "protection_wfs_id":str(row["protection_wfs_id"]),
                "protection_externalid":str(row["protection_externalid"]),
                "registration_no_management":str(row["registration_no_management"]),
                "overlap_area_m2":round(area,2),"overlap_area_ha":round(area/10000,6),
                "piece_share_pct":round(share,4)}})
    if link_geoms:
        geo=json.loads(gpd.GeoSeries(link_geoms,crs=6933).to_crs(4326).to_json())["features"]
        for meta,g in zip(link_props,geo):
            links.append({"type":"Feature","id":meta["id"],"geometry":g["geometry"],"properties":meta["properties"]})

    for pid,f in piece_by_id.items():
        props=f.setdefault("properties",{}); props.update(defaults)
        a=accum[pid]
        def joined_values(group:str,source:str)->str:
            return "; ".join(sorted(a[f"{group}_{source}"],key=natural_key))
        for group,prefix in (("category","protective_category"),("subcategory","protective_subcategory")):
            pub=joined_values(group,"public"); mgmt=joined_values(group,"management")
            props[prefix+"_public"]=pub; props[prefix+"_management"]=mgmt
            props[prefix]="; ".join(dict.fromkeys(x for x in (pub,mgmt) if x))
        opub=joined_values("ozu","public"); omgmt=joined_values("ozu","management")
        props["ozu_public"]="да" if opub else "нет"; props["ozu_management"]="да" if omgmt else "нет"
        props["ozu_present"]="да" if opub or omgmt else "нет"
        props["ozu_type_public"]=opub; props["ozu_type_management"]=omgmt
        props["ozu_type"]="; ".join(dict.fromkeys(x for x in (opub,omgmt) if x))
        props["protection_sources"]="; ".join(sorted(a["sources"]))
        props["protection_intersections"]=len(a["areas"])
        # Сумма может превышать площадь выдела при наложении разных тематических слоёв.
        props["protection_overlap_area_ha"]=round(sum(a["areas"])/10000,6)
    return pieces,links


# ───────────────────────── запись результатов ─────────────────────────

def save_geojson(path:Path,features:list[dict[str,Any]])->None:
    fc={"type":"FeatureCollection","name":path.stem,
        "crs":{"type":"name","properties":{"name":"urn:ogc:def:crs:OGC:1.3:CRS84"}},"features":features}
    path.write_text(json.dumps(fc,ensure_ascii=False),encoding="utf-8")


def save_csv(path:Path,features:list[dict[str,Any]])->None:
    keys=set()
    for f in features: keys.update(f.get("properties",{}))
    preferred=["wfs_id","id","externalid","label_name","number","number_lud","forest_quarter_number",
      "protective_category","protective_category_public","protective_category_management",
      "protective_subcategory","protective_subcategory_public","protective_subcategory_management",
      "ozu_present","ozu_public","ozu_management","ozu_type","ozu_type_public","ozu_type_management",
      "protection_sources","protection_intersections","protection_overlap_area_ha"]
    fields=[x for x in preferred if x in keys]+sorted(keys.difference(preferred))
    with path.open("w",encoding="utf-8-sig",newline="") as stream:
        w=csv.DictWriter(stream,fieldnames=fields,extrasaction="ignore"); w.writeheader()
        for f in features: w.writerow(f.get("properties",{}))


def _cell(v: Any) -> Any:
    if v is None or (isinstance(v, float) and math.isnan(v)): return None
    if isinstance(v, str): return v
    if isinstance(v, (dict, list, tuple)): return json.dumps(v, ensure_ascii=False)
    return str(v)


def features_to_gdf(fs: list[dict[str,Any]]):
    """GeoDataFrame, пригодный для GPKG: вложенные значения — JSON-строкой, смешанные типы — текстом,
    имена полей без повторов без учёта регистра, поле fid источника переименовано в fid_src."""
    import geopandas as gpd
    gdf = gpd.GeoDataFrame.from_features(fs, crs="EPSG:4326")
    seen: set[str] = set(); ren = {}
    for c in gdf.columns:
        if c == "geometry": continue
        n = "fid_src" if str(c).lower() == "fid" else str(c); base = n; k = 2
        while n.lower() in seen or n.lower() == "geometry":
            n = f"{base}_{k}"; k += 1
        seen.add(n.lower())
        if n != c: ren[c] = n
    if ren: gdf = gdf.rename(columns=ren)
    for c in gdf.columns:
        if c != "geometry" and gdf[c].dtype == object:
            gdf[c] = gdf[c].map(_cell)
    return prepare_gdf(gdf)


NUMERIC_FIELDS = ("square", "totalArea", "timber_stock")


def to_multipolygon(g: Any) -> Any:
    """Площадная геометрия в виде MultiPolygon: исправление невалидной, извлечение полигонов
    из GeometryCollection; непригодная геометрия — None."""
    from shapely.geometry import MultiPolygon
    if g is None or g.is_empty: return None
    if not g.is_valid:
        try:
            from shapely import make_valid; g = make_valid(g)
        except Exception: g = g.buffer(0)
    if g.geom_type == "GeometryCollection":
        parts = []
        for p in g.geoms:
            if p.geom_type == "Polygon": parts.append(p)
            elif p.geom_type == "MultiPolygon": parts.extend(p.geoms)
        g = MultiPolygon(parts) if parts else None
    if g is None or g.is_empty: return None
    if g.geom_type == "Polygon": return MultiPolygon([g])
    if g.geom_type == "MultiPolygon": return g
    return None


def prepare_gdf(gdf: Any) -> Any:
    """Приведение слоя к виду, который одинаково читают QGIS и ArcGIS Pro:
    один тип геометрии (MultiPolygon) на слой и числовые поля площади/запаса."""
    import pandas as pd
    if "geometry" in gdf.columns and gdf.geometry.notna().any():
        types = gdf.geometry.dropna().geom_type
        if types.isin(["Polygon", "MultiPolygon", "GeometryCollection"]).all():
            bad = int((~types.isin(["Polygon", "MultiPolygon"])).sum() + (~gdf.geometry.dropna().is_valid).sum())
            gdf = gdf.set_geometry(gdf.geometry.map(to_multipolygon), crs=gdf.crs)
            if bad: print(f"    исправлено геометрий: {bad}")
    for c in NUMERIC_FIELDS:
        if c in gdf.columns and not pd.api.types.is_numeric_dtype(gdf[c]):
            vals = gdf[c].astype(object).where(gdf[c].notna(), None)
            num = pd.to_numeric(vals.map(lambda v: v.replace(",", ".") if isinstance(v, str) else v),
                                errors="coerce")
            if int(num.notna().sum()) == int(vals.map(lambda v: v not in (None, "")).sum()):
                gdf[c] = num
    return gdf


def write_layer(path: Path, name: str, gdf: Any, first: bool) -> None:
    import pyogrio
    polygonal_layer = ("geometry" in gdf.columns and gdf.geometry.notna().any()
                       and gdf.geometry.dropna().geom_type.eq("MultiPolygon").all())
    kw = {"geometry_type": "MultiPolygon"} if polygonal_layer else {"promote_to_multi": True}
    pyogrio.write_dataframe(gdf, path, layer=name, driver="GPKG", append=not first, **kw)


def finalize_gpkg(path: Path) -> None:
    """Обычный журнал SQLite вместо WAL: без файлов -wal/-shm рядом с GPKG
    (важно для ArcGIS Pro и синхронизации облачных дисков)."""
    import sqlite3
    con = sqlite3.connect(str(path))
    try:
        con.execute("PRAGMA journal_mode=DELETE"); con.commit()
    finally:
        con.close()


def repair_gpkg(path: Path) -> Path:
    """Исправляет уже созданный GPKG без повторной загрузки: типы геометрии, числовые поля, журнал."""
    import pyogrio
    path = Path(path)
    if not path.exists(): raise RuntimeError(f"Файл не найден: {path}")
    tmp = path.with_name(path.stem + ".repair.partial.gpkg")
    if tmp.exists(): tmp.unlink()
    layers = pyogrio.list_layers(path)
    print(f"Исправление {path.name}: слоёв {len(layers)}")
    first = True
    for name, gtype in layers:
        t = time.time()
        df = pyogrio.read_dataframe(path, layer=name, fid_as_index=False)
        if gtype is None or "geometry" not in getattr(df, "columns", []):
            pyogrio.write_dataframe(df, tmp, layer=name, driver="GPKG", append=not first)
        else:
            if "fid" in df.columns: df = df.drop(columns="fid")
            write_layer(tmp, name, prepare_gdf(df), first)
        first = False
        print(f"  {name}: {len(df)} записей, {gtype or 'таблица'} ({fmt_time(time.time()-t)})")
    finalize_gpkg(tmp)
    os.replace(tmp, path)
    print(f"Готово: {path.resolve()}")
    return path


def save_gpkg(path:Path,datasets:list[tuple[str,list[dict[str,Any]]]],metadata:dict[str,Any])->Path|None:
    """Запись во временный файл и замена итогового: прерванная запись не портит прежний GPKG."""
    import pandas as pd
    tmp = path.with_name(path.stem + ".partial.gpkg")
    if tmp.exists(): tmp.unlink()
    written = 0
    for name,fs in datasets:
        if not fs:
            print(f"  {name}: пустой слой, в GPKG не записан"); continue
        gdf = features_to_gdf(fs)
        write_layer(tmp, name, gdf, not written)
        written += 1
        print(f"  {name}: {len(gdf)} объектов, {len(gdf.columns)-1} полей")
    if not written:
        print("  нет данных для записи GPKG"); return None
    try:
        import pyogrio
        meta = pd.DataFrame({"key": list(metadata), "value": [_cell(v) for v in metadata.values()]})
        pyogrio.write_dataframe(meta, tmp, layer="metadata", driver="GPKG", append=True)
    except Exception as exc:
        print(f"  таблица metadata не записана: {short(exc)}")
    try: finalize_gpkg(tmp)
    except Exception as exc: print(f"  журнал SQLite не переключён: {short(exc)}")
    try:
        os.replace(tmp, path); return path
    except OSError:
        alt = path.with_name(path.stem + time.strftime("_%d%m%y_%H%M") + ".gpkg")
        os.replace(tmp, alt)
        print(f"  {path.name} занят другой программой; результат записан в {alt.name}")
        return alt


# ───────────────────────── журнал ─────────────────────────

class Tee:
    def __init__(self, stream: Any, fh: Any):
        self.stream, self.fh, self.lock = stream, fh, threading.Lock()

    def write(self, s: str) -> int:
        with self.lock:
            try: self.stream.write(s)
            except Exception: pass
            self.fh.write(s); self.fh.flush()
        return len(s)

    def flush(self) -> None:
        try: self.stream.flush()
        except Exception: pass
        self.fh.flush()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.stream, name)


_LOG: dict[str, Any] = {}


def start_log(path: Path) -> None:
    stop_log()
    fh = path.open("a", encoding="utf-8")
    fh.write("\n" + "=" * 78 + f"\nЗапуск {time.strftime('%d.%m.%Y %H:%M:%S')}, fgislk_export.py v{VERSION}\n")
    _LOG.update(fh=fh, stdout=sys.stdout, stderr=sys.stderr)
    sys.stdout = Tee(sys.stdout, fh); sys.stderr = Tee(sys.stderr, fh)


def stop_log() -> None:
    """Возвращает исходные потоки вывода (важно для Jupyter, где ядро продолжает работать)."""
    if not _LOG:
        return
    try: sys.stdout.flush(); sys.stderr.flush()
    except Exception: pass
    sys.stdout, sys.stderr = _LOG["stdout"], _LOG["stderr"]
    try: _LOG["fh"].close()
    except Exception: pass
    _LOG.clear()


def in_ipython() -> bool:
    """Запуск из Jupyter/IPython (%run): окно не закрывается, ядро завершать нельзя."""
    import builtins
    return bool(getattr(builtins, "__IPYTHON__", False))


# ───────────────────────── экспорт ─────────────────────────

OPTION_KEYS = ("page_size","timeout","workers","verify_ssl","no_attributes","no_protection",
               "min_overlap_m2","max_quarters","max_pieces","geojson","csv")


def export(args:argparse.Namespace,code:str,selected:list[dict[str,Any]],all_flag:bool,
           out:Path,session:requests.Session)->Path|None:
    out.mkdir(parents=True,exist_ok=True)
    selected_ids=[ext(f) for f in selected]
    tag=selection_tag(code,selected_ids,all_flag)
    stem=f"fgislk_{tag}"+("_test" if args.max_quarters or args.max_pieces else "")
    start_log(out/f"{stem}_log.txt")
    root=cache_dir(out,tag)
    if args.restart and root.exists():
        shutil.rmtree(root); print("Сохранённые ранее данные удалены (--restart)")
    store=Store(root)
    run=read_run(root) or {"started":time.strftime("%d.%m.%Y %H:%M")}
    run.update({"status":"running","version":VERSION,"region":code,"all":all_flag,
                "selected":[{"externalid":ext(f),"label_name":label(f)} for f in selected],
                "options":{k:getattr(args,k) for k in OPTION_KEYS}})
    atomic_write_text(root/"run.json",json.dumps(run,ensure_ascii=False,indent=2))
    save_settings(last_out=str(out))
    print(f"Регион {code} — {REGIONS.get(code,code)}; лесничества: {', '.join(selected_ids)}")
    print(f"Папка: {out.resolve()}\nПромежуточные данные: {root.name}")
    t0=time.time()

    print("Каталог участковых лесничеств...")
    district_catalog=store.get("catalog","districts",lambda part: fetch_district_catalog(session,selected_ids,args.timeout))
    central_full=[]; district_full=[]; quarters_all={}; pieces_all={}
    protection_all={k:{} for k in PROTECTION_LAYERS}

    for ci,item in enumerate(selected,1):
        cid=ext(item); print(f"\n=== [{ci}/{len(selected)}] {cid} {label(item)} ===")
        cfull=store.get("central",cid,lambda part: fetch_exact(session,LAYERS["central_forestries"],cid,args.timeout))
        central_full.extend(cfull)
        districts=[d for d in district_catalog if ext(d).startswith(cid+":") and len(ext(d).split(":"))==len(cid.split(":"))+1]
        sources=[]
        for d in districts:
            dfull=store.get("district",ext(d),lambda part,d=d: fetch_exact(session,LAYERS["district_forestries"],ext(d),args.timeout))
            district_full.extend(dfull); sources.extend((ext(d),x) for x in dfull)
        if not sources: sources.extend((cid,x) for x in cfull)

        if cfull and not args.no_protection:
            bbox=padded(union_bbox(cfull))
            print("  Защитные слои:")
            for key,cfg in PROTECTION_LAYERS.items():
                was=store.has("protection",f"{cid}__{key}")
                fs=store.get("protection",f"{cid}__{key}",
                             lambda part,cfg=cfg: fetch_pages(session,cfg["layer"],args.timeout,args.page_size,
                                                              bbox=bbox,quiet=True,ckpt=part))
                for f in fs: protection_all[key][str(f.get("id"))]=f
                print(f"    {key}: {len(fs)}"+(" (сохранено ранее)" if was else ""))

        for di,(did,dfeature) in enumerate(sources,1):
            print(f"  [{di}/{len(sources)}] участковое {did} {label(dfeature)}")
            def get_quarters(part:Path,did:str=did,dfeature:dict[str,Any]=dfeature)->list[dict[str,Any]]:
                cand=fetch_pages(session,LAYERS["quarters"],args.timeout,args.page_size,
                                 bbox=padded(feature_bbox(dfeature)),quiet=True,ckpt=part)
                return sort_features(f for f in cand if ext(f).startswith(did+":")
                                     and len(ext(f).split(":"))==len(did.split(":"))+1)
            quarters=store.get("quarters",did,get_quarters)
            if args.max_quarters: quarters=quarters[:args.max_quarters]
            nq=len(quarters); cached=0; td=time.time(); fetched=0
            print(f"    кварталов: {nq}")
            for qi,q in enumerate(quarters,1):
                qid=ext(q); quarters_all[str(q.get("id"))]=q
                was=store.has("pieces",qid)
                def get_pieces(part:Path,q:dict[str,Any]=q,qid:str=qid)->list[dict[str,Any]]:
                    cand=fetch_pages(session,LAYERS["taxation_pieces"],args.timeout,args.page_size,
                                     bbox=padded(feature_bbox(q)),quiet=True,ckpt=part)
                    return sort_features(f for f in cand if ext(f).startswith(qid+":")
                                         and len(ext(f).split(":"))==len(qid.split(":"))+1)
                pieces=store.get("pieces",qid,get_pieces)
                if args.max_pieces: pieces=pieces[:args.max_pieces]
                for p in pieces: pieces_all[str(p.get("id"))]=p
                if was:
                    cached+=1; continue
                fetched+=1; el=time.time()-td; eta=el/fetched*(nq-qi)
                print(f"      [{qi}/{nq}] квартал {qid}, выделов {len(pieces)}; осталось около {fmt_time(eta)}")
            if cached: print(f"    сохранено ранее: {cached} из {nq} кварталов")

    print(f"\nЗагрузка геометрии завершена за {fmt_time(time.time()-t0)}; "
          f"запросов к API {store.fetched}, взято из сохранённого {store.hits}")
    quarters=sort_features(quarters_all.values()); pieces=sort_features(pieces_all.values())
    central_boundaries=sort_features(central_full)
    forestry_bboxes,selected_area_bbox=make_forestry_bboxes(central_boundaries)
    if forestry_bboxes:
        print("BBOX выбранных лесничеств (west,south,east,north; EPSG:4326):")
        for f in forestry_bboxes:
            pr=f["properties"]
            print(f"  {pr['forestry_id']}: {pr['bbox_wsen']}")
        if len(forestry_bboxes)>1:
            print(f"  общий: {selected_area_bbox[0]['properties']['bbox_wsen']}")
    attr_gaps=0
    if not args.no_attributes:
        attr_gaps=enrich_attributes(pieces,root/"attributes.jsonl",args.timeout,args.verify_ssl,args.workers)
    protection={k:sort_features(v.values()) for k,v in protection_all.items()}
    links=[]
    if not args.no_protection:
        print("Пространственное пересечение защитных слоёв с выделами...")
        pieces,links=spatial_protection_join(pieces,protection,args.min_overlap_m2)
        print(f"  записей пересечений: {len(links)}; выделов с ОЗУ: {sum(f['properties'].get('ozu_present')=='да' for f in pieces)}")

    datasets=[("selected_forestry_boundaries",central_boundaries),
              ("selected_forestry_bbox",forestry_bboxes),
              ("selected_area_bbox",selected_area_bbox),
              ("central_forestries",central_boundaries),("district_forestries",sort_features(district_full)),
              ("quarters",quarters),("taxation_pieces",pieces)]
    if not args.no_protection:
        for key,fs in protection.items(): datasets.append((key,fs))
        datasets.append(("piece_protection_links",links))
    metadata={"source":GEO_API,"script_version":VERSION,"region_code":code,"region_name":REGIONS.get(code),
      "selected_central_forestries":selected_ids,"central_forestries":len(central_full),
      "district_forestries":len(district_full),"quarters":len(quarters),"taxation_pieces":len(pieces),
      "detailed_attributes":not args.no_attributes,"attributes_missing":attr_gaps,
      "protection_enabled":not args.no_protection,
      "protection_layers":{k:len(v) for k,v in protection.items()},"protection_links":len(links),
      "pieces_with_ozu":sum(f["properties"].get("ozu_present")=="да" for f in pieces),
      "min_overlap_m2":args.min_overlap_m2,"max_quarters":args.max_quarters,"max_pieces":args.max_pieces,
      "crs":"EPSG:4326 / OGC:CRS84","area_crs":"EPSG:6933",
      "forestry_bbox_crs":"EPSG:4326",
      "forestry_bboxes":{str(f["properties"]["forestry_id"]):[
          f["properties"]["min_lon"],f["properties"]["min_lat"],
          f["properties"]["max_lon"],f["properties"]["max_lat"]] for f in forestry_bboxes},
      "selected_area_bbox":[selected_area_bbox[0]["properties"][k] for k in
          ("min_lon","min_lat","max_lon","max_lat")] if selected_area_bbox else None,
      "created_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}

    print("\nЗапись GeoPackage...")
    gpkg=save_gpkg(out/f"{stem}.gpkg",datasets,metadata)
    if args.geojson or args.csv:
        for name,fs in datasets:
            if args.geojson:
                d=out/f"{stem}_geojson"; d.mkdir(exist_ok=True); save_geojson(d/f"{name}.geojson",fs)
            if args.csv:
                d=out/f"{stem}_csv"; d.mkdir(exist_ok=True); save_csv(d/f"{name}.csv",fs)
    print("\n"+json.dumps(metadata,ensure_ascii=False,indent=2))

    test_run=bool(args.max_quarters or args.max_pieces)
    run["status"]="running" if attr_gaps or gpkg is None else "done"
    run["finished"]=time.strftime("%d.%m.%Y %H:%M"); run["gpkg"]=gpkg.name if gpkg else None
    atomic_write_text(root/"run.json",json.dumps(run,ensure_ascii=False,indent=2))
    if gpkg is None:
        print("GPKG не создан: нет данных."); return None
    if run["status"]=="done" and not test_run and args.clean_cache:
        shutil.rmtree(root,ignore_errors=True); print("Промежуточные данные удалены (--clean-cache).")
    elif run["status"]=="done":
        print(f"Промежуточные данные сохранены в {root.name}: повторный запуск с той же папкой "
              f"пересоберёт GPKG без загрузки. Удалить их: --clean-cache.")
    elif attr_gaps:
        print(f"Промежуточные данные сохранены в {root.name}: запустите скрипт с той же папкой, "
              f"чтобы дозапросить атрибуты.")
    print(f"\nГотово за {fmt_time(time.time()-t0)}: {gpkg.resolve()}")
    return gpkg


def parse_args()->argparse.Namespace:
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region"); p.add_argument("--forestry",help="Учётные номера через запятую")
    p.add_argument("--all-forestries",action="store_true"); p.add_argument("--list-regions",action="store_true")
    p.add_argument("--list-forestries",metavar="REGION")
    p.add_argument("--out",help="Папка загрузки; без параметра в интерактивном режиме открывается окно выбора")
    p.add_argument("--page-size",type=int,default=500); p.add_argument("--timeout",type=int,default=180)
    p.add_argument("--workers",type=int,default=4); p.add_argument("--verify-ssl",action="store_true")
    p.add_argument("--no-attributes",action="store_true"); p.add_argument("--no-protection",action="store_true")
    p.add_argument("--min-overlap-m2",type=float,default=1.0,help="Минимальная площадь пересечения, м²")
    p.add_argument("--max-quarters",type=int,default=0); p.add_argument("--max-pieces",type=int,default=0)
    p.add_argument("--geojson",action="store_true",help="Дополнительно GeoJSON по слоям")
    p.add_argument("--csv",action="store_true",help="Дополнительно CSV по слоям")
    p.add_argument("--clean-cache",action="store_true",help="Удалить промежуточные данные после успешной записи GPKG")
    p.add_argument("--keep-cache",action="store_true",help=argparse.SUPPRESS)   # совместимость: теперь по умолчанию
    p.add_argument("--repair",metavar="GPKG",help="Исправить готовый GPKG для ArcGIS Pro без повторной загрузки")
    p.add_argument("--restart",action="store_true",help="Начать загрузку заново, удалив сохранённое")
    p.add_argument("--no-pause",action="store_true",help="Не ждать Enter перед закрытием окна")
    a=p.parse_args(); a.workers=max(1,min(8,a.workers)); return a


def check_dependencies()->None:
    missing=[]
    for mod in ("geopandas","pyogrio","shapely"):
        try: __import__(mod)
        except ImportError: missing.append(mod)
    if missing:
        raise RuntimeError("Для записи GPKG нужны пакеты: "+", ".join(missing)+
                           ". Установка: python -m pip install requests geopandas pyogrio shapely")


def drop_test(central:list[dict[str,Any]])->list[dict[str,Any]]:
    """При выборе всех лесничеств служебные «Тестовое лесничество» исключаются (у 50:105 граница на Урале)."""
    keep=[f for f in central if "тестов" not in label(f).lower()]
    for f in central:
        if f not in keep: print(f"  пропущено служебное: {ext(f)} {label(f)}")
    return keep or central


def main(args:argparse.Namespace)->int:
    if args.repair:
        check_dependencies(); repair_gpkg(Path(args.repair).expanduser()); return 0
    session=make_session(args.verify_ssl)
    if args.list_regions: print_regions(); return 0
    if args.list_forestries:
        code=str(args.list_forestries).zfill(2); central=fetch_region_central(session,code,args.timeout)
        print(f"\n{code} — {REGIONS.get(code,code)}")
        for f in central: print(f"{ext(f):12} {label(f)}")
        return 0
    check_dependencies()
    interactive=not (args.region or args.forestry or args.all_forestries)
    out=Path(args.out).expanduser() if args.out else (ask_folder() if interactive else None)

    resumed=pick_unfinished(out) if (out is not None and interactive and not args.restart) else None
    if resumed:
        d,run=resumed; code=run["region"]; all_flag=bool(run.get("all"))
        for k,v in run.get("options",{}).items():
            if k in OPTION_KEYS: setattr(args,k,v)
        selected=[{"id":x["externalid"],"properties":{"externalid":x["externalid"],"label_name":x.get("label_name","")}}
                  for x in run["selected"]]
        print("Продолжаю незавершённую загрузку.")
        return 0 if export(args,code,selected,all_flag,out,session) else 1

    code=str(args.region or "").zfill(2)
    if not args.region:
        print_regions(); codes=sorted(REGIONS,key=natural_key); code=codes[choose("Выберите регион",len(codes))[0]]
    central=fetch_region_central(session,code,args.timeout)
    if not central: raise RuntimeError(f"Для региона {code} центральные лесничества не найдены")
    all_flag=False
    if args.forestry:
        wanted={x.strip() for x in args.forestry.split(",")}; selected=[f for f in central if ext(f) in wanted]
        missing=wanted-{ext(f) for f in selected}
        if missing: raise RuntimeError("Не найдены: "+", ".join(sorted(missing)))
    elif args.all_forestries: selected=drop_test(central); all_flag=True
    else:
        print(f"\n{code} — {REGIONS.get(code,code)}")
        for i,f in enumerate(central,1): print(f"{i:3}. {ext(f):12} {label(f)}")
        idx=choose("Выберите лесничество",len(central),True)
        selected=[central[i] for i in idx]; all_flag=len(idx)==len(central)
        if all_flag: selected=drop_test(selected)
    if out is None: out=Path(f"fgislk_{code}")
    return 0 if export(args,code,selected,all_flag,out,session) else 1


if __name__=="__main__":
    args=parse_args(); notebook=in_ipython()
    pause=not notebook and (len(sys.argv)==1 or
          (not args.no_pause and not (args.region or args.list_regions or args.list_forestries)))
    rc=1
    try: rc=main(args)
    except KeyboardInterrupt:
        _STOP.set()
        print("\nПрервано. Скачанное сохранено: запустите скрипт с той же папкой, чтобы продолжить.",file=sys.stderr); rc=130
    except Exception as exc:
        _STOP.set()
        print(f"ОШИБКА: {exc}",file=sys.stderr); rc=1
    if pause and not args.no_pause:
        try: input("\nНажмите Enter, чтобы закрыть окно...")
        except (EOFError, KeyboardInterrupt): pass
    stop_log()
    if notebook:
        # В Jupyter не завершаем ядро: фоновые потоки остановятся сами (_STOP установлен).
        if rc: print(f"Код завершения: {rc}", file=sys.stderr)
    elif _STOP.is_set():
        # Фоновые запросы атрибутов не ждём: всё полученное уже записано на диск.
        try: sys.stdout.flush(); sys.stderr.flush()
        except Exception: pass
        os._exit(rc)
    else:
        raise SystemExit(rc)
