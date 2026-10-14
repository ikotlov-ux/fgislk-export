#!/usr/bin/env python3
"""Экспорт лесоустроительных данных из Публичной лесной карты ФГИС ЛК.

Интерактивно выбирает регион и центральные лесничества, затем выгружает:
центральные и участковые лесничества, кварталы, выделы, подробные атрибуты
выделов и шесть слоёв категорий защитности/ОЗУ. Защитные сведения добавляются
к выделам пространственным пересечением. Дополнительно создаётся таблица
всех пересечений с площадью и долей выдела.

Установка:
    python -m pip install requests geopandas pyogrio shapely

Примеры:
    python fgislk_export.py
    python fgislk_export.py --list-regions
    python fgislk_export.py --list-forestries 26
    python fgislk_export.py --region 26 --forestry 26:4 --out essentuki
"""
from __future__ import annotations

import argparse
import base64
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MAP_API = "https://pub.fgislk.gov.ru/map/geo/map_api"
GEO_API = MAP_API + "/geo"
ATTR_API = MAP_API + "/layer/attributesinfo"

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


def natural_key(value: str) -> list[Any]:
    return [int(x) if x.isdigit() else x.lower() for x in re.split(r"(\d+)", value or "")]


def region_code(value: str) -> str:
    first = (value or "").split(":", 1)[0]
    return first.zfill(2) if first.isdigit() else first


def make_session(verify_ssl: bool) -> requests.Session:
    s = requests.Session(); s.verify = verify_ssl
    retry = Retry(total=6, connect=6, read=6, backoff_factor=1.0,
                  status_forcelist=(429,500,502,503,504), allowed_methods=frozenset(["GET"]))
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20))
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


def fetch_json(session: requests.Session, url: str, params: dict[str,str], timeout: int) -> Any:
    r = session.get(url, params=params, timeout=timeout); r.raise_for_status(); return r.json()


def normalize_collection(data: Any) -> dict[str,Any]:
    if isinstance(data, list) and (not data or "Данные не обнаружены" in str(data[0])):
        return {"type":"FeatureCollection", "features":[], "numberMatched":0}
    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise RuntimeError(f"Неожиданный ответ API: {str(data)[:500]}")
    return data


def fetch_pages(session: requests.Session, layer: str, timeout: int, page_size: int,
                geometry: bool=True, bbox: tuple[float,float,float,float]|None=None,
                cql: str|None=None, quiet: bool=False) -> list[dict[str,Any]]:
    result: dict[str,dict[str,Any]] = {}; start=0; previous:set[str]=set()
    current=max(1,page_size); total: int|None=None
    while True:
        p={"service":"WFS","version":"2.0.0","request":"GetFeature","typeNames":layer,
           "outputFormat":"application/json","srsName":"EPSG:4326","count":str(current),
           "startIndex":str(start),"selectControl":"true","sortBy":"id"}
        if not geometry: p["propertyName"]="id,label_name,externalid"
        if bbox: p["bbox"] = ",".join(f"{x:.8f}" for x in bbox)+",EPSG:4326"
        if cql: p["CQL_FILTER"] = encoded_cql(cql)
        try: data=normalize_collection(fetch_json(session,GEO_API,p,timeout))
        except requests.RequestException:
            if current>1:
                current=max(1,current//2)
                if not quiet: print(f"  сервер отклонил страницу; размер уменьшен до {current}")
                continue
            raise
        fs=data.get("features",[])
        if not fs: break
        ids=set()
        for f in fs:
            fid=str(f.get("id") or f.get("properties",{}).get("id")); ids.add(fid)
            f.setdefault("properties",{})["wfs_id"]=fid; result[fid]=f
        if ids==previous: raise RuntimeError("API повторил страницу")
        previous=ids; start += current
        matched=data.get("numberMatched",data.get("totalFeatures"))
        if isinstance(matched,int): total=matched
        if not quiet: print(f"  получено {len(result)}"+(f" из {matched}" if isinstance(matched,int) else ""))
        if total is not None and start>=total: break
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
    return min(b[0] for b in boxes),min(b[1] for b in boxes),max(b[2] for b in boxes),max(b[3] for b in boxes)


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


def object_id(f:dict[str,Any])->str:
    return str(f.get("id") or f.get("properties",{}).get("wfs_id","")).rsplit(".",1)[-1]


def load_cache(path:Path)->dict[str,dict[str,Any]]:
    out={}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row=json.loads(line); out[str(row["wfs_object_id"])]=row["payload"]
            except (ValueError,KeyError): pass
    return out


def fetch_attr(f:dict[str,Any],timeout:int,verify:bool)->tuple[str,dict[str,Any]]:
    oid=object_id(f); data=fetch_json(worker_session(verify),ATTR_API,
        {"layer_code":"TAXATION_PIECE","object_id":oid},timeout)
    payload=data.get("payload",{}) if isinstance(data,dict) else {}
    return oid,payload if isinstance(payload,dict) else {}


def enrich_attributes(fs:list[dict[str,Any]],cache_path:Path,timeout:int,verify:bool,workers:int)->None:
    cache=load_cache(cache_path); pending=[f for f in fs if object_id(f) not in cache]
    print(f"Подробные атрибуты: кэш {len(fs)-len(pending)}, запросить {len(pending)}")
    if pending:
        with cache_path.open("a",encoding="utf-8") as stream, ThreadPoolExecutor(max_workers=workers) as pool:
            futures={pool.submit(fetch_attr,f,timeout,verify):f for f in pending}
            for i,future in enumerate(as_completed(futures),1):
                fallback=object_id(futures[future])
                try: oid,payload=future.result()
                except Exception as exc:
                    print(f"  атрибуты {fallback}: {exc}",file=sys.stderr); oid,payload=fallback,{}
                cache[oid]=payload; stream.write(json.dumps({"wfs_object_id":oid,"payload":payload},ensure_ascii=False)+"\n"); stream.flush()
                if i%100==0 or i==len(pending): print(f"  атрибуты: {i}/{len(pending)}")
    for f in fs:
        props=f.setdefault("properties",{}); payload=cache.get(object_id(f),{})
        props["attributes_loaded"]=bool(payload)
        for k,v in payload.items(): props[k if k not in props else "attr_"+k]=v


def protection_value(props:dict[str,Any])->str:
    for key in ("type","label_name","name"):
        value=props.get(key)
        if value not in (None,""): return str(value)
    return str(props.get("externalid", ""))


def spatial_protection_join(pieces:list[dict[str,Any]], protection:dict[str,list[dict[str,Any]]],
                            min_area_m2:float)->tuple[list[dict[str,Any]],list[dict[str,Any]]]:
    """Добавляет защитные поля в pieces и возвращает геометрию пересечений."""
    try:
        import geopandas as gpd
        import pandas as pd
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
            inter=pgeom.intersection(zgeom)
            area=float(inter.area)
            if inter.is_empty or area<min_area_m2: continue
            pid=str(row["piece_wfs_id"]); value=str(row["protection_type"])
            group=cfg["group"]; source=cfg["source"]
            accum[pid][f"{group}_{source}"].add(value)
            accum[pid]["sources"].add(key); accum[pid]["areas"].append(area)
            share=100.0*area/float(row["piece_area_m2"]) if row["piece_area_m2"] else 0.0
            links.append({"type":"Feature","id":f"{pid}|{row['protection_wfs_id']}|{key}",
              "geometry":json.loads(gpd.GeoSeries([inter],crs=6933).to_crs(4326).to_json())["features"][0]["geometry"],
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


def save_gpkg(path:Path,datasets:list[tuple[str,list[dict[str,Any]]]])->bool:
    try: import geopandas as gpd
    except ImportError: print("GeoPandas не установлен: GeoPackage пропущен."); return False
    if path.exists(): path.unlink()
    first=True
    for name,fs in datasets:
        if not fs: continue
        gdf=gpd.GeoDataFrame.from_features(fs,crs="EPSG:4326")
        gdf.to_file(path,layer=name,driver="GPKG",mode="w" if first else "a",encoding="UTF-8")
        first=False
    return not first


def export(args:argparse.Namespace,code:str,selected:list[dict[str,Any]],session:requests.Session)->None:
    out=Path(args.out or f"fgislk_{code}"); out.mkdir(parents=True,exist_ok=True)
    selected_ids=[ext(f) for f in selected]
    print("Загружаю каталог участковых лесничеств...")
    district_catalog=fetch_district_catalog(session,selected_ids,args.timeout)
    central_full=[]; district_full=[]; quarters_all={}; pieces_all={}
    protection_all={k:{} for k in PROTECTION_LAYERS}

    for ci,item in enumerate(selected,1):
        cid=ext(item); print(f"\n=== [{ci}/{len(selected)}] {cid} {label(item)} ===")
        cfull=fetch_exact(session,LAYERS["central_forestries"],cid,args.timeout); central_full.extend(cfull)
        districts=[d for d in district_catalog if ext(d).startswith(cid+":") and len(ext(d).split(":"))==len(cid.split(":"))+1]
        sources=[]
        for d in districts:
            dfull=fetch_exact(session,LAYERS["district_forestries"],ext(d),args.timeout)
            district_full.extend(dfull); sources.extend((ext(d),x) for x in dfull)
        if not sources: sources.extend((cid,x) for x in cfull)

        if cfull and not args.no_protection:
            bbox=padded(union_bbox(cfull))
            print("  Защитные слои:")
            for key,cfg in PROTECTION_LAYERS.items():
                fs=fetch_pages(session,cfg["layer"],args.timeout,args.page_size,bbox=bbox,quiet=True)
                for f in fs: protection_all[key][str(f.get("id"))]=f
                print(f"    {key}: {len(fs)}")

        for di,(did,dfeature) in enumerate(sources,1):
            print(f"  [{di}/{len(sources)}] участковое {did} {label(dfeature)}")
            candidates=fetch_pages(session,LAYERS["quarters"],args.timeout,args.page_size,bbox=padded(feature_bbox(dfeature)),quiet=True)
            prefix=did+":"
            quarters=sort_features(f for f in candidates if ext(f).startswith(prefix) and len(ext(f).split(":"))==len(did.split(":"))+1)
            if args.max_quarters: quarters=quarters[:args.max_quarters]
            print(f"    кварталов: {len(quarters)}")
            for qi,q in enumerate(quarters,1):
                qid=ext(q); quarters_all[str(q.get("id"))]=q
                candidates_p=fetch_pages(session,LAYERS["taxation_pieces"],args.timeout,args.page_size,bbox=padded(feature_bbox(q)),quiet=True)
                pfx=qid+":"
                pieces=sort_features(f for f in candidates_p if ext(f).startswith(pfx) and len(ext(f).split(":"))==len(qid.split(":"))+1)
                if args.max_pieces: pieces=pieces[:args.max_pieces]
                for p in pieces: pieces_all[str(p.get("id"))]=p
                print(f"      [{qi}/{len(quarters)}] квартал {qid}, выделов {len(pieces)}")

    quarters=sort_features(quarters_all.values()); pieces=sort_features(pieces_all.values())
    if not args.no_attributes: enrich_attributes(pieces,out/"attributes_cache.jsonl",args.timeout,args.verify_ssl,args.workers)
    protection={k:sort_features(v.values()) for k,v in protection_all.items()}
    links=[]
    if not args.no_protection:
        print("Выполняю пространственное пересечение защитных слоёв с выделами...")
        pieces,links=spatial_protection_join(pieces,protection,args.min_overlap_m2)
        print(f"  записей пересечений: {len(links)}; выделов с ОЗУ: {sum(f['properties'].get('ozu_present')=='да' for f in pieces)}")

    datasets=[("central_forestries",sort_features(central_full)),("district_forestries",sort_features(district_full)),
              ("quarters",quarters),("taxation_pieces",pieces)]
    for key,fs in protection.items(): datasets.append((key,fs))
    datasets.append(("piece_protection_links",links))
    for name,fs in datasets:
        save_geojson(out/f"{name}.geojson",fs); save_csv(out/f"{name}.csv",fs)
    gpkg=save_gpkg(out/f"fgislk_{code}.gpkg",datasets)
    metadata={"source":GEO_API,"region_code":code,"region_name":REGIONS.get(code),
      "selected_central_forestries":selected_ids,"central_forestries":len(central_full),
      "district_forestries":len(district_full),"quarters":len(quarters),"taxation_pieces":len(pieces),
      "detailed_attributes":not args.no_attributes,"protection_enabled":not args.no_protection,
      "protection_layers":{k:len(v) for k,v in protection.items()},"protection_links":len(links),
      "pieces_with_ozu":sum(f["properties"].get("ozu_present")=="да" for f in pieces),
      "min_overlap_m2":args.min_overlap_m2,"crs":"EPSG:4326 / OGC:CRS84","area_crs":"EPSG:6933",
      "gpkg_created":gpkg,"created_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
    (out/"metadata.json").write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    print("\n"+json.dumps(metadata,ensure_ascii=False,indent=2)); print(f"Готово: {out.resolve()}")


def parse_args()->argparse.Namespace:
    p=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--region"); p.add_argument("--forestry",help="Учётные номера через запятую")
    p.add_argument("--all-forestries",action="store_true"); p.add_argument("--list-regions",action="store_true")
    p.add_argument("--list-forestries",metavar="REGION"); p.add_argument("--out")
    p.add_argument("--page-size",type=int,default=500); p.add_argument("--timeout",type=int,default=180)
    p.add_argument("--workers",type=int,default=4); p.add_argument("--verify-ssl",action="store_true")
    p.add_argument("--no-attributes",action="store_true"); p.add_argument("--no-protection",action="store_true")
    p.add_argument("--min-overlap-m2",type=float,default=1.0,help="Минимальная площадь пересечения, м²")
    p.add_argument("--max-quarters",type=int,default=0); p.add_argument("--max-pieces",type=int,default=0)
    a=p.parse_args(); a.workers=max(1,min(8,a.workers)); return a


def main()->int:
    args=parse_args(); session=make_session(args.verify_ssl)
    if args.list_regions: print_regions(); return 0
    code=str(args.region or args.list_forestries or "").zfill(2)
    if not code or code=="00":
        print_regions(); codes=sorted(REGIONS,key=natural_key); code=codes[choose("Выберите регион",len(codes))[0]]
    central=fetch_region_central(session,code,args.timeout)
    if args.list_forestries:
        print(f"\n{code} — {REGIONS.get(code,code)}")
        for f in central: print(f"{ext(f):12} {label(f)}")
        return 0
    if args.forestry:
        wanted={x.strip() for x in args.forestry.split(",")}; selected=[f for f in central if ext(f) in wanted]
        missing=wanted-{ext(f) for f in selected}
        if missing: raise RuntimeError("Не найдены: "+", ".join(sorted(missing)))
    elif args.all_forestries: selected=central
    else:
        print(f"\n{code} — {REGIONS.get(code,code)}")
        for i,f in enumerate(central,1): print(f"{i:3}. {ext(f):12} {label(f)}")
        selected=[central[i] for i in choose("Выберите лесничество",len(central),True)]
    export(args,code,selected,session); return 0

if __name__=="__main__":
    try: raise SystemExit(main())
    except KeyboardInterrupt: print("\nПрервано",file=sys.stderr); raise SystemExit(130)
    except Exception as exc: print(f"ОШИБКА: {exc}",file=sys.stderr); raise SystemExit(1)
