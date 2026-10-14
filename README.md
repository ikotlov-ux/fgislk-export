# Экспорт ФГИС ЛК: выделы и защитность

## Назначение

`fgislk_export.py` позволяет выбрать регион и одно, несколько либо все центральные лесничества. Скрипт выгружает:

- центральные лесничества;
- участковые лесничества;
- кварталы;
- лесотаксационные выделы и их подробные атрибуты;
- категории защитных лесов и особо защитные участки из шести слоёв ФГИС ЛК;
- пространственную таблицу связей между выделами и защитными объектами.

## Установка

```bash
python -m pip install requests geopandas pyogrio shapely
```

## Запуск

Интерактивный режим:

```bash
python fgislk_export.py
```

Командная строка:

```bash
python fgislk_export.py --list-regions
python fgislk_export.py --list-forestries 26
python fgislk_export.py --region 26 --forestry 26:4 --out essentuki
python fgislk_export.py --region 26 --forestry 26:4,26:7 --out kmv
python fgislk_export.py --region 26 --all-forestries --out stavropol
```

Короткий тест:

```bash
python fgislk_export.py --region 21 --forestry 21:1 \
  --max-quarters 1 --max-pieces 30 --out test
```

## Защитные слои

Используются:

- `PROTECTIVE_FOREST` — категории защитных лесов;
- `SPECIAL_PROTECT_STEAD` — особо защитные участки лесов;
- `PROTECTIVE_FOREST_SUBCATEGORY` — ценные леса и леса, выполняющие функции защиты объектов;
- `KZL_MANAGEMENT` — категории защитных лесов по лесоустройству;
- `OZUL_MANAGEMENT` — особо защитные участки по лесоустройству;
- `PROTECTIVE_FOREST_SUBCATEGORY_MANAGEMENT` — подкатегории защитных лесов по лесоустройству.

Каждый исходный слой сохраняется в GeoPackage, GeoJSON и CSV. Если в выбранной территории слой пуст, он остаётся пустым — категория не подставляется предположительно.

## Поля выделов

В слое `taxation_pieces` появляются:

- `protective_category_public` — категория из `PROTECTIVE_FOREST`;
- `protective_category_management` — категория из `KZL_MANAGEMENT`;
- `protective_category` — объединённое значение из обоих источников;
- `protective_subcategory_public` — значение из `PROTECTIVE_FOREST_SUBCATEGORY`;
- `protective_subcategory_management` — значение из `PROTECTIVE_FOREST_SUBCATEGORY_MANAGEMENT`;
- `protective_subcategory` — объединённая подкатегория;
- `ozu_public` — пересекается ли выдел с `SPECIAL_PROTECT_STEAD`;
- `ozu_management` — пересекается ли выдел с `OZUL_MANAGEMENT`;
- `ozu_present` — итоговое наличие ОЗУ;
- `ozu_type_public`, `ozu_type_management`, `ozu_type` — типы ОЗУ;
- `protection_sources` — названия фактически пересекающихся слоёв;
- `protection_intersections` — число записей пересечений;
- `protection_overlap_area_ha` — сумма площадей пересечений всех тематических слоёв.

Если выдел относится к нескольким типам, значения записываются через `; `. При совпадении одной категории в публичном и лесоустроительном слое итоговое поле не дублирует текст.

## Таблица пересечений

`piece_protection_links` сохраняет отдельную запись для каждого сочетания «выдел — защитный объект — исходный слой». Поля включают:

- номер и идентификатор выдела;
- защитный слой и тип;
- идентификатор защитного объекта;
- регистрационный номер материалов лесоустройства;
- площадь пересечения в м² и га;
- долю площади выдела в процентах.

Геометрия записи — фактическая геометрия пересечения. Площади вычисляются в равноплощадной проекции `EPSG:6933`.

`protection_overlap_area_ha` в таблице выделов может превышать площадь выдела, поскольку один участок одновременно учитывается в нескольких тематических слоях. Для недублирующего анализа площадей следует пользоваться `piece_protection_links` и разделять записи по `source_layer`.

## Порог пересечения

По умолчанию игнорируются пересечения меньше 1 м², чтобы не учитывать топологические щели и касания границ:

```bash
python fgislk_export.py --region 21 --forestry 21:1 --min-overlap-m2 10
```

## Результаты

Основной результат — `fgislk_<код региона>.gpkg`. В нём находятся четыре административно-лесоустроительных слоя, шесть защитных слоёв и `piece_protection_links`.

Также каждый слой записывается отдельно в GeoJSON и CSV. `attributes_cache.jsonl` позволяет продолжить прерванную загрузку подробных атрибутов выделов.
