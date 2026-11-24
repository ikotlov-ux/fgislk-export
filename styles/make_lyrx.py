"""Генерация стилей ArcGIS Pro (.lyrx) для слоя taxation_pieces по полю tree_species.
Цвета: ЛесИС, «RGB цвета плана лесонасаждений», Инструкция 1999
https://lesis.ru/infot/spr_ins_kart1.html
Тона: молодняки, средневозрастные, приспевающие, спелые и перестойные."""
import json
P = {  # группа: (подпись, [4 тона], значения tree_species)
 "pine":  ("Сосна",[(248,176,152),(248,144,96),(248,104,48),(248,64,0)],["Сосна"]),
 "spruce":("Ель",[(232,160,224),(208,96,200),(160,16,176),(128,16,136)],["Ель","Ель колючая (голубая)"]),
 "fir":   ("Пихта",[(216,208,240),(184,168,232),(128,80,208),(64,32,136)],["Пихта","Пихта сибирская"]),
 "cedar": ("Кедр",[(240,136,136),(248,88,88),(248,32,32),(224,0,0)],["Кедр"]),
 "larch": ("Лиственница",[(216,168,168),(176,104,104),(152,64,64),(128,0,0)],
           ["Лиственница","Лиственница сибирская","Лиственница европейская"]),
 "oak":   ("Дуб",[(176,168,168),(128,128,128),(104,96,96),(72,72,72)],["Дуб","Дуб низкоствольный","Дуб красный"]),
 "ash":   ("Ясень",[(168,192,216),(128,160,192),(64,104,136),(0,0,128)],["Ясень"]),
 "maple": ("Клен",[(232,200,200),(208,144,160),(184,96,120),(184,32,40)],
           ["Клен остролистный","Клен","Клен ясенелистный"]),
 "elm":   ("Вяз",[(248,176,104),(224,112,0),(184,88,0),(128,64,0)],
           ["Вяз (ильм)","Вяз (ильм) шершавый (ильм горный)"]),
 "birch": ("Береза",[(176,216,248),(128,200,248),(80,184,248),(24,144,248)],["Береза","Береза камчатская"]),
 "aspen": ("Осина, тополь",[(184,216,200),(136,200,160),(80,168,120),(0,136,80)],["Осина","Тополь"]),
 "galder":("Ольха серая",[(208,248,160),(168,240,72),(120,200,0),(80,136,0)],["Ольха серая (белая)"]),
 "balder":("Ольха черная",[(232,216,240),(200,176,216),(152,96,192),(112,32,168)],["Ольха черная (клейкая)"]),
 "linden":("Липа",[(248,248,208),(240,232,144),(248,248,88),(248,248,0)],["Липа"]),
 "willow":("Ива",[(248,208,232),(248,160,208),(248,88,168),(248,48,152)],
           ["Ива древовидная","Ива белая (дерево)","Ива козья"]),
 "rowan": ("Рябина",[(248,152,144),(248,96,88),(232,16,8),(184,16,0)],["Рябина"]),
 "apple": ("Яблоня",[(248,208,200),(248,152,136),(248,96,56),(240,48,8)],["Яблоня"]),
}
# Каштана в таблице ЛесИС нет — нейтральный коричневый
P["chestnut"]=("Каштан (нет в таблице ЛесИС)",[(222,200,170),(196,164,124),(160,124,84),(120,88,56)],["Каштан"])
AGES=[("молодняки",["молодняки I класса возраста","молодняки II класса возраста"]),
      ("средневозрастные",["средневозрастные"]),
      ("приспевающие",["приспевающие","Средневозрастные и приспевающие"]),
      ("спелые и перестойные",["спелые","перестойные"])]
NODATA=["<Null>",""]
GPKG=r"D:\Yandex.Disk\MOS_OBL\FGISLK\fgislk_50_all.gpkg"

def color(rgb,a=100): return {"type":"CIMRGBColor","values":[*rgb,a]}
def sym(rgb,outline=(110,110,110),w=0.2,fill_alpha=100):
    return {"type":"CIMSymbolReference","symbol":{"type":"CIMPolygonSymbol","symbolLayers":[
        {"type":"CIMSolidStroke","enable":True,"capStyle":"Round","joinStyle":"Round","lineStyle3D":"Strip",
         "miterLimit":10,"width":w,"color":color(outline)},
        {"type":"CIMSolidFill","enable":True,"color":color(rgb,fill_alpha)}]}}
def cls(label,rgb,values):
    return {"type":"CIMUniqueValueClass","label":label,"patch":"Default","symbol":sym(rgb),"visible":True,
            "values":[{"type":"CIMUniqueValue","fieldValues":v if isinstance(v,list) else [v]} for v in values]}

def doc(name,fields,classes,heading):
    renderer={"type":"CIMUniqueValueRenderer","colorRamp":None,
      "defaultLabel":"Прочие / нет данных","defaultSymbol":sym((255,255,255),(150,150,150)),
      "defaultSymbolPatch":"Default","useDefaultSymbol":True,"fields":fields,"fieldDelimiter":", ",
      "groups":[{"type":"CIMUniqueValueGroup","heading":heading,"classes":classes}],
      "polygonSymbolColorTarget":"Fill"}
    layer={"type":"CIMFeatureLayer","name":name,"uRI":"CIMPATH=map/taxation_pieces.json",
      "sourceModifiedTime":{"type":"TimeInstant"},"useSourceMetadata":True,"description":name,
      "layerType":"Operational","showLegends":True,"visibility":True,"displayCacheType":"Permanent",
      "maxDisplayCacheAge":5,"showPopups":True,"serviceLayerID":-1,"refreshRate":-1,"refreshRateUnit":"esriTimeUnitsSeconds",
      "autoGenerateFeatureTemplates":True,
      "featureTable":{"type":"CIMFeatureTable","displayField":"tree_species","editable":True,
        "dataConnection":{"type":"CIMStandardDataConnection",
          "workspaceConnectionString":"AUTHENTICATION_MODE=OSA;DATABASE="+GPKG,
          "workspaceFactory":"Sql","dataset":"main.taxation_pieces","datasetType":"esriDTFeatureClass"},
        "studyAreaSpatialRel":"esriSpatialRelUndefined","searchOrder":"esriSearchOrderSpatial"},
      "htmlPopupEnabled":True,"selectable":True,"featureCacheType":"Session",
      "renderer":renderer,"scaleSymbols":True,"snappable":True}
    return {"type":"CIMLayerDocument","version":"3.1.0","build":41833,
            "layers":["CIMPATH=map/taxation_pieces.json"],"layerDefinitions":[layer]}

# 1) только порода: тон «приспевающие» (3-й)
c1=[cls(lab,tones[2],vals) for lab,tones,vals in P.values()]
c1.append(cls("Порода не указана",(235,235,235),NODATA))
json.dump(doc("Выделы — преобладающая порода",["tree_species"],c1,"tree_species"),
          open("fgislk_tree_species.lyrx","w",encoding="utf-8"),ensure_ascii=False,indent=1)

# 2) порода × группа возраста: 4 тона
c2=[]
for lab,tones,vals in P.values():
    for (alab,avals),rgb in zip(AGES,tones):
        c2.append(cls(f"{lab} — {alab}",rgb,[[v,a] for v in vals for a in avals]))
    c2.append(cls(f"{lab} — возраст не указан",tones[1],[[v,a] for v in vals for a in NODATA+
        ["Не установлена (деревья с диаметром 6 см и более отсутствуют)"]]))
c2.append(cls("Порода не указана",(235,235,235),[[s,a] for s in NODATA for a in
    NODATA+[x for _,v in AGES for x in v]+["Не установлена (деревья с диаметром 6 см и более отсутствуют)"]]))
json.dump(doc("Выделы — порода и группа возраста",["tree_species","age_group"],c2,"tree_species, age_group"),
          open("fgislk_tree_species_age.lyrx","w",encoding="utf-8"),ensure_ascii=False,indent=1)
print(len(c1),len(c2))
