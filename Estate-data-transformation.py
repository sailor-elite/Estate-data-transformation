# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # CONFIG

import polars as pl
import sqlite3
import tomllib
import glob
import numpy as np

from datetime import datetime, date
import os

import paramiko
from scp import SCPClient

with open("secrets.toml", "rb") as f:
    config = tomllib.load(f)
creds = config["mikrus"]

TABLES = ["offers", "offers_tytan", "offers_gralczyk", "offers_polnoc", "analyzed_offers" ]

pl.Config.set_tbl_rows(10)
pl.Config.STR_SET_ELIDED_BOUND = 500 
pl.Config.TT_CHOP_STR_BOUND = 500     
pl.Config.TABLE_WIDTH = 1000     
pl.Config.set_fmt_str_lengths(1000)

CITIES_GEO = {
    'ELŻBIECIN': {'lat': 53.1672, 'lon': 22.1231},
    'ZAMBRÓW': {'lat': 52.9856, 'lon': 22.2428},
    'STARE ZAKRZEWO': {'lat': 53.0042, 'lon': 22.2983},
    'PUPKI': {'lat': 53.2258, 'lon': 21.8497},
    'WYGODA': {'lat': 53.1022, 'lon': 22.1481},
    'STARE MODZELE': {'lat': 53.1114, 'lon': 22.3164},
    'NOWOGRÓD': {'lat': 53.2264, 'lon': 21.8806},
    'CZERWONE': {'lat': 53.4336, 'lon': 21.9044},
    'JANKI STARE': {'lat': 52.9419, 'lon': 22.4278},
    'GRĄDY': {'lat': 53.1158, 'lon': 22.1158},
    'CZACHY': {'lat': 53.0453, 'lon': 22.3456},
    'GROCHY': {'lat': 52.9461, 'lon': 22.3614},
    'DROZDOWO': {'lat': 53.1492, 'lon': 22.1644},
    'DĄBEK': {'lat': 53.0414, 'lon': 21.4647},
    'OSTROŁĘKA': {'lat': 53.0842, 'lon': 21.5731},
    'SIEMIEŃ NADRZECZNY': {'lat': 53.1978, 'lon': 22.1853},
    'SZLASY': {'lat': 53.0614, 'lon': 22.0647},
    'GRZYMAŁY SZCZEPANKOWSKIE': {'lat': 53.1436, 'lon': 21.9892},
    'CZERWIN': {'lat': 52.9239, 'lon': 21.6442},
    'FILOCHY': {'lat': 52.9481, 'lon': 21.7258},
    'KISIOŁKI': {'lat': 53.1667, 'lon': 22.3333},
    'BALIKI': {'lat': 53.2847, 'lon': 21.8594},
    'JANOWO': {'lat': 53.4864, 'lon': 21.9069},
    'SZUMOWO': {'lat': 52.9153, 'lon': 22.0736},
    'STARA ŁOMŻA PRZY SZOSIE': {'lat': 53.1603, 'lon': 22.0944},
    'NOWY CYDZYN': {'lat': 53.2753, 'lon': 22.1764},
    'STARE BOŻEJEWO': {'lat': 53.2383, 'lon': 22.3486},
    'DŁUGI KĄT': {'lat': 53.3308, 'lon': 21.7375},
    'ZAWADY': {'lat': 53.1539, 'lon': 22.6653},
    'MIASTKOWO': {'lat': 53.1489, 'lon': 21.8211},
    'KSIĘŻOPOLE': {'lat': 52.3306, 'lon': 22.2547},
    'JEDWABNE': {'lat': 53.2861, 'lon': 22.3019},
    'NOWE KUPISKI': {'lat': 53.1975, 'lon': 21.9844},
    'TRUSZKI': {'lat': 53.3236, 'lon': 22.1158},
    'ŚWIDRY': {'lat': 53.1561, 'lon': 21.9056},
    'SASINY': {'lat': 52.6186, 'lon': 22.9511},
    'WYSOKIE MAŁE': {'lat': 53.2681, 'lon': 22.1156},
    'WIZNA': {'lat': 53.1931, 'lon': 22.3831},
    'RUTKI': {'lat': 53.1000, 'lon': 22.4167},
    'SIEMNOCHA': {'lat': 53.2458, 'lon': 22.0833},
    'NAGÓRKI': {'lat': 53.1581, 'lon': 22.3333},
    'CZAPLICE': {'lat': 53.1114, 'lon': 22.1258},
    'MROCZKI': {'lat': 52.9983, 'lon': 22.3364},
    'JURZEC WŁOŚCIAŃSKI': {'lat': 53.3000, 'lon': 22.2167},
    'KOLNO': {'lat': 53.4131, 'lon': 21.9322},
    'STARE RATOWO': {'lat': 53.0114, 'lon': 21.8456},
    'KRUSZA': {'lat': 53.0781, 'lon': 21.5031},
    'GRODZISK DUŻY': {'lat': 52.4833, 'lon': 22.7500},
    'OJCEWO': {'lat': 53.3364, 'lon': 22.2542},
    'BACZE SUCHE': {'lat': 53.0564, 'lon': 22.1647},
    'GOSIE MAŁE': {'lat': 53.0244, 'lon': 22.4214},
    'JABŁONKA': {'lat': 53.5358, 'lon': 21.5061},
    'POPIOŁKI': {'lat': 53.3642, 'lon': 21.8597},
    'DRWĘCZ': {'lat': 53.0647, 'lon': 21.6642},
    'PODGÓRZE': {'lat': 53.1203, 'lon': 22.1236},
    'RZEKUŃ': {'lat': 53.0561, 'lon': 21.6167},
    'STARA ŁOMŻA NAD RZEKĄ': {'lat': 53.1656, 'lon': 22.1128},
    'SZCZEPANKOWO': {'lat': 53.1167, 'lon': 21.9833},
    'JARNUTY': {'lat': 53.1167, 'lon': 21.6833},
    'TUROŚL': {'lat': 53.3853, 'lon': 21.7258},
    'STARE KUPISKI': {'lat': 53.1972, 'lon': 21.9964},
    'KUPISKI STARE': {'lat': 53.1972, 'lon': 21.9964},
    'DŁUGOBÓRZ PIERWSZY': {'lat': 52.9667, 'lon': 22.2167},
    'WIKTORZYN': {'lat': 52.8833, 'lon': 21.7500},
    'KACZYNEK': {'lat': 53.0114, 'lon': 21.6831},
    'RYBAKI': {'lat': 53.1842, 'lon': 22.3164},
    'SAMBORY': {'lat': 53.2258, 'lon': 22.3853},
    'LELIS': {'lat': 53.1592, 'lon': 21.5644},
    'RUDKA': {'lat': 52.7333, 'lon': 22.7333},
    'KRAJEWO': {'lat': 52.9833, 'lon': 22.3333},
    'WYŁUDZIN': {'lat': 53.2458, 'lon': 21.7258},
    'LASKI SZLACHECKIE': {'lat': 52.9414, 'lon': 21.4647},
    'RYDZEWO': {'lat': 53.3086, 'lon': 22.0647},
    'SULĘCIN SZLACHECKI': {'lat': 52.8464, 'lon': 21.9167},
    'PIĄTNICA PODUCHOWNA': {'lat': 53.1894, 'lon': 22.0919},
    'BARTKI': {'lat': 53.3414, 'lon': 21.5258},
    'KORYTKI LEŚNE': {'lat': 53.1333, 'lon': 21.9500},
    'OSTROŻNE': {'lat': 52.9833, 'lon': 22.1167},
    'PROSIENICA': {'lat': 52.9167, 'lon': 21.9500},
    'STARY CYDZYN': {'lat': 53.2753, 'lon': 22.1856},
    'JURKI': {'lat': 53.3983, 'lon': 22.1114},
    'DOBRY LAS': {'lat': 53.3167, 'lon': 21.8333},
    'LACHOWO': {'lat': 53.4667, 'lon': 22.0167},
    'GOSTERY': {'lat': 53.0333, 'lon': 21.8667},
    'STARE KRAJEWO': {'lat': 52.9833, 'lon': 22.3500},
    'STARY LUBOTYŃ': {'lat': 52.9167, 'lon': 21.8833},
    'BORKOWO': {'lat': 53.2986, 'lon': 21.9142},
    'MOCARZE': {'lat': 53.2758, 'lon': 22.4258},
    'TABĘDZ': {'lat': 53.0333, 'lon': 22.1667},
    'JEZIORKO': {'lat': 53.2281, 'lon': 22.1456},
    'ŁAWY': {'lat': 53.0667, 'lon': 21.6167},
    'ŁOCHTYNOWO': {'lat': 53.1667, 'lon': 22.0333},
    'WOLA ZAMBROWSKA': {'lat': 52.9667, 'lon': 22.2500},
    'LASKOWIEC': {'lat': 53.0833, 'lon': 21.5333},
    'GIEŁCZYN': {'lat': 53.1114, 'lon': 22.0542},
    'PORYTE': {'lat': 53.3258, 'lon': 22.0831},
    'ŁOMŻA': {'lat': 53.1781, 'lon': 22.0592},
    'TROSZYN': {'lat': 53.0333, 'lon': 21.7333},
    'MOTYKA': {'lat': 53.1667, 'lon': 21.8833},
    'PIANKI': {'lat': 53.2167, 'lon': 22.0167},
    'KISIELNICA': {'lat': 53.2286, 'lon': 22.0542},
    'KONOPKI': {'lat': 53.0667, 'lon': 22.3167},
    'BURZYN': {'lat': 53.2758, 'lon': 22.4647},
    'KOSAKI': {'lat': 53.1667, 'lon': 22.2833},
    'KĄTY': {'lat': 53.3167, 'lon': 22.1667},
    'RATOWO': {'lat': 53.0167, 'lon': 21.8333},
    'PNIEWO': {'lat': 53.1167, 'lon': 22.0833},
    'KOŁAKI KOŚCIELNE': {'lat': 53.0167, 'lon': 22.3667},
    'BUDY CZARNOCKIE': {'lat': 53.2167, 'lon': 22.1500},
    'KOWNATY': {'lat': 53.2667, 'lon': 22.2167},
    'OSETNO': {'lat': 53.6833, 'lon': 19.8667},
    'MIKOŁAJKI': {'lat': 53.8014, 'lon': 21.5714},
    'KONARZYCE': {'lat': 53.1333, 'lon': 22.0500},
    'JEDNACZEWO': {'lat': 53.2081, 'lon': 21.9542},
    'PRZYTUŁY': {'lat': 53.3667, 'lon': 22.3167},
    'GOŁASZE': {'lat': 52.9833, 'lon': 22.5000},
    'KRZEWO': {'lat': 53.1500, 'lon': 22.3667},
    'STARY GROMADZYN': {'lat': 53.4000, 'lon': 21.9167},
    'KARWOWO': {'lat': 53.2667, 'lon': 22.3833},
    'OBRYTKI': {'lat':	53.3687798,'lon':22.2742335},
    'ZABAWKA': {'lat' :53.1995772, 'lon': 22.1571153},
    'SIEBURCZYN': 	{ 'lat' : 53.2414279, 'lon' : 22.4342496},
    'SZÓSTAKI' : { 'lat' :53.2833309, 'lon' : 22.4569495},
    'GÓRKI-SYPNIEWO' : { 'lat' : 53.273853, 'lon' : 22.1353291},
    'ŚNIADOWO': 	{'lat':53.0386233, 'lon':21.9902764},
    'SIESTRZANKI': { 'lat':53.306823, 'lon': 22.4139482},
    'BUDY MIKOłAJKA': { 'lat': 53.255353, 'lon' : 22.1711752},
    'MODZELE SKUDOSZE': { 'lat' : 53.0550814, 'lon': 22.1775928},
    'PIĄTNICA WŁOŚCIAŃSKA': { 'lat' :53.1859359, 'lon': 22.1050388},
    'RZADKOWO': {'lat':53.2219677, 'lon':22.143627},
    'PIĄTNICA' : {'lat':53.21542775, 'lon':22.177244553669112},
    'ZOSIN' : {'lat': 53.141755, 'lon': 22.1116481},
    'MORGOWNIKI': {'lat': 53.2275, 'lon': 21.8903},       
    'JANÓW': {'lat': 53.1558, 'lon': 22.1353},            
    'JĘCZNIKI': {'lat': 53.5358, 'lon': 20.9414},         
    'WYK': {'lat': 53.2758, 'lon': 21.9058},              
    'PTAKI': {'lat': 53.3058, 'lon': 21.8481},           
    'ZBÓJNA': {'lat': 53.2358, 'lon': 21.8153},          
    'BUDNE': {'lat': 53.3342, 'lon': 22.4286},            
    'NOWY KRZEW': {'lat': 53.1158, 'lon': 22.2542},       
    'KOZIOŁ': {'lat': 53.4258, 'lon': 21.8986},          
    'MIASTKOWO': {'lat': 53.1508, 'lon': 21.8386},        
    'DĘBOWO': {'lat': 53.6058, 'lon': 22.9253},           
    'BUDY KRANOCKIE': {'lat': 53.1958, 'lon': 22.1486},  
    'STARA ŁOMŻA': {'lat': 53.1658, 'lon': 22.0986},  
    'STAREJ ŁOMŻY': {'lat': 53.1658, 'lon': 22.0986},
    'SIEMIĘ NADRZECZNE': {'lat': 53.1858, 'lon': 22.0253}, 
    'WYRZYKI': {'lat': 53.1058, 'lon': 22.1553},
    'ROGIENICE WIELKIE': {'lat':53.1616, 'lon':22.044},
    'KOBYLIN': {'lat':53.293171789627955, 'lon': 22.14513394107225,}
}


# # HELPER FUNCTIONS

def download_db():
    now = datetime.now().strftime("%Y-%m-%d")
    folder_name = "db"
    
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    
    local_filename = f"{now}_olx.db"
    local_full_path = os.path.join(folder_name, local_filename)

    print(f"Connecting {creds['host']}...")
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(
            hostname=creds["host"],
            port=creds["port"],
            username=creds["user"],
            password=creds["password"]
        )
        
        with SCPClient(ssh.get_transport()) as scp:
            print(f"Pobieranie {creds['remote_path']} -> {local_full_path}")
            scp.get(creds["remote_path"], local_full_path)
            
        print(f"Database saved as: {local_full_path}")
        return local_full_path  
        
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        ssh.close()


def get_latest_db_path(folder="db"):
    files = glob.glob(os.path.join(folder, "*.db"))
    
    if not files:
        print("Not found file with extension 'db'!")
        return None
    
    latest_file = max(files)
    print(f"newest file: {latest_file}")
    return latest_file


def load_data_to_df(db_name: str, table_name: str) -> pl.DataFrame | None:
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        print(f"DB connected: {db_name}")

        query = f"SELECT * FROM {table_name}"
        df = pl.read_database(query, conn)

        print(f"DF loaded: {table_name}")
        print(f"DF shape: {df.shape}")
        return df

    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if conn:
            conn.close()
            print("DB connection closed.")


def simplify_text(text: str) -> str:
    trans = str.maketrans("ĄĆĘŁŃÓŚŹŻ", "ACELNOSZZ")
    return text.upper().translate(trans)


def add_haversine_distance(df, target_lat, target_lon):
    R = 6371.0
    lat1 = target_lat * np.pi / 180
    lon1 = target_lon * np.pi / 180

    return df.with_columns(
        (
            2 * R * (
                (
                    ((pl.col("LAT") * np.pi / 180 - lat1) / 2).sin()**2 +
                    pl.lit(np.cos(lat1)) * (pl.col("LAT") * np.pi / 180).cos() * ((pl.col("LON") * np.pi / 180 - lon1) / 2).sin()**2
                ).sqrt().arcsin()
            )
        ).cast(pl.Float64).alias("MAIN_CITY_DIST") 
    )


# # DOWNLOADING DATA

download_db()

DB_NAME = get_latest_db_path()


data = {}

for TABLE in TABLES:
    data[TABLE] = load_data_to_df(DB_NAME, TABLE)

data.keys()

# # Data processing

# ## OLX ("offers")

data["offers"].head()

data["offers"] = data["offers"].rename({"DATE_ISO":"DATE_ADDED"})

data["offers"] = data["offers"].with_columns(
    pl.col(["LAST_UPDATED", "DATE_ADDED"]).str.to_date(format="%Y-%m-%d")
            )

data["offers"] = data["offers"].with_columns(
    pl.col(["AREA_M2", "PRICE_M2"]).cast(pl.Float64)
)

data["offers"] = data["offers"].with_columns(
    pl.col("CITY").str.to_uppercase()
)

# +
geo_df = pl.DataFrame([
    {"CITY": k, "LAT": v["lat"], "LON": v["lon"]} 
    for k, v in CITIES_GEO.items()
])

data["offers"] = data["offers"].join(geo_df, on="CITY", how="left")
# -

print(data["offers"].null_count())

data["offers"] = data["offers"].with_columns(pl.col("DATE_ADDED").fill_null(pl.col("LAST_UPDATED"))
)

data["offers"] = data["offers"].with_columns(
    pl.col("DATE_ADDED").dt.strftime("%A").alias("DAY_NAME_EN")
)

data["offers"].head(5)

data["offers"].null_count()

data["offers"].filter(pl.col("LAT").is_null())["CITY"].unique().to_list()

# ## TYTAN

data["offers_tytan"].head(2)

data["offers_tytan"] = data["offers_tytan"].with_columns(
    pl.col(["LAST_UPDATED", "DATE_ADDED"]).str.to_date(format="%Y-%m-%d")
            )

data["offers_tytan"] = data["offers_tytan"].rename({"LOCATION":"CITY"})

data["offers_tytan"] = data["offers_tytan"].with_columns(
    pl.col("CITY").str.to_uppercase()
)

data["offers_tytan"] = data["offers_tytan"].with_columns(
    pl.col("DATE_ADDED").dt.strftime("%A").alias("DAY_NAME_EN")
)

# +
geo_df = pl.DataFrame([
    {"CITY": k, "LAT": v["lat"], "LON": v["lon"]} 
    for k, v in CITIES_GEO.items()
])

data["offers_tytan"] = data["offers_tytan"].join(geo_df, on="CITY", how="left")
# -

data["offers_tytan"].filter(pl.col("LAT").is_null())["CITY"].unique().to_list()

# # GRALCZYK

data["offers_gralczyk"] = data["offers_gralczyk"].rename({"FIRST_ADDED":"DATE_ADDED"})

data["offers_gralczyk"] = data["offers_gralczyk"].with_columns(
    pl.col(["LAST_UPDATED", "DATE_ADDED"]).str.to_date(format="%Y-%m-%d")
            )

data["offers_gralczyk"] = data["offers_gralczyk"].rename({"Cena":"PRICE"})
data["offers_gralczyk"] = data["offers_gralczyk"].rename({"Cena za m2":"PRICE_M2"})


data["offers_gralczyk"] = data["offers_gralczyk"].rename({"Powierzchnia":"AREA_M2"})


data["offers_gralczyk"] = data["offers_gralczyk"].with_columns(
    pl.col("AREA_M2")
    .str.replace("  m2", "")
    .str.replace(" ", "")
    .str.replace(",", ".")       
    .cast(pl.Float64)
)

data["offers_gralczyk"]

data["offers_gralczyk"] = data["offers_gralczyk"].with_columns(
    pl.col("ID")
    .str.replace_all("dzialka", "")
    .str.replace_all("-", " ")      
    .str.strip_chars()              
    .str.to_uppercase()             
    .alias("CITY")                  
)

data["offers_gralczyk"]

# +
data["offers_gralczyk"] = data["offers_gralczyk"].with_columns(
    pl.col("CITY")
    .str.to_uppercase()
    .str.replace_all("Ł", "L")
    .str.replace_all("Ó", "O")
    .str.replace_all("Ś", "S")
    .str.replace_all("Ż", "Z")
    .str.replace_all("Ź", "Z")
    .str.replace_all("Ć", "C")
    .str.replace_all("Ń", "N")
    .str.replace_all("Ą", "A")
    .str.replace_all("Ę", "E")
    .alias("SEARCH_CITY")
)

condition = pl.when(False).then(None)

for original_city in CITIES_GEO.keys():
    simple_city = simplify_text(original_city) 
    
    condition = condition.when(
        pl.col("SEARCH_CITY").str.contains(simple_city)
    ).then(pl.lit(original_city)) 

data["offers_gralczyk"] = data["offers_gralczyk"].with_columns(
    condition.alias("CLEAN_CITY")
)

data["offers_gralczyk"] = data["offers_gralczyk"].drop("SEARCH_CITY")
# -

data["offers_gralczyk"].filter(pl.col("CLEAN_CITY").is_null())["CITY"].unique().to_list()

data["offers_gralczyk"] = data["offers_gralczyk"].drop( ["CITY"])
data["offers_gralczyk"] = data["offers_gralczyk"].rename({"CLEAN_CITY":"CITY"})

data["offers_gralczyk"] = data["offers_gralczyk"].join(geo_df, on="CITY", how="left")

data["offers_gralczyk"].head(2)

# # POLNOC

data["offers_polnoc"] = data["offers_polnoc"].rename({"FIRST_ADDED":"DATE_ADDED"})

data["offers_polnoc"] = data["offers_polnoc"].with_columns(
    pl.col(["LAST_UPDATED", "DATE_ADDED"]).str.to_date(format="%Y-%m-%d")
            )

data["offers_polnoc"] = data["offers_polnoc"].rename({"Cena":"PRICE"})
data["offers_polnoc"] = data["offers_polnoc"].rename({"Cena za m2":"PRICE_M2"})
data["offers_polnoc"] = data["offers_polnoc"].rename({"Powierzchnia działki":"AREA_M2"})


data["offers_polnoc"] = data["offers_polnoc"].filter(
    (pl.col("AREA_M2").is_not_null()) & 
    (pl.col("ID").str.contains("dzialka"))
)

data["offers_polnoc"]

data["offers_polnoc"] = data["offers_polnoc"].with_columns(
    pl.col("ID")
    .str.replace_all("dzialka", "")
    .str.replace_all("-", " ")      
    .str.strip_chars()              
    .str.to_uppercase()             
    .alias("CITY")                  
)

data["offers_polnoc"]

data["offers_polnoc"] = data["offers_polnoc"].with_columns(
    pl.col(["AREA_M2", "PRICE_M2", "PRICE"]).cast(pl.Float64)
)

# +
data["offers_polnoc"] = data["offers_polnoc"].with_columns(
    pl.col("CITY")
    .str.to_uppercase()
    .str.replace_all("Ł", "L")
    .str.replace_all("Ó", "O")
    .str.replace_all("Ś", "S")
    .str.replace_all("Ż", "Z")
    .str.replace_all("Ź", "Z")
    .str.replace_all("Ć", "C")
    .str.replace_all("Ń", "N")
    .str.replace_all("Ą", "A")
    .str.replace_all("Ę", "E")
    .alias("SEARCH_CITY")
)

condition = pl.when(False).then(None)

for original_city in CITIES_GEO.keys():
    simple_city = simplify_text(original_city) 
    
    condition = condition.when(
        pl.col("SEARCH_CITY").str.contains(simple_city)
    ).then(pl.lit(original_city)) 

data["offers_polnoc"] = data["offers_polnoc"].with_columns(
    condition.alias("CLEAN_CITY")
)

data["offers_polnoc"] = data["offers_polnoc"].drop("SEARCH_CITY")
# -

data["offers_polnoc"].filter(pl.col("CLEAN_CITY").is_null())["CITY"].unique().to_list()

data["offers_polnoc"] = data["offers_polnoc"].drop( ["CITY"])
data["offers_polnoc"] = data["offers_polnoc"].rename({"CLEAN_CITY":"CITY"})
data["offers_polnoc"] = data["offers_polnoc"].join(geo_df, on="CITY", how="left")

data["offers_polnoc"]

# # 4LOMZA

data["analyzed_offers"] = data["analyzed_offers"].filter((pl.col("ESTATE_TYPE").str.contains("DZIAŁKA BUDOWLANA"))&
                               (pl.col("OFFER_TYPE").str.contains("SPRZEDAZ")))

data["analyzed_offers"] = data["analyzed_offers"].unique(subset=["ID"])

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col(["LAST_UPDATED", "DATE_ADDED"]).str.to_date(format="%Y-%m-%d")
            )

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col([ "ANNOUNCE_DATE"]).str.to_date(format="%Y-%m-%d")
            )

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col("AREA_M2")
    .str.replace(" m2", "")
    .str.replace(",", ".")
    .cast(pl.Float64, strict=False)  
)

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col("PRICE")
    .str.replace(" m2", "")
    .str.replace(",", ".")
    .cast(pl.Float64, strict=False)  
)

data["analyzed_offers"]

# +
stats = data["analyzed_offers"].select([
    pl.col("AREA_M2").quantile(0.25).alias("q1"),
    pl.col("AREA_M2").quantile(0.75).alias("q3")
])

q1 = stats[0, "q1"]
q3 = stats[0, "q3"]
iqr = q3 - q1

lower_bound = q1 - 1.5 * iqr
upper_bound = q3 + 1.5 * iqr

print(f"Statystyczne granice dla AREA_M2: {lower_bound:.2f} - {upper_bound:.2f}")

outliery_statystyczne = data["analyzed_offers"].filter(
    (pl.col("AREA_M2") < lower_bound) | (pl.col("AREA_M2") > upper_bound)
)
# -

corrected_outliers = outliery_statystyczne.with_columns(
    pl.when(
        (pl.col("TEXT").str.contains(f"(?i)ar")) | 
        (pl.col("TEXT").str.contains(f"(?i)ara"))   
    )
    .then(pl.col("AREA_M2") / 10)  
    .otherwise(pl.col("AREA_M2"))
    .alias("AREA_M2_CORRECTED")
).select(["ID", "AREA_M2_CORRECTED"])

# +
data["analyzed_offers"] = data["analyzed_offers"].join(
    corrected_outliers, on="ID", how="left"
)

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col("AREA_M2_CORRECTED").fill_null(pl.col("AREA_M2")).alias("AREA_M2")
).drop("AREA_M2_CORRECTED")
# -

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    (pl.col("PRICE") / pl.col("AREA_M2")).round(2).alias("PRICE_M2")
)

data["analyzed_offers"] = data["analyzed_offers"].rename({"LOCATION":"CITY"})

# +
data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.col("CITY")
    .str.to_uppercase()
    .str.replace_all("Ł", "L")
    .str.replace_all("Ó", "O")
    .str.replace_all("Ś", "S")
    .str.replace_all("Ż", "Z")
    .str.replace_all("Ź", "Z")
    .str.replace_all("Ć", "C")
    .str.replace_all("Ń", "N")
    .str.replace_all("Ą", "A")
    .str.replace_all("Ę", "E")
    .alias("SEARCH_CITY")
)

condition = pl.when(False).then(None)

for original_city in CITIES_GEO.keys():
    simple_city = simplify_text(original_city) 
    
    condition = condition.when(
        pl.col("SEARCH_CITY").str.contains(simple_city)
    ).then(pl.lit(original_city)) 

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    condition.alias("CLEAN_CITY")
)

data["analyzed_offers"] = data["analyzed_offers"].drop("SEARCH_CITY")
# -

data["analyzed_offers"].filter(pl.col("CLEAN_CITY").is_null())["CITY"].unique().to_list()

data["analyzed_offers"] = data["analyzed_offers"].with_columns(
    pl.when(pl.col("CITY").str.to_uppercase() == "BRAK")
    .then(None) 
    .when(pl.col("CLEAN_CITY").is_null()) 
    .then(pl.lit("ŁOMŻA")) 
    .otherwise(pl.col("CLEAN_CITY")) 
    .alias("CLEAN_CITY")
)

data["analyzed_offers"] = data["analyzed_offers"].drop( ["CITY"])
data["analyzed_offers"] = data["analyzed_offers"].rename({"CLEAN_CITY":"CITY"})
data["analyzed_offers"] = data["analyzed_offers"].join(geo_df, on="CITY", how="left")

data.keys()

# # Concating all datasets

for key in data.keys():
    print (key, data[key].columns)

target_cols = ["ID", "DATE_ADDED", "LAST_UPDATED", "AREA_M2", "PRICE_M2", "CITY", "LAT", "LON", "PRICE"]

# +
processed_dfs = []

for key, df in data.items():
    temp_df = df.rename({"VALUE": "PRICE"}) if "VALUE" in df.columns else df
    temp_df = temp_df.select(target_cols).with_columns([
        pl.col("ID").cast(pl.String),        
        pl.col("PRICE").cast(pl.Float64),  
        pl.col("AREA_M2").cast(pl.Float64),
        pl.col("PRICE_M2").cast(pl.Float64),
        pl.col("LAT").cast(pl.Float64),
        pl.col("LON").cast(pl.Float64),
        pl.lit(key).alias("SOURCE")
    ])
    processed_dfs.append(temp_df)

data["Offers_all"] = pl.concat(processed_dfs)
# -

data["Offers_all"] = data["Offers_all"].with_columns([
    pl.when(pl.col("PRICE") < 15_000)
    .then(None)
    .otherwise(pl.col("PRICE"))
    .alias("PRICE"),
]).with_columns([
    pl.when(pl.col("PRICE").is_null())
    .then(None)
    .otherwise(pl.col("PRICE_M2"))
    .alias("PRICE_M2")
])

data["Offers_all"]

# ## Day of the week mapping

week_days = {
    1: "Poniedziałek",
    2: "Wtorek",
    3: "Środa",
    4: "Czwartek",
    5: "Piątek",
    6: "Sobota",
    7: "Niedziela"
}

data["Offers_all"] = data["Offers_all"].with_columns([
    pl.col("DATE_ADDED").cast(pl.Date),
    pl.col("DATE_ADDED")
    .dt.weekday()
    .cast(pl.String) 
    .replace(week_days)
    .alias("DAY_NAME_PL")
])

# ## Cleaning AREA_M2

data["Offers_all"] = data["Offers_all"].with_columns(
    pl.when(pl.col("AREA_M2") <= 1.0)
    .then(None)
    .otherwise(pl.col("AREA_M2"))
    .alias("AREA_M2")
)

data["Offers_all"] = data["Offers_all"].with_columns([
    pl.when(pl.col("AREA_M2") < 100.0)
    .then(pl.col("AREA_M2") * 100.0)
    .otherwise(pl.col("AREA_M2"))
    .alias("AREA_M2")
]).with_columns([
    pl.when(pl.col("AREA_M2") < 100.0)
    .then(None)
    .otherwise(pl.col("AREA_M2"))
    .alias("AREA_M2")
]).with_columns([
    pl.when((pl.col("PRICE").is_not_null()) & (pl.col("AREA_M2").is_not_null()))
    .then(pl.col("PRICE") / pl.col("AREA_M2"))
    .otherwise(None)
    .alias("PRICE_M2")
])
print(data["Offers_all"].filter(pl.col("ID").str.contains("MORGOWNIKI|NOWOGRÓD|Zawady_10")).select([
    "ID", "AREA_M2", "PRICE", "PRICE_M2"
]))

# +
data["Offers_all"] = data["Offers_all"].with_columns(
    pl.when(pl.col("ID") == "dzialka-jedwabne-lomzynska")
    .then(pl.lit(10200.0))
    .otherwise(pl.col("AREA_M2"))
    .alias("AREA_M2")
)

data["Offers_all"] = data["Offers_all"].with_columns(
    pl.when(pl.col("ID") == "dzialka-jedwabne-lomzynska")
    .then(pl.col("PRICE") / pl.col("AREA_M2"))
    .otherwise(pl.col("PRICE_M2"))
    .alias("PRICE_M2")
)
# -

data["Offers_all"]

# ## Measuring distance from main_city

LOMZA_LAT = 53.1781
LOMZA_LON = 22.0592
data["Offers_all"] = add_haversine_distance(data["Offers_all"], LOMZA_LAT, LOMZA_LON)

data["Offers_all"]

# ## Size segments

# +
size_bins = [700.0, 1000.0, 1500.0, 5000.0, 10000.0]

size_labels = [
    "Tiny/Sub-standard",    
    "Small Plot",           
    "Medium Plot",         
    "Large Plot",           
    "Investment/Small Farm",
    "Hectares/Agriculture" 
]

data["Offers_all"] = data["Offers_all"].with_columns(
    pl.col("AREA_M2").cut(
        size_bins, 
        labels=size_labels
    ).alias("SIZE_SEGMENT")
)
# -

# ## Days on market

data["Offers_all"] = data["Offers_all"].with_columns([
    pl.when(pl.col("DATE_ADDED") > pl.col("LAST_UPDATED"))
    .then(pl.col("LAST_UPDATED"))
    .otherwise(pl.col("DATE_ADDED"))
    .alias("DATE_ADDED"),

    pl.when(pl.col("DATE_ADDED") > pl.col("LAST_UPDATED"))
    .then(pl.col("DATE_ADDED"))
    .otherwise(pl.col("LAST_UPDATED"))
    .alias("LAST_UPDATED")
])

# +
data["Offers_all"] = data["Offers_all"].with_columns(
    (pl.col("LAST_UPDATED").cast(pl.Date) - pl.col("DATE_ADDED").cast(pl.Date))
    .dt.total_days()
    .alias("DAYS_ON_MARKET")
)

data["Offers_all"] = data["Offers_all"].with_columns(
    pl.when(pl.col("DAYS_ON_MARKET") <= 7)
    .then(pl.lit("New Offer"))
    .when(pl.col("DAYS_ON_MARKET") <= 30)
    .then(pl.lit("Active"))
    .when(pl.col("DAYS_ON_MARKET") <= 90)
    .then(pl.lit("Stale"))
    .otherwise(pl.lit("Old/Negotiable"))
    .alias("MARKET_STATUS")
)
# -

data["Offers_all"]

# ## Price time series

data["Offers_daily"] = data["Offers_all"].with_columns(
    pl.date_ranges(
        start=pl.col("DATE_ADDED"),
        end=pl.col("LAST_UPDATED"),
        interval="1d",
        eager=False
    ).alias("ACTIVE_DAY")
)

data["Time_series"] = data["Offers_daily"].explode("ACTIVE_DAY")

data["Time_series_stats"] = (
    data["Time_series"]
    .group_by("ACTIVE_DAY")
    .agg([
        pl.count("ID").alias("TOTAL_ACTIVE_OFFERS"),
        
        pl.col("PRICE_M2").mean().round(2).alias("AVG_PRICE_M2"),
        pl.col("PRICE_M2").median().round(2).alias("MEDIAN_PRICE_M2"),
        pl.col("PRICE").median().round(2).alias("MEDIAN_PRICE"),
        pl.col("PRICE").mean().round(2).alias("AVG_PRICE"),
        pl.col("AREA_M2").mean().round(0).alias("AVG_AREA_M2"),
        pl.col("AREA_M2").sum().round(0).alias("TOTAL_AREA_ON_MARKET"),
        pl.col("AREA_M2").median().round(0).alias("MEDIAN_AREA_M2")
    ])
    .sort("ACTIVE_DAY")
)

start_date = data["Offers_all"]["LAST_UPDATED"].min()
print(start_date)

data["Time_series_stats"] = data["Time_series_stats"].filter(pl.col("ACTIVE_DAY") >= start_date)

data["Time_series_stats"]

# # Anonymizing source

source_mapping = {
    "offers_polnoc": "Website A",
    "analyzed_offers": "Website B",
    "offers_tytan": "Website C",
    "offers_gralczyk": "Website D",
    "offers": "Website E"
}

data["Offers_all"] = data["Offers_all"].with_columns(
    pl.col("SOURCE")  
    .replace(source_mapping, default="Other")
    .alias("SOURCE")
)

data["Offers_all"]

# ## Saving dataframes

today = date.today().isoformat()

data["Offers_all"].write_parquet(f"{today}_offers_cleaned_final.parquet")
data["Time_series_stats"].write_parquet(f"{today}_time_series_pbi.parquet")


