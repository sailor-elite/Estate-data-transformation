# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Analiza rynku nieruchomości działek budowlanych w okolicach Łomży (English version below)
#
#
# ## Opis problemu
# Rynek nieruchomości w Polsce nieprzerwanie od kilkunastu lat jest rynkiem niesprzyjającym grupie inwestorów z niskim kapitałem, do których bardzo często zaliczają się osoby w przedziale wiekowym 20-30. Znalezienie okazji inwestycyjnej wymaga ciągłego monitoringu ogłoszeń. Głównym wyzwaniem jest brak ujednolicenia danych z różnych źródeł oraz obecność błędów w nazewnictwie lokalizacji, co utrudnia rzetelną analizę trendów cenowych i dostępności ofert.
#
# ## Opis dostępu do danych i sposób ich pozyskania
# Proces pozyskiwania danych jest w pełni zautomatyzowany i opiera się na architekturze rozproszonej:
# 1.  VPS - Skrypt typu Scrapper uruchamiany jest cyklicznie przez harmonogram Cron. Pobrane surowe dane trafiają do plikowej bazy danych.
# 2.  Przetwarzanie danych przy pomocy Bielik LLM - Dane są przesyłane protokołem SCP do środowiska, gdzie model językowy Bielik-1.5B-v3 dokonuje unifikacji i czyszczenia danych. (https://github.com/sailor-elite/building-plots-bielik-processor)
# 3.  Final Processing - Przetworzone dane wracają na VPS (Processed data), skąd są pobierane do końcowej analizy w formacie Parquet. (https://github.com/sailor-elite/Estate-data-transformation/blob/master/Estate-data-transformation.ipynb)
# 4.  Końcowym odbiorcą danych są użytkownicy raportu Power BI, nieumieszczonego w niniejszym Notebook'u.
#
# ### Schemat procesu
# ![graf procesu](data/estate-scrapper-process.drawio.png)
#
# ## Opis danych i znaczenie atrybutów
# Zbiór danych składa się z 757 rekordów opisanych przez 15 atrybutów:
# | Atrybut          | Typ       | Znaczenie                                                                 |
# |------------------|-----------|---------------------------------------------------------------------------|
# | ID               | str       | Unikalny identyfikator oferty.                                            |
# | DATE_ADDED       | object    | Data dodania oferty do systemu.                                           |
# | LAST_UPDATED     | object    | Data ostatniej aktualizacji oferty.                                       |
# | AREA_M2          | float64   | Powierzchnia nieruchomości w metrach kwadratowych.                        |
# | PRICE_M2         | float64   | Cena za 1 metr kwadratowy.                                                |
# | CITY             | str       | Miejscowość.            |
# | LAT / LON        | float64   | Współrzędne geograficzne nieruchomości.                                   |
# | PRICE            | float64   | Całkowita cena ofertowa.                                                  |
# | SOURCE           | str       | Portal, z którego pochodzi ogłoszenie.                                    |
# | DAY_NAME_PL      | str       | Nazwa dnia tygodnia dodania oferty.                |
# | MAIN_CITY_DIST   | float64   | Odległość od głównego centrum (miasta Łomża).                                |
# | SIZE_SEGMENT     | category  | Podział nieruchomości na segmenty wielkościowe.       |
# | DAYS_ON_MARKET   | int64     | Liczba dni, przez które oferta jest aktywna.                              |
# | MARKET_STATUS    | str       | Status oferty                                                             |
#
# ## Definicja problemu: Klasyfikacja / Regresja
#
# Projekt de facto realizuje oba podejścia. W zbiorze danych zawarty jest atrybut ("SIZE_SEGMENT"), który zawiera informację o segmencie wielkościowym. Oprócz tego, w niniejszym notatniku zostanie przedstawione podejście do klasyfikacji atrakcyjności cenowej danej oferty oraz przy wykorzystaniu regresji, przewidywanie ceny nieruchomości na podstawie atrybutów.
#
# ## Znaczenie problemu
# Analiza pozwala na udzielenie odpowiedzi na następujące pytania biznesowe:
#
# - Kiedy przeglądać oferty? 
# - Za jaką cenę w danej lokalizacji warto kupować?
# - Jaka jest podaż - określenie nasycenia rynku w konkretnych segmentach wielkościowych.

# %% [markdown] jp-MarkdownHeadingCollapsed=true
# # Analysis of the real estate market for building plots in the vicinity of Łomża
#
# ## Problem description
#
# The real estate market in Poland has continuously, for over a dozen years, been unfavorable for investors with low capital, who very often include individuals aged 20–30. Finding an investment opportunity requires constant monitoring of listings. The main challenge is the lack of data standardization from different sources, as well as the presence of errors in location naming, which makes reliable analysis of price trends and offer availability difficult.
#
# ## Description of data access and acquisition method
#
# The data acquisition process is fully automated and based on a distributed architecture:
#
# 1. VPS – A scraper script is executed cyclically via a Cron scheduler. The collected raw data is stored in a file-based database.
#
# 2. Data processing using Bielik LLM – The data is transferred via the SCP protocol to an environment where the Bielik-1.5B-v3 language model performs data unification and cleaning. (https://github.com/sailor-elite/building-plots-bielik-processor)
#
# 3. Final Processing – The processed data is sent back to the VPS (Processed data), from where it is retrieved for final analysis in Parquet format. (https://github.com/sailor-elite/Estate-data-transformation/blob/master/Estate-data-transformation.ipynb)
#
# 4. The final recipients of the data are users of a Power BI report, which is not included in this Notebook.
#
# ### Process diagram
#
# ![process diagram](data/estate-scrapper-process.drawio.png)
#
# ## Data description and attribute meanings
#
# The dataset consists of 757 records described by 15 attributes:
#
# | Attribute        | Type      | Description                                                               |
# |------------------|-----------|---------------------------------------------------------------------------|
# | ID               | str       | Unique identifier of the listing.                                         |
# | DATE_ADDED       | object    | Date when the listing was added to the system.                            |
# | LAST_UPDATED     | object    | Date of the last update of the listing.                                   |
# | AREA_M2          | float64   | Property area in square meters.                                           |
# | PRICE_M2         | float64   | Price per square meter.                                                   |
# | CITY             | str       | Locality.                                                                 |
# | LAT / LON        | float64   | Geographic coordinates of the property.                                   |
# | PRICE            | float64   | Total listing price.                                                      |
# | SOURCE           | str       | Portal from which the listing originates.                                 |
# | DAY_NAME_PL      | str       | Name of the weekday when the listing was added.                           |
# | MAIN_CITY_DIST   | float64   | Distance from the main center (city of Łomża).                            |
# | SIZE_SEGMENT     | category  | Division of properties into size segments.                                |
# | DAYS_ON_MARKET   | int64     | Number of days the listing is active.                                     |
# | MARKET_STATUS    | str       | Listing status                                                            |
#
# ## Problem definition: Classification / Regression
#
# The project effectively implements both approaches. The dataset contains an attribute ("SIZE_SEGMENT") that includes information about the size segment. Additionally, this notebook presents an approach to classifying the price attractiveness of a given listing, as well as using regression to predict property prices based on attributes.
#
# ## Importance of the problem
#
# The analysis allows answering the following business questions:
#
# - When should listings be browsed?
#
# - At what price is it worth buying in a given location?
#
# - What is the supply – determining market saturation in specific size segments.

# %% [markdown]
# # Config

# %%
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import Point

# %% [markdown]
# # Data loading

# %%
data = {}

# %%
data["offers"] = pd.read_parquet("2026-03-29_offers_cleaned_final.parquet")

# %%
data["offers"].info()

# %% [markdown]
# # EDA

# %% [markdown]
# ## When are the most offers added?

# %%
data["offers"]["DAY_NAME_PL"].value_counts()

# %%
sort_order = ["Poniedziałek", "Wtorek", "Środa", "Czwartek", "Piątek", "Sobota", "Niedziela"]
ax = (data["offers"]["DAY_NAME_PL"]
      .value_counts()
      .reindex(sort_order)
      .plot(kind="bar", figsize=(4, 4), color="#4C72B0", edgecolor="black"))


# %% [markdown]
# Najwięcej ofert nieruchomości jest dodawanych w sobotę (141 ofert) oraz we wtorek (120 ofert). Wysoka aktywność w sobotę i niedzielę (łącznie 250 ofert) sugeruje, że spora część rynku to ogłoszenia prywatne, przygotowywane przez właścicieli w czasie wolnym. Z punktu widzenia inwestora, najgorętszym okresem na przeglądanie nowych ogłoszeń jest weekend oraz początek weekendu (piątek).

# %% [markdown]
# The highest number of real estate offers is added on Saturday (141 offers) and Tuesday (120 offers). The high activity observed on Saturday and Sunday (totaling 250 offers) suggests that a significant portion of the market consists of private listings prepared by owners during their leisure time. From an investor's perspective, the "hottest" period for browsing new listings is the weekend and its onset (Friday), allowing for a quick response and scheduling plot viewings as early as Saturday afternoon.

# %% [markdown]
# ## What is the size segment of the offered plot?

# %%
data["offers"]["SIZE_SEGMENT"].value_counts()

# %%
ax = (data["offers"]["SIZE_SEGMENT"]
      .value_counts()
      .plot(kind="bar", figsize=(4, 4), color="#4C72B0", edgecolor="black"))

# %% [markdown]
# | Segment                    | Zakres powierzchni/ Area (m²) | Liczba ofert / Number of offers |
# |----------------------------|--------------------------|--------------|
# | Tiny / Sub-standard        | < 700                    | 53           |
# | Small Plot                 | 700 – 1000               | 165          |
# | Medium Plot                | 1000 – 1500              | 189          |
# | Large Plot                 | 1500 – 5000              | 107          |
# | Investment / Small Farm    | 5000 – 10000             | 33           |
# | Hectares / Agriculture     | > 10000                  | 106          |

# %% [markdown]
# Analiza struktury wielkości działek wykazuje dominację gruntów o średniej i małej powierzchni. Najliczniejszą grupę stanowią Medium Plots (189 ofert) o metrażu między 1500 $m^2$ a 5000 $m^2$ oraz Small Plots (165 ofert) mieszczące się w przedziale 1000–1500 $m^2$. Jest to typowa charakterystyka rynku podmiejskiego, nastawionego na budownictwo jednorodzinne. Znaczący udział mają również działki o charakterze rolnym/wielkopowierzchniowym (106 ofert powyżej 1 ha), co wskazuje na dużą dostępność gruntów inwestycyjnych poza ścisłą zabudową. Najmniejszą grupę stanowią działki poniżej 700 $m^2$ (Tiny/Sub-standard).

# %% [markdown]
# The analysis of the plot size structure shows a dominance of medium and small-sized lands. The most numerous groups are Medium Plots (189 offers) with an area between 1,500 $m^2$ and 5,000 $m^2$, and Small Plots (165 offers) ranging from 1,000 to 1,500 $m^2$. This is a typical characteristic of a suburban market focused on single-family housing. There is also a significant share of agricultural/large-scale plots (106 offers over 1 ha), indicating high availability of investment land outside densely built-up areas. The smallest group consists of plots under 700 $m^2$ (Tiny/Sub-standard).

# %% [markdown]
# ## Where are the most offers?

# %%
data["offers"]["CITY"].value_counts().nlargest(20)

# %%
ax = (data["offers"]["CITY"].value_counts().nlargest(20)
      .plot(kind="bar", figsize=(4, 4), color="#4C72B0", edgecolor="black"))

# %% [markdown]
# Rozkład geograficzny ofert w badanym zbiorze danych koncentruje się wokół kilku kluczowych punktów. Najwięcej ogłoszeń pochodzi z Łomży, gdzie odnotowano 159 ofert. Kolejną istotną lokalizacją jest Nowogród z liczbą 63 ofert. Znaczącą grupę stanowi również Stara Łomża, która łącznie w różnych wariantach nazewnictwa (Nad Rzeką, Przy Szosie, Starej Łomży) obejmuje 33 oferty. W Zambrowie zarejestrowano 28 ogłoszeń, natomiast w Starych Kupiskach 26. Mniejszą, ale wciąż zauważalną liczbę ofert posiadają Zawady (18), Jednaczewo (17), Konarzyce (17), Giełczyn (15) oraz Budy Czarnockie (14). Pozostałe miejscowości w zestawieniu posiadają mniej niż 14 ogłoszeń na lokalizację.

# %% [markdown]
# The geographical distribution of offers in the analyzed dataset focuses on several key points. The largest number of listings comes from Łomża, with 159 offers recorded. Another significant location is Nowogród with 63 offers. A substantial group is also formed by Stara Łomża, which collectively across various naming variants (Nad Rzeką, Przy Szosie, Starej Łomży) includes 33 offers. In Zambrów, 28 listings were registered, while in Stare Kupiski there were 26. A smaller but still noticeable number of offers can be found in Zawady (18), Jednaczewo (17), Konarzyce (17), Giełczyn (15), and Budy Czarnockie (14). Other locations in the summary have fewer than 14 listings per location.

# %% [markdown]
# ### Fixing problem with city naming

# %%
data["offers"][data["offers"]["CITY"].str.contains("ŁOMŻ")]["CITY"].unique()

# %%
cities_unification = ['STARA ŁOMŻA NAD RZEKĄ', 'STARA ŁOMŻA PRZY SZOSIE', 'STAREJ ŁOMŻY']
for city in cities_unification:
    print(data["offers"].loc[data["offers"]["CITY"].str.contains(city)]["CITY"].unique())
    data["offers"].loc[data["offers"]["CITY"].str.contains(city),"CITY"] = "STARA ŁOMŻA"

# %%
data["offers"][data["offers"]["CITY"].str.contains("ŁOMŻ")]["CITY"].unique()

# %% [markdown]
# ### Geopandas

# %%
data["offers_geopandas"] = data["offers"]

# %%
file_path = "data/ms_A06_Granice_obrebow_ewidencyjnych.gml"

# %%
data["boundaries"] = gpd.read_file(file_path)

# %%
data["offers_geopandas"] = data["offers_geopandas"].dropna(subset=["LAT", "LON"])
geometry = [Point(xy) for xy in zip(data["offers_geopandas"]["LON"], data["offers_geopandas"]["LAT"])]

# %%
data["gdf_points"] = gpd.GeoDataFrame(data["offers_geopandas"], geometry=geometry, crs="EPSG:4326")

# %%
data["boundaries_wgs"] = data["boundaries"].to_crs(epsg=4326)

# %%
data["gdf_plot"] = gpd.sjoin(data["gdf_points"], data["boundaries_wgs"], how="inner", predicate="within")

# %%
active_district_indices = data["gdf_plot"].index.unique()

# %%
data["active_boundaries"] = data["boundaries_wgs"].loc[active_district_indices]

# %%
fig, ax = plt.subplots(figsize=(15, 12))
data["boundaries_wgs"].plot(ax=ax, color='lightgrey', edgecolor='grey', alpha=0.5, label='boundaries')
lomza_boundary = data["boundaries_wgs"][data["boundaries_wgs"]['JPT_NAZWA_'].str.contains(r'^Łomża\b', regex=True, na=False)]
data["boundaries_wgs"].plot(ax=ax, color='lightgrey', edgecolor='grey', alpha=0.5, label='boundaries')
lomza_boundary.plot(ax=ax, color='#FFFFE0', edgecolor='#FFD700', linewidth=1.5, alpha=0.9, label='Miasto Łomża')
data["gdf_plot"].plot(ax=ax, marker='o', color='red', markersize=9, alpha=0.8, label='offers')
ax.set_axis_off() 
plt.tight_layout()
plt.title("Unique points on map")
plt.show()

# %% [markdown]
# ## Price per segment

# %%
data["offers"]["SIZE_SEGMENT"].value_counts()

# %%
data["offers"].groupby("SIZE_SEGMENT")[["PRICE", "PRICE_M2"]].agg(["mean", "median"])

# %%
segment_stats = data["offers"].groupby("SIZE_SEGMENT")[["PRICE", "PRICE_M2"]].agg(["mean", "median"])

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

segment_stats["PRICE"].plot(kind="bar", ax=axes[0], color=["#1f77b4", "#aec7e8"], edgecolor="black")
axes[0].set_title("PRICE per area segment", fontsize=14)
axes[0].set_ylabel("Price (PLN)")
axes[0].set_xlabel("Area segment")
axes[0].legend(["Średnia (Avg)", "Mediana (Median)"])
axes[0].tick_params(axis='x', rotation=45)

segment_stats["PRICE_M2"].plot(kind="bar", ax=axes[1], color=["#2ca02c", "#98df8a"], edgecolor="black")
axes[1].set_title("PRICE_M2 per segment ", fontsize=14)
axes[1].set_ylabel("PRICE_M2 (PLN)")
axes[1].set_xlabel("Area segment")
axes[1].legend(["Średnia (Avg)", "Mediana (Median)"])
axes[1].tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()

# %%
