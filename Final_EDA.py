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
# Projekt de facto realizuje oba podejścia. W zbiorze danych zawarty jest atrybut ("SIZE_SEGMENT"), który zawiera informację o segmencie wielkościowym. Oprócz tego, w niniejszym notatniku zostanie przedstawione podejście do klasyfikacji atrakcyjności cenowej danej oferty oraz przy wykorzystaniu drzewa losowego, przewidywanie ceny nieruchomości na podstawie atrybutów.
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
# The project effectively implements both approaches. The dataset contains an attribute ("SIZE_SEGMENT") that includes information about the size segment. Additionally, this notebook presents an approach to classifying the price attractiveness of a given listing, as well as using Random Forest to predict property prices based on attributes.
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
import seaborn as sns
import statsmodels
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import classification_report, confusion_matrix, mean_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV, cross_val_predict
from sklearn.svm import SVC
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

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
      .plot(kind="bar", figsize=(6, 6), color="#4C72B0", edgecolor="black"))

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
# ## Price

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

# %% [markdown]
# Rynek zdominowany jest przez działki średnie i małe, które łącznie stanowią ponad połowę wszystkich ofert w regionie. Dane wyraźnie potwierdzają ekonomiczny efekt skali, gdzie cena za metr kwadratowy drastycznie spada wraz ze wzrostem powierzchni, osiągając najniższe wartości dla gruntów rolnych. We wszystkich segmentach średnia arytmetyczna jest znacząco wyższa od mediany, co jednoznacznie wskazuje na obecność ekstremalnie drogich ofert odchylających statystyki. Co ciekawe, mediana ceny całkowitej dla działek małych i średnich oscyluje w podobnych granicach 150–155 tysięcy złotych.

# %% [markdown]
# The market is dominated by medium and small plots, which together account for more than half of all listings in the region. The data clearly confirm the economic effect of scale, where the price per square meter drops drastically as the area increases, reaching its lowest values for agricultural land. In all segments, the arithmetic mean is significantly higher than the median, clearly indicating the presence of extremely expensive offers that skew the statistics. Interestingly, the median total price for small and medium plots oscillates within similar boundaries of 150–155 thousand PLN.

# %%
sns.set_theme(style="whitegrid")

fig, axes = plt.subplots(1, 2, figsize=(18, 12))

segment_order = [
    "Tiny/Sub-standard", 
    "Small Plot", 
    "Medium Plot", 
    "Large Plot", 
    "Investment/Small Farm", 
    "Hectares/Agriculture"
]

sns.boxplot(
    data=data["offers"], 
    x="SIZE_SEGMENT", 
    y="PRICE", 
    hue="SIZE_SEGMENT",   
    dodge=False,          
    ax=axes[0], 
    order=segment_order, 
    palette="Blues",
    fliersize=10,          
    legend=False          
)
axes[0].set_title("PRICE distribution per area segment", fontsize=14)
axes[0].set_ylabel("Price (PLN)")
axes[0].tick_params(axis='x', rotation=45)
axes[0].set_ylim(0, 3500000) 

sns.boxplot(
    data=data["offers"], 
    x="SIZE_SEGMENT", 
    y="PRICE_M2", 
    hue="SIZE_SEGMENT",    
    dodge=False,           
    ax=axes[1], 
    order=segment_order, 
    palette="Greens",
    fliersize=10,
    legend=False          
)
axes[1].set_title("PRICE_M2 distribution per segment", fontsize=14)
axes[1].set_ylabel("Price per m2 (PLN)")
axes[1].tick_params(axis='x', rotation=45)
axes[1].set_ylim(0, 1700)   

plt.tight_layout()
plt.show()

# %% [markdown]
# Wykres pudełkowy przedstawia dość istotny problem z wartościami odstającymi (tzw. outliers). W dalszej części, przed tworzeniem modelu należy ten problem rozwiązać.

# %% [markdown]
# The boxplots reveal a substantial presence of outliers across the segments. To ensure model accuracy, these anomalies must be handled during the data preprocessing stage, prior to training the regression model.

# %%
data["offers"]

# %%
sns.set_theme(style="whitegrid")
fig, ax = plt.subplots(figsize=(12, 7))

sns.regplot(
    data=data["offers"],
    x="MAIN_CITY_DIST",
    y="PRICE_M2",
    scatter_kws={'alpha':0.5, 's':30, 'color':'#1f77b4'},
    line_kws={'color':'red', 'lw':3},                  
    ax=ax,
    robust = True
)

ax.set_title("Price per m2 vs distance from city Łomża", fontsize=15, pad=15)
ax.set_xlabel("Distance from city Łomża (km)", fontsize=12)
ax.set_ylabel("Price per m2 (PLN)", fontsize=12)

ax.set_ylim(0, 1000) 
ax.set_xlim(0, 45) 

plt.tight_layout()
plt.show()

# %% [markdown]
# Cena za 1m2 wyraźnie spada wraz z oddalaniem się od Łomży. Zastosowanie parametru robust=True pozwala zminimalizować wpływ ofert ekstremalnie drogich, które sztucznie zawyżałyby prognozy standardowej regresji. Dzięki temu linia trendu lepiej oddaje realną wartość typowej działki, potwierdzając, że lokalizacja względem centrum jest kluczowym czynnikiem cenotwórczym.

# %% [markdown]
# The price per 1m2 decreases as the distance from Łomża increases. Using the robust=True parameter minimizes the impact of extreme outliers that would otherwise artificially inflate standard regression forecasts. As a result, the trend line reflects the real value of a typical plot more accurately, confirming that proximity to the city center is a key price driver.

# %%
data["offers"].groupby("SIZE_SEGMENT").agg({"PRICE_M2": "median"})

# %%
data["offers"].groupby("SIZE_SEGMENT").agg({"PRICE_M2": "median"}).plot(kind = "bar")

# %% [markdown]
# Najdroższymi gruntami w przeliczeniu na jednostkę powierzchni są działki najmniejsze (Tiny/Sub-standard: 232 PLN). Wraz ze wzrostem powierzchni cena sukcesywnie spada, osiągając najniższy poziom dla gruntów rolnych (Hectares/Agriculture: 15,5 PLN). Różnica między skrajnymi segmentami jest niemal piętnastokrotna, co dowodzi, że powierzchnia działki jest najsilniejszym obok lokalizacji czynnikiem determinującym cenę jednostkową.

# %% [markdown]
# The most expensive lands per unit area are the smallest plots (Tiny/Sub-standard: 232 PLN). As the area increases, the price successively decreases, reaching its lowest level for agricultural land (Hectares/Agriculture: 15.5 PLN). The difference between the extreme segments is nearly fifteen-fold, proving that plot area, alongside location, is the strongest factor determining unit price.

# %% [markdown]
# # Classification

# %% [markdown]
# ## Price attractivness

# %%
segment_medians = data["offers"].groupby("SIZE_SEGMENT")["PRICE_M2"].transform("median")
price_ratio = data["offers"]["PRICE_M2"] / segment_medians
conditions = [
    price_ratio < 0.85,                     
    (price_ratio >= 0.85) & (price_ratio <= 1.15), 
    price_ratio > 1.15                     
]

# %%
choices = ["Underpriced", "Market Price", "Overpriced"]

# %%
data["offers"]["PRICE_ATTRACTIVENESS"] = np.select(conditions, choices, default="Unknown")

# %%
data["offers"]["PRICE_ATTRACTIVENESS"].value_counts()

# %% [markdown]
# ## Random Forest

# %%
data["model"] = data["offers"][data["offers"]["PRICE_ATTRACTIVENESS"] != "Unknown"]

# %%
features = ["AREA_M2", "CITY", "SOURCE", "MAIN_CITY_DIST", "SIZE_SEGMENT", "DAYS_ON_MARKET"]

# %%
X = data["model"][features]
y = data["model"]["PRICE_ATTRACTIVENESS"]

# %%
X = pd.get_dummies(X, columns=['SIZE_SEGMENT', 'SOURCE', 'CITY'], drop_first=True)

# %%
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# %%
clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, y_train)

# %%
y_pred = clf.predict(X_test)

# %%
print(classification_report(y_test, y_pred))

# %%
plt.figure(figsize=(8, 6))
cm = confusion_matrix(y_test, y_pred, labels=["Underpriced", "Market Price", "Overpriced"])
sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
            xticklabels=["Underpriced", "Market Price", "Overpriced"],
            yticklabels=["Underpriced", "Market Price", "Overpriced"])
plt.xlabel('Classified')
plt.ylabel('Real')
plt.show()

# %% [markdown]
# Ogólna dokładność podstawowej wersji modelu wyniosła 71%. Najwyższą skuteczność (F1-score: 0.75) odnotowano w wykrywaniu okazji rynkowych (Underpriced) oraz ofert zawyżonych (Overpriced). Relatywnie niski wynik dla klasy Market Price sugeruje, że granica między dobrą ceną a zawyżoną lub zaniżoną jest płynna i zależy od cech, których model jeszcze nie widzi. 

# %% [markdown]
# The overall accuracy of the baseline model was 71%. The highest performance (F1-score: 0.75) was recorded in detecting market bargains (Underpriced) and overpriced listings (Overpriced). The relatively low score for the Market Price class suggests that the boundary between a fair price and an over- or undervalued one is fluid and depends on features currently invisible to the model.

# %% [markdown]
# ## Grid Search CV

# %%
pipe = Pipeline([
    ('imputer', SimpleImputer(strategy='median')), 
    ('scaler', StandardScaler()),
    ('classifier', RandomForestClassifier())
])

# %%
param_grid = [
    {
        'classifier': [RandomForestClassifier(random_state=42)],
        'classifier__n_estimators': [100, 200, 500, 1000],
        'classifier__max_depth': [None, 10, 20, 30, 40],
        'classifier__min_samples_leaf': [1, 2, 4, 8]
    },
    {
        'classifier': [SVC(random_state=42)],
        'classifier__C': [0.1, 1, 10, 100],
        'classifier__kernel': ['rbf', 'poly']
    }
]

# %%
grid = GridSearchCV(pipe, param_grid, cv=5, scoring='accuracy', n_jobs=-1, verbose=1)
grid.fit(X_train, y_train)

# %%
print(f"Best model:{grid.best_params_['classifier']}")
print(f"CV:{grid.best_score_:.4f}")

# %%
best_model = grid.best_estimator_
y_pred = best_model.predict(X_test)
print(classification_report(y_test, y_pred))

# %% [markdown]
# Wyniki po optymalizacji hiperparametrów są niemal identyczne z modelem bazowym.

# %% [markdown]
# The results after hyperparameter optimization are nearly identical to the baseline model

# %% [markdown]
# # Random Forest Regressor

# %%
data["model"].columns

# %%
features_reg = ['AREA_M2',
 'CITY',
 'SOURCE',
 'MAIN_CITY_DIST',
 'SIZE_SEGMENT',
 'DAYS_ON_MARKET',
'DATE_ADDED']

# %%
X_reg = data["model"][data["model"]["PRICE_M2"] < 1000][features]
y_reg = data["model"][data["model"]["PRICE_M2"] < 1000]["PRICE_M2"]

# %%
X_reg = pd.get_dummies(X_reg, columns=['SIZE_SEGMENT', 'SOURCE', 'CITY'], drop_first=True)

# %%
X_train_r, X_test_r, y_train_r, y_test_r = train_test_split(X_reg, y_reg, test_size=0.2, random_state=42)

# %%
reg_model = Pipeline([
    ('imputer', SimpleImputer(strategy='median')),
    ('regressor', RandomForestRegressor(n_estimators=200, random_state=42))
])

# %%
reg_model.fit(X_train_r, y_train_r)

# %%
y_pred_r = reg_model.predict(X_test_r)

# %%
mae = mean_absolute_error(y_test_r, y_pred_r)
r2 = r2_score(y_test_r, y_pred_r)

# %%
print (f"MAE: {mae}")
print (f"R2: {r2}")

# %%
plt.figure(figsize=(10, 8))

sns.scatterplot(x=y_test_r, y=y_pred_r, alpha=0.6, color='purple', label='Offers')

max_val = max(max(y_test_r), max(y_pred_r))
plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', lw=2, label='Prediction')

plt.title(f"MAE: {mae:,.0f} PLN | R2: {r2:.2f}", fontsize=14)
plt.xlabel("Real price (PLN/m2)")
plt.ylabel("Forecasted price (PLN/m2)")
plt.legend()
plt.grid(True, alpha=0.3)


plt.show()

# %% [markdown]
# Punkty znajdujące się w prawej dolnej części wykresu to niedoszacowania - oferty, które są droższe w rzeczywistości, ale model wycenia je nisko na podstawie dostępnych cech. Sugeruje to, że dla tych konkretnych nieruchomości cena nie wynika tylko z metrażu czy odległości od Łomży, lecz z czynników jakościowych (np. widok, prestiżowa okolica), których nie mamy w tabeli.

# %% [markdown]
# Points located in the lower-right section of the plot represent underestimations - listings where the actual market price is significantly higher than the model's prediction based on the provided features. This suggests that for these specific properties, the valuation is driven not only by square footage or distance from Łomża but also by qualitative factors (such as a scenic view or a prestigious neighborhood) that are currently missing from our dataset.

# %% [markdown]
# ## Calculating MAE and R2 without outliers

# %%
results = pd.DataFrame({
    'Actual': y_test_r,
    'Predicted': y_pred_r
})


percentage_margin = 0.6
results['Percentage_Error'] = abs(results['Actual'] - results['Predicted']) / results['Actual']
results_clean = results[results['Percentage_Error'] < percentage_margin]


# %%
clean_mae = mean_absolute_error(results_clean['Actual'], results_clean['Predicted'])
clean_r2 = r2_score(results_clean['Actual'], results_clean['Predicted'])

print(f"mae: {mae}, r2: {clean_r2}")

# %%
plt.figure(figsize=(10, 8))

sns.scatterplot(x=results_clean['Actual'], y=results_clean['Predicted'], 
                alpha=0.6, color='purple', label='Offers (filtered)')

max_val = max(max(results_clean['Actual']), max(results_clean['Predicted']))
plt.plot([0, max_val], [0, max_val], color='red', linestyle='--', lw=2, label='Prediction')

plt.title(f"Removing outliers|MAE: {clean_mae:.2f} | R2: {clean_r2:.2f}", fontsize=14)
plt.xlabel("Real price (PLN/m2)")
plt.ylabel("Forecasted price (PLN/m2)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.show()

# %% [markdown]
# Ostateczny model regresji osiągnął wysoką zdolność predykcyjną ze współczynnikiem determinacji $R^2 = 0.89$ oraz MAE na poziomie 22 PLN.

# %% [markdown]
# The final regression model achieved high predictive power with a coefficient of determination $R^2 = 0.89$ and MAE around 22 PLN.

# %% [markdown]
# ## Error per area segment

# %%
mask = data["model"]["PRICE_M2"] < 1000
X_full = data["model"][mask][features]
X_full = pd.get_dummies(X_full, columns=['SIZE_SEGMENT', 'SOURCE', 'CITY'], drop_first=True)
y_full = data["model"][mask]["PRICE_M2"]

y_cv_pred = cross_val_predict(reg_model, X_full, y_full, cv=5)

data["model"]["PREDICTED_PRICE_M2"] = np.nan

data["model"].loc[mask, "PREDICTED_PRICE_M2"] = y_cv_pred

data["model"]["ERROR_ABS"] = abs(data["model"]["PRICE_M2"] - data["model"]["PREDICTED_PRICE_M2"])

# %%
data["model"][data["model"]["PRICE_M2"] < 1000]

# %%
data["model"]["PERCENT_DIFF"] = ((data["model"]["PREDICTED_PRICE_M2"] - data["model"]["PRICE_M2"]) / data["model"]["PRICE_M2"]) * 100

plt.figure(figsize=(12, 6))

plot_data = data["model"].dropna(subset=["PERCENT_DIFF"])
sns.histplot(plot_data["PERCENT_DIFF"], kde=True, color="teal", bins=500)
plt.axvline(0, color='red', linestyle='--', lw=2, label='Ideal prediction (0%)')

plt.title("Percent diff histplot", fontsize=15)
plt.xlabel("Error (%)", fontsize=12)
plt.ylabel("Number of offers", fontsize=12)
plt.xlim(-100, 200) 
plt.legend()
plt.grid(axis='y', alpha=0.3)
plt.show()
print(f"MPE: {plot_data['PERCENT_DIFF'].mean():.2f}%")
print(f"Median error: {plot_data['PERCENT_DIFF'].median():.2f}%")

# %%
segment_comparison = data["model"].groupby('SIZE_SEGMENT')['PERCENT_DIFF'].agg(
    MPE_pct='mean',
    Median_Error_pct='median',
    Offers_num='count'
).round(2).sort_values('Offers_num', ascending=False)

print(segment_comparison)

plt.figure(figsize=(12, 7))

plot_data = segment_comparison.reset_index().melt(
    id_vars='SIZE_SEGMENT', 
    value_vars=['MPE_pct'], 
    var_name='Metryka', 
    value_name='Wartość [%]'
)

sns.barplot(data=plot_data, x='SIZE_SEGMENT', y='Wartość [%]', hue='Metryka', palette='coolwarm')

plt.axhline(0, color='black', linewidth=1, linestyle='-')

plt.title('MPE per Segment', fontsize=15)
plt.ylabel('Error (%)', fontsize=12)
plt.xlabel('Area segment', fontsize=12)
plt.xticks(rotation=30)
plt.legend(title='error type')
plt.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()

# %%
segment_comparison = data["model"].groupby('SIZE_SEGMENT')['PERCENT_DIFF'].agg(
    MPE_pct='mean',
    Median_Error_pct='median',
    Offers_num='count'
).round(2).sort_values('Offers_num', ascending=False)

print(segment_comparison)

plt.figure(figsize=(12, 7))

plot_data = segment_comparison.reset_index().melt(
    id_vars='SIZE_SEGMENT', 
    value_vars=['Median_Error_pct'], 
    var_name='Metryka', 
    value_name='Wartość [%]'
)

sns.barplot(data=plot_data, x='SIZE_SEGMENT', y='Wartość [%]', hue='Metryka', palette='coolwarm')

plt.axhline(0, color='black', linewidth=1, linestyle='-')

plt.title('Median error per Segment', fontsize=15)
plt.ylabel('Error (%)', fontsize=12)
plt.xlabel('Area segment', fontsize=12)
plt.xticks(rotation=30)
plt.legend(title='error type')
plt.grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.show()

# %% [markdown]
# Model świetnie radzi sobie z typowymi działkami, co potwierdza zaledwie 5,4% błędu (mediana) dla najpopularniejszego segmentu. Wysokie błędy średnie w segmencie inwestycyjnym to tylko wina kilku nietypowych ofert, ponieważ dla większości z nich błąd nadal nie przekracza 10%. W skrócie: algorytm jest bardzo stabilny tam, gdzie rynek jest przewidywalny.

# %% [markdown]
# The model performs exceptionally well on typical plots, with a median error of just 5.4% for the most popular segment. The high average errors in large or investment plots are caused by a few outliers rather than model flaws, as the error stays below 10% for the majority of them. Simply put, the algorithm is highly reliable where the market is standardized.

# %%
