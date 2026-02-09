from pathlib import Path
import pandas as pd
import streamlit as st
import altair as alt

st.set_page_config(page_title="Transportes TLOG - Bridge (MVP)", layout="wide")

DATA_PATH = Path("data/summary_allperiods.parquet")

MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
MONTH_TO_NUM = {m: i+1 for i, m in enumerate(MONTHS)}

PERIOD_TYPE_LABEL = {
    "M": "Mensual",
    "YTD": "YTD",
    "Q": "Quarter",
    "H": "Half-year",
    "FY": "Full year",
}

ALLOWED_YEARS = [2025, 2026]
REAL_BASE_YEAR = 2025

# ====== Métricas del summary (según tus definiciones) ======
TOTAL_METRIC = "Gasto total + BKHL + FP + PA"

MET_TRACTORES = "Tractores (fijos)"
MET_VAR_DED = "Variable dedicado"
MET_DIESEL_DED = "Diesel dedicado"
MET_CASETAS = "Casetas"
MET_REM_QUINT = "Remolques y quintas"
MET_PXV = "Gasto tercero"
MET_OTROS = "Otros variables"
MET_NETO_BKHL = "Neto BKHL"
MET_FP_PA = "Freight program & PA"


def build_period_label(period_type: str, year: int, extra_value) -> tuple[str, int | None]:
    """Regresa (period_label, month_num). month_num solo aplica para M/YTD."""
    if period_type in ["M", "YTD"]:
        month_num = MONTH_TO_NUM[extra_value]
        return f"{year:04d}-{month_num:02d}", month_num
    if period_type == "Q":
        return f"{year:04d}-Q{int(extra_value)}", None
    if period_type == "H":
        return f"{year:04d}-H{int(extra_value)}", None
    return f"{year:04d}", None  # FY


@st.cache_data
def load_df(_mtime: float) -> pd.DataFrame:
    if not DATA_PATH.exists():
        st.error(f"No encuentro {DATA_PATH}. Corre el pipeline (Cargar base o py build.py).")
        st.stop()

    df = pd.read_parquet(DATA_PATH)

    # solo 2025/2026
    if "year" in df.columns:
        df = df[df["year"].isin(ALLOWED_YEARS)].copy()

    # normaliza region si viene en otra col
    if "region" not in df.columns and "Region" in df.columns:
        df = df.rename(columns={"Region": "region"})

    if "region" not in df.columns:
        df["region"] = "Total logística"

    # normaliza valores
    df["region"] = (
        df["region"]
        .astype(str)
        .str.strip()
        .replace({
            "Zona Norte": "Norte",
            "Zona Sur": "Sur",
            "Zona Centro": "Centro",
            "Total logistica": "Total logística",
        })
    )

    return df


def metric_sum(df_slice: pd.DataFrame, scenario: str, metric: str) -> float:
    s = df_slice[(df_slice["scenario"] == scenario) & (df_slice["metric"] == metric)]["value"]
    return float(s.sum()) if not s.empty else 0.0


def metric_delta(df_slice: pd.DataFrame, metric: str) -> float:
    """Delta FCST - BP para una métrica."""
    return metric_sum(df_slice, "FCST", metric) - metric_sum(df_slice, "BP", metric)


st.title("Transportes TLOG — Bridge / Cascada (MVP)")
mtime = DATA_PATH.stat().st_mtime if DATA_PATH.exists() else 0.0
df = load_df(mtime)

# --- Región options ---
preferred_order = ["Total logística", "Norte", "Centro", "Sur"]
regions_found = sorted(df["region"].dropna().astype(str).unique().tolist())
region_options = [r for r in preferred_order if r in regions_found] + [r for r in regions_found if r not in preferred_order]
default_region_index = region_options.index("Total logística") if "Total logística" in region_options else 0

# --- UI ---
c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

with c1:
    period_type = st.selectbox(
        "Tipo de periodo",
        options=["M", "YTD", "Q", "H", "FY"],
        format_func=lambda x: PERIOD_TYPE_LABEL.get(x, x),
        index=0,
    )

with c2:
    year = st.selectbox("Año (BP/FCST)", options=[2026, 2025], index=0)

with c3:
    region = st.selectbox("Región", options=region_options, index=default_region_index)

with c4:
    extra_value = None
    if period_type in ["M", "YTD"]:
        extra_value = st.selectbox("Mes", options=MONTHS, index=0)
    elif period_type == "Q":
        extra_value = st.selectbox("Quarter", options=[1, 2, 3, 4], index=0)
    elif period_type == "H":
        extra_value = st.selectbox("Half-year", options=[1, 2], index=0)
    else:
        st.write("")

period_label_main, month_num = build_period_label(period_type, year, extra_value)

# Real siempre amarrado a 2025 (mismo tipo de periodo)
if period_type in ["M", "YTD"]:
    period_label_real = f"{REAL_BASE_YEAR:04d}-{month_num:02d}"
elif period_type == "Q":
    period_label_real = f"{REAL_BASE_YEAR:04d}-Q{int(extra_value)}"
elif period_type == "H":
    period_label_real = f"{REAL_BASE_YEAR:04d}-H{int(extra_value)}"
else:
    period_label_real = f"{REAL_BASE_YEAR:04d}"

# --- slices ---
slice_main = df[
    (df["period_type"] == period_type)
    & (df["period_label"] == period_label_main)
    & (df["region"] == region)
].copy()

slice_real = df[
    (df["period_type"] == period_type)
    & (df["period_label"] == period_label_real)
    & (df["region"] == region)
    & (df["scenario"] == "REAL2025")
].copy()

if slice_main.empty:
    st.warning(f"No hay datos para {PERIOD_TYPE_LABEL.get(period_type, period_type)} · {period_label_main} · {region}")
    st.stop()

# --- Totales ---
last_year = metric_sum(slice_real, "REAL2025", TOTAL_METRIC)
bp_total = metric_sum(slice_main, "BP", TOTAL_METRIC)
fc_total = metric_sum(slice_main, "FCST", TOTAL_METRIC)

# --- Drivers (deltas FCST - BP) ---
fijos = metric_delta(slice_main, MET_TRACTORES)

variables = (
    metric_delta(slice_main, MET_VAR_DED)
    + metric_delta(slice_main, MET_DIESEL_DED)
    + metric_delta(slice_main, MET_CASETAS)
)

rem_quintas = metric_delta(slice_main, MET_REM_QUINT)
pxv = metric_delta(slice_main, MET_PXV)
otros = metric_delta(slice_main, MET_OTROS)

# iniciativas ahorro = delta( Neto BKHL + Freight program & PA )
iniciativas_ahorro = (
    metric_delta(slice_main, MET_NETO_BKHL)
    + metric_delta(slice_main, MET_FP_PA)
)

# ====== CIERRE PERFECTO SIN "Residual" ======
# Queremos que:
#   bp_total + (sum_drivers) == fc_total
delta_total = fc_total - bp_total
sum_drivers = fijos + variables + rem_quintas + pxv + otros + iniciativas_ahorro
eps = delta_total - sum_drivers

# Absorbemos cualquier mini-diferencia en "Otros" (para que cierre exacto sin residual)
otros += eps

# Recalcula sum_drivers ya ajustado
sum_drivers = fijos + variables + rem_quintas + pxv + otros + iniciativas_ahorro

# --- arma waterfall dataset ---
rows = []
rows.append({"step": "Last year (Real 2025)", "start": 0.0, "end": last_year, "delta": 0.0, "kind": "total"})
rows.append({"step": "Business plan (BP 2026)", "start": 0.0, "end": bp_total, "delta": 0.0, "kind": "total"})

cum = bp_total
for name, dv in [
    ("Fijos", fijos),
    ("Variables", variables),
    ("Remolques y quintas", rem_quintas),
    ("PxV", pxv),
    ("Otros", otros),
    ("Iniciativas ahorro", iniciativas_ahorro),
]:
    rows.append({"step": name, "start": cum, "end": cum + dv, "delta": dv, "kind": "delta"})
    cum += dv

# Sin residual: el cierre debe dar EXACTO al forecast
rows.append({"step": "Gasto actual (Forecast)", "start": 0.0, "end": fc_total, "delta": 0.0, "kind": "total"})

wdf = pd.DataFrame(rows)

st.subheader(
    f"Bridge — {PERIOD_TYPE_LABEL.get(period_type, period_type)} · {period_label_main} "
    f"(Real: {period_label_real}) · {region}"
)

chart = (
    alt.Chart(wdf)
    .mark_bar()
    .encode(
        x=alt.X("step:N", sort=None, axis=alt.Axis(labelAngle=-90)),
        y=alt.Y("start:Q", title=None),
        y2=alt.Y2("end:Q"),
        tooltip=[
            alt.Tooltip("step:N"),
            alt.Tooltip("delta:Q", format=",.2f"),
            alt.Tooltip("end:Q", format=",.2f"),
        ],
    )
)

st.altair_chart(chart, use_container_width=True)

with st.expander("Debug (números)"):
    st.write({
        "Last year (Real 2025)": last_year,
        "Business Plan": bp_total,
        "Forecast actual": fc_total,
        "Drivers sum (ajustado)": sum_drivers,
        "Epsilon absorbido en 'Otros'": eps,
        "BP + Drivers (debe = Forecast)": bp_total + sum_drivers,
    })
