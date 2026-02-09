from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Transportes TLOG - Summary (MVP)", layout="wide")

DATA_PATH = Path("data/summary_allperiods.parquet")

MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]
MONTH_TO_NUM = {m: i+1 for i, m in enumerate(MONTHS)}

METRIC_ORDER = [
    "Venta",
    "Volumen ocupación",
    "Valor de la caja (transporte)",
    "%venta",
    "Ocupación x remolque",
    "Kilometros recorridos",
    "Remolques embarcados",
    "WAD",
    "Gasto total + BKHL + FP + PA",
    "$/caja transportada",
    "Tractores (fijos)",
    "Variable dedicado",
    "Diesel dedicado",
    "Casetas",
    "Gasto dedicado",
    "$ de km tercero",
    "Diesel tercero",
    "Gasto tercero",
    "Remolques",
    "Quintas",
    "Remolques y quintas",
    "Ferry",
    "Aclaraciones",
    "Gastos secundarios (SICI)",
    "Desconsolidador",
    "Monitoreo",
    "Transferencias",
    "Intermodal",
    "Otros variables",
    "Gasto BKHL",
    "Ingreso BKHL",
    "Neto BKHL",
    "Devoluciones",
    "LI",
    "Devo & LI",
    "FP",
    "PA",
    "Freight program & PA",
]

SCENARIO_LABEL = {
    "REAL2025": "Real 2025",
    "BP": "Business Plan",
    "FCST": "Forecast actual",
}

PERIOD_TYPE_LABEL = {
    "M": "Mensual",
    "YTD": "YTD",
    "Q": "Quarter",
    "H": "Half-year",
    "FY": "Full year",
}

ALLOWED_YEARS = [2025, 2026]

@st.cache_data
def load_data(_mtime: float) -> pd.DataFrame:
    if not DATA_PATH.exists():
         st.warning("Aún no hay datos generados. Ve a la página **Cargar base** y carga un archivo (o modo demo) para generar el parquet.")
         st.stop()

    df = pd.read_parquet(DATA_PATH)

    if "year" in df.columns:
        df = df[df["year"].isin(ALLOWED_YEARS)].copy()

    # --- Normaliza nombre de columna de región ---
    if "region" not in df.columns:
        if "Region" in df.columns:
            df = df.rename(columns={"Region": "region"})
        elif "REGION" in df.columns:
            df = df.rename(columns={"REGION": "region"})

    # --- Normaliza valores de región ---
    if "region" in df.columns:
        df["region"] = df["region"].astype(str).str.strip()

        region_map = {
            "Zona Norte": "Norte",
            "Zona Sur": "Sur",
            "Zona Centro": "Centro",
            "Total logística": "Total logística",
            "Total logistica": "Total logística",
        }
        df["region"] = df["region"].replace(region_map)
    else:
        df["region"] = "Total logística"

    return df

def ensure_cols(summary: pd.DataFrame) -> pd.DataFrame:
    for c in ["Real 2025", "Business Plan", "Forecast actual"]:
        if c not in summary.columns:
            summary[c] = 0.0

    # Variaciones
    if {"Business Plan", "Forecast actual"}.issubset(summary.columns):
        summary["Δ Forecast vs BP"] = summary["Forecast actual"] - summary["Business Plan"]
    if {"Real 2025", "Forecast actual"}.issubset(summary.columns):
        summary["Δ Forecast vs Real"] = summary["Forecast actual"] - summary["Real 2025"]

    ordered = ["Real 2025", "Business Plan", "Forecast actual", "Δ Forecast vs BP", "Δ Forecast vs Real"]
    ordered = [c for c in ordered if c in summary.columns]
    return summary[ordered]

st.title("Transportes TLOG — Summary (MVP)")

mtime = DATA_PATH.stat().st_mtime if DATA_PATH.exists() else 0.0
df = load_data(mtime)

# --------- Región options  ----------
preferred_order = ["Total logística", "Norte", "Centro", "Sur"]
regions_found = sorted(df["region"].dropna().astype(str).unique().tolist())

# arma lista final: primero los preferidos que existan, luego los demás
region_options = [r for r in preferred_order if r in regions_found] + [r for r in regions_found if r not in preferred_order]
default_region_index = region_options.index("Total logística") if "Total logística" in region_options else 0

# ---------------- UI Controls ----------------
c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

with c1:
    period_type = st.selectbox(
        "Tipo de periodo",
        options=["M", "YTD", "Q", "H", "FY"],
        format_func=lambda x: PERIOD_TYPE_LABEL.get(x, x),
        index=0,
    )

with c2:
    year = st.selectbox("Año", options=ALLOWED_YEARS, index=1)  # default 2026

with c3:
    region = st.selectbox("Región", options=region_options, index=default_region_index)

# Selector adicional según el tipo
extra_label = None
extra_value = None

with c4:
    if period_type in ["M", "YTD"]:
        extra_label = "Mes"
        extra_value = st.selectbox("Mes", options=MONTHS, index=0)
    elif period_type == "Q":
        extra_label = "Quarter"
        extra_value = st.selectbox("Quarter", options=[1, 2, 3, 4], index=0)
    elif period_type == "H":
        extra_label = "Half"
        extra_value = st.selectbox("Half-year", options=[1, 2], index=0)
    else:
        st.write("")  # FY no necesita selector extra

# Construye el period_label que usó build.py
if period_type in ["M", "YTD"]:
    month_num = MONTH_TO_NUM[extra_value]
    period_label = f"{year:04d}-{month_num:02d}"
elif period_type == "Q":
    period_label = f"{year:04d}-Q{int(extra_value)}"
elif period_type == "H":
    period_label = f"{year:04d}-H{int(extra_value)}"
else:  # FY
    period_label = f"{year:04d}"

REAL_BASE_YEAR = 2025

# period_label del Real siempre amarrado a 2025 (misma granularidad)
if period_type in ["M", "YTD"]:
    period_label_real = f"2025-{month_num:02d}"
elif period_type == "Q":
    period_label_real = f"2025-Q{int(extra_value)}"
elif period_type == "H":
    period_label_real = f"2025-H{int(extra_value)}"
else:  # FY
    period_label_real = "2025"


# ---------------- Data Slice (FILTRADO POR REGIÓN) ----------------
slice_bp_fcst = df[
    (df["period_type"] == period_type)
    & (df["period_label"] == period_label)
    & (df["region"] == region)
    & (df["scenario"].isin(["BP", "FCST"]))
].copy()

slice_real = df[
    (df["period_type"] == period_type)
    & (df["period_label"] == period_label_real)
    & (df["region"] == region)
    & (df["scenario"] == "REAL2025")
].copy()

slice_df = pd.concat([slice_real, slice_bp_fcst], ignore_index=True)


if slice_df.empty:
    st.warning(
        f"No hay datos para: {PERIOD_TYPE_LABEL.get(period_type, period_type)} · "
        f"{period_label} (BP/FCST) + {period_label_real} (Real 2025) · {region}"
    )
    st.stop()

summary = (
    slice_df.pivot_table(index="metric", columns="scenario", values="value", aggfunc="sum")
    .reindex(METRIC_ORDER)
    .rename(columns=SCENARIO_LABEL)
    .fillna(0)
)

summary = ensure_cols(summary)

st.subheader(f"Summary — {PERIOD_TYPE_LABEL.get(period_type, period_type)} · {period_label} · {region}")

st.dataframe(
    summary.style.format("{:,.2f}"),
    use_container_width=True,
    height=900,
)

with st.expander("Debug"):
    st.write("Registros en el slice:", len(slice_df))
    st.write("Región:", region)
    st.write("Escenarios presentes:", sorted(slice_df["scenario"].unique().tolist()))
    st.write("Regiones presentes en parquet:", sorted(df["region"].dropna().astype(str).unique().tolist()))
