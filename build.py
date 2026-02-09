from pathlib import Path
import pandas as pd
import duckdb

RAW_CSV = "input/raw_dummy.csv"
BASE_XLSX = "Base_xepelin.xlsx"
SHEET = "Base"

OUT_DIR = Path("data")
OUT_PARQUET = OUT_DIR / "summary_allperiods.parquet"

# ---------- helpers: Excel col letters -> index ----------
def excel_col_to_0idx(col: str) -> int:
    """
    'A' -> 0, 'B' -> 1, ..., 'Z' -> 25, 'AA' -> 26 ...
    """
    col = col.strip().upper()
    n = 0
    for ch in col:
        if not ("A" <= ch <= "Z"):
            raise ValueError(f"Excel column inválida: {col}")
        n = n * 26 + (ord(ch) - ord("A") + 1)
    return n - 1

def get_offset_and_csv_cols(anchor="Tipo de reporte"):
    # Columnas como están en tu Excel (incluye Unnamed:0 y Unnamed:1)
    xls_cols = pd.read_excel(BASE_XLSX, sheet_name=SHEET, header=2, nrows=0).columns.tolist()
    # Columnas como quedaron en el CSV (tu dummy)
    csv_cols = pd.read_csv(RAW_CSV, nrows=0).columns.tolist()

    if anchor not in xls_cols or anchor not in csv_cols:
        raise ValueError(
            f"No encontré anchor '{anchor}' en ambos archivos.\n"
            f"En XLSX existe: {anchor in xls_cols}\n"
            f"En CSV existe:  {anchor in csv_cols}"
        )

    offset = xls_cols.index(anchor) - csv_cols.index(anchor)
    return offset, csv_cols

def colname_from_excel_letter(letter: str, offset: int, csv_cols: list[str]) -> str:
    idx_excel = excel_col_to_0idx(letter)
    idx_csv = idx_excel - offset
    if idx_csv < 0 or idx_csv >= len(csv_cols):
        raise IndexError(
            f"Letra {letter} -> idx_excel={idx_excel}, offset={offset}, idx_csv={idx_csv} fuera de rango.\n"
            f"CSV tiene {len(csv_cols)} columnas."
        )
    return csv_cols[idx_csv]

def sum_letters_sql(letters: list[str], offset: int, csv_cols: list[str]) -> str:
    """
    Devuelve expresión SQL tipo:
    SUM(COALESCE(TRY_CAST("col" AS DOUBLE),0)) + SUM(...)
    """
    colnames = [colname_from_excel_letter(l, offset, csv_cols) for l in letters]
    terms = [f'SUM(COALESCE(TRY_CAST("{c}" AS DOUBLE), 0))' for c in colnames]
    return " + ".join(terms) if terms else "0"


# ---------- main ----------
def main():
    OUT_DIR.mkdir(exist_ok=True)

    # 1) Prepara mapeo Excel letters -> nombres reales del CSV
    offset, csv_cols = get_offset_and_csv_cols(anchor="Tipo de reporte")
    print(f"✅ Offset detectado vs Excel: {offset} columnas")

    # =============================
    #   LETRAS (según tu layout)
    # =============================

    # Ventas y Volumen 
    venta_letters = ["M"]             # "Ventas" en el template (si cambia, ajusta)
    volumen_letters = ["N"]           # Columna N

    # Gasto dedicado 
    tractores_letters = ["EC", "ED", "ET", "EU"]
    variable_dedicado_letters = ["BF", "BG"]
    diesel_dedicado_letters = ["AX", "BV", "CF"]
    casetas_letters = ["AV", "BT"]

    # Tercero (PxV)
    km_tercero_letters = ["CA"]       # "porteo tercero"
    diesel_tercero_letters = ["CB"]   # "base diesel tercero"

    # Remolques y quintas
    remolques_letters = ["EK"]        # "remolques"
    quintas_letters = ["EL"]          # "quintas"

    # Otros variables 
    ferry_letters = ["CI"]            # "ferry"
    aclaraciones_letters = ["AZ"]     # "aclaraciones"
    transferencias_letters = ["CG"]   # "transferencias"
    intermodal_letters = ["AQ"]       # "intermodal / rail"
    monitoreo_letters = ["BN"]        # "monitoreo"
    desconsolidadores_letters = ["CR"]# "desconsolidadores"

    # Si existe en tu layout real (si no, déjalo vacío)
    gastos_secundarios_letters = []   # <-- si lo tienes, pon su letra aquí (ej: ["DP"])

    # BKHL / Devoluciones
    ingreso_bkhl_letters = ["CW"]     # "ingreso bkhl" (debería venir negativo)
    gasto_bkhl_letters = ["CX"]       # "bkhl expense" (positivo)
    devoluciones_letters = ["AH"]     # "neto devoluciones"

    # FP y PA 
    fp_letters = ["DL"]               # "fp expense"
    pa_letters = ["DE"]               # "pallet and packaging expense" (si tu PA está en otra, ajústala)

    # =============================
    #   EXPRESIONES
    # =============================
    expr_venta = sum_letters_sql(venta_letters, offset, csv_cols)
    expr_volumen = sum_letters_sql(volumen_letters, offset, csv_cols)

    expr_tractores = sum_letters_sql(tractores_letters, offset, csv_cols)
    expr_variable = sum_letters_sql(variable_dedicado_letters, offset, csv_cols)
    expr_diesel = sum_letters_sql(diesel_dedicado_letters, offset, csv_cols)
    expr_casetas = sum_letters_sql(casetas_letters, offset, csv_cols)

    expr_gasto_dedicado = " + ".join([expr_tractores, expr_variable, expr_diesel, expr_casetas])

    expr_km_tercero = sum_letters_sql(km_tercero_letters, offset, csv_cols)
    expr_diesel_tercero = sum_letters_sql(diesel_tercero_letters, offset, csv_cols)
    expr_gasto_tercero = " + ".join([expr_km_tercero, expr_diesel_tercero])

    expr_remolques = sum_letters_sql(remolques_letters, offset, csv_cols)
    expr_quintas = sum_letters_sql(quintas_letters, offset, csv_cols)
    expr_remolques_quintas = " + ".join([expr_remolques, expr_quintas])

    expr_ferry = sum_letters_sql(ferry_letters, offset, csv_cols)
    expr_aclaraciones = sum_letters_sql(aclaraciones_letters, offset, csv_cols)
    expr_gastos_sec = sum_letters_sql(gastos_secundarios_letters, offset, csv_cols) if gastos_secundarios_letters else "0"
    expr_desconso = sum_letters_sql(desconsolidadores_letters, offset, csv_cols)
    expr_monitoreo = sum_letters_sql(monitoreo_letters, offset, csv_cols)
    expr_transfer = sum_letters_sql(transferencias_letters, offset, csv_cols)
    expr_intermodal = sum_letters_sql(intermodal_letters, offset, csv_cols)

    expr_otros_var = " + ".join([
        expr_ferry,
        expr_aclaraciones,
        expr_gastos_sec,
        expr_desconso,
        expr_monitoreo,
        expr_transfer,
        expr_intermodal,
    ])

    expr_ingreso_bkhl = sum_letters_sql(ingreso_bkhl_letters, offset, csv_cols)
    expr_gasto_bkhl = sum_letters_sql(gasto_bkhl_letters, offset, csv_cols)
    expr_neto_bkhl = " + ".join([expr_gasto_bkhl, expr_ingreso_bkhl])

    expr_devoluciones = sum_letters_sql(devoluciones_letters, offset, csv_cols)

    expr_fp = sum_letters_sql(fp_letters, offset, csv_cols)
    expr_pa = sum_letters_sql(pa_letters, offset, csv_cols)
    expr_fp_pa = " + ".join([expr_fp, expr_pa])

    expr_gasto_total = " + ".join([
        expr_gasto_dedicado,
        expr_gasto_tercero,
        expr_remolques_quintas,
        expr_otros_var,
        expr_neto_bkhl,
        expr_devoluciones,
        expr_fp_pa,
    ])

    # Derivadas
    expr_valor_caja = f"({expr_venta}) / NULLIF(({expr_volumen}), 0)"
    expr_pct_venta = f"({expr_gasto_total}) / NULLIF(({expr_venta}), 0)"
    expr_cost_per_box = f"({expr_gasto_total}) / NULLIF(({expr_volumen}), 0)"

    # =============================
    #   ORDEN DE METRICS (sin WAD/ocupación x remolque/kms/remolques embarcados)
    # =============================
    METRIC_ORDER = [
        "Venta",
        "Volumen ocupación",
        "Valor de la caja (transporte)",
        "%venta",
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

        "FP",
        "PA",
        "Freight program & PA",
    ]

    METRICS = {
        "Venta": expr_venta,
        "Volumen ocupación": expr_volumen,
        "Valor de la caja (transporte)": expr_valor_caja,
        "%venta": expr_pct_venta,
        "Gasto total + BKHL + FP + PA": expr_gasto_total,
        "$/caja transportada": expr_cost_per_box,

        "Tractores (fijos)": expr_tractores,
        "Variable dedicado": expr_variable,
        "Diesel dedicado": expr_diesel,
        "Casetas": expr_casetas,
        "Gasto dedicado": expr_gasto_dedicado,

        "$ de km tercero": expr_km_tercero,
        "Diesel tercero": expr_diesel_tercero,
        "Gasto tercero": expr_gasto_tercero,

        "Remolques": expr_remolques,
        "Quintas": expr_quintas,
        "Remolques y quintas": expr_remolques_quintas,

        "Ferry": expr_ferry,
        "Aclaraciones": expr_aclaraciones,
        "Gastos secundarios (SICI)": expr_gastos_sec,
        "Desconsolidador": expr_desconso,
        "Monitoreo": expr_monitoreo,
        "Transferencias": expr_transfer,
        "Intermodal": expr_intermodal,
        "Otros variables": expr_otros_var,

        "Gasto BKHL": expr_gasto_bkhl,
        "Ingreso BKHL": expr_ingreso_bkhl,
        "Neto BKHL": expr_neto_bkhl,

        "Devoluciones": expr_devoluciones,

        "FP": expr_fp,
        "PA": expr_pa,
        "Freight program & PA": expr_fp_pa,
    }

    # 3) SQL base: leer, limpiar, crear scenario + month_num + region
    month_case = """
    CASE lower(trim("Mes"))
      WHEN 'enero' THEN 1
      WHEN 'febrero' THEN 2
      WHEN 'marzo' THEN 3
      WHEN 'abril' THEN 4
      WHEN 'mayo' THEN 5
      WHEN 'junio' THEN 6
      WHEN 'julio' THEN 7
      WHEN 'agosto' THEN 8
      WHEN 'septiembre' THEN 9
      WHEN 'octubre' THEN 10
      WHEN 'noviembre' THEN 11
      WHEN 'diciembre' THEN 12
      ELSE NULL
    END
    """

    con = duckdb.connect()

    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW clean AS
    SELECT
      *,
      CASE
        WHEN trim("Tipo folio") = 'Business Plan' THEN 'BP'
        WHEN trim("Tipo folio") = 'Real 2025' THEN 'REAL2025'
        WHEN trim("Tipo folio") = 'Forecast actual' THEN 'FCST'
        ELSE 'OTRO'
      END AS scenario,
      TRY_CAST(CAST("Periodo" AS VARCHAR) AS INTEGER) AS year,
      {month_case} AS month_num,
      COALESCE(NULLIF(trim("Region"), ''), 'Total logística') AS region
    FROM read_csv_auto('{RAW_CSV}', union_by_name=True)
    WHERE "Tipo folio" IS NOT NULL;
    """)

    # 4) Construye tabla mensual LONG (metric, value) + region
    union_parts = []
    for metric_name in METRIC_ORDER:
        expr = METRICS.get(metric_name, "0")
        union_parts.append(f"""
        SELECT
          scenario,
          year,
          month_num,
          region,
          "Mes" AS month_name,
          '{metric_name}' AS metric,
          {expr} AS value
        FROM clean
        WHERE year IS NOT NULL AND month_num IS NOT NULL
        GROUP BY scenario, year, month_num, region, month_name
        """)

    monthly_sql = "\nUNION ALL\n".join(union_parts)
    con.execute(f"CREATE OR REPLACE TEMP VIEW monthly AS {monthly_sql};")

    # 5) Derivados: Q / H / FY / YTD desde mensual (con region)
    con.execute("""
    CREATE OR REPLACE TEMP VIEW monthly_labeled AS
    SELECT
      'M' AS period_type,
      printf('%04d-%02d', year, month_num) AS period_label,
      scenario, year, month_num, region, metric, value
    FROM monthly;
    """)

    con.execute("""
    CREATE OR REPLACE TEMP VIEW quarterly AS
    SELECT
      'Q' AS period_type,
      printf('%04d-Q%d', year, ((month_num-1)/3)::INT + 1) AS period_label,
      scenario, year,
      NULL::INT AS month_num,
      region,
      metric,
      SUM(value) AS value
    FROM monthly
    GROUP BY scenario, year, ((month_num-1)/3)::INT + 1, region, metric;
    """)

    con.execute("""
    CREATE OR REPLACE TEMP VIEW halfyear AS
    SELECT
      'H' AS period_type,
      printf('%04d-H%d', year, CASE WHEN month_num <= 6 THEN 1 ELSE 2 END) AS period_label,
      scenario, year,
      NULL::INT AS month_num,
      region,
      metric,
      SUM(value) AS value
    FROM monthly
    GROUP BY scenario, year, CASE WHEN month_num <= 6 THEN 1 ELSE 2 END, region, metric;
    """)

    con.execute("""
    CREATE OR REPLACE TEMP VIEW fullyear AS
    SELECT
      'FY' AS period_type,
      printf('%04d', year) AS period_label,
      scenario, year,
      NULL::INT AS month_num,
      region,
      metric,
      SUM(value) AS value
    FROM monthly
    GROUP BY scenario, year, region, metric;
    """)

    con.execute("""
    CREATE OR REPLACE TEMP VIEW ytd AS
    SELECT
      'YTD' AS period_type,
      printf('%04d-%02d', year, month_num) AS period_label,
      scenario, year, month_num,
      region,
      metric,
      SUM(value) OVER (
        PARTITION BY scenario, year, region, metric
        ORDER BY month_num
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS value
    FROM monthly;
    """)

    final_df = con.execute("""
      SELECT * FROM monthly_labeled
      UNION ALL SELECT * FROM quarterly
      UNION ALL SELECT * FROM halfyear
      UNION ALL SELECT * FROM fullyear
      UNION ALL SELECT * FROM ytd
    """).fetchdf()

    final_df.to_parquet(OUT_PARQUET, index=False)
    print(f"✅ Generado: {OUT_PARQUET} (rows={len(final_df)})")


if __name__ == "__main__":
    main()
