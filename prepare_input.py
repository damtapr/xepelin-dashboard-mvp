import re
import numpy as np
import pandas as pd
from pathlib import Path

SOURCE_XLSX = "Base_xepelin.xlsx"
SHEET = "Base"

ROWS_PER_FOLIO = 1000  # <- lo que pediste
SEED = 42
INSERT_BLANK_ROWS_BETWEEN_BLOCKS = True  
MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]

FOLIOS = [
    {"tipo_folio": "Business Plan",   "periodo": 2026, "mult_mu": 1.00, "mult_sigma": 0.06},
    {"tipo_folio": "Real 2025",       "periodo": 2025, "mult_mu": 0.98, "mult_sigma": 0.10},
    {"tipo_folio": "Forecast actual", "periodo": 2026, "mult_mu": 1.02, "mult_sigma": 0.05},
]

def main():
    np.random.seed(SEED)
    Path("input").mkdir(exist_ok=True)

    # Header real en tu archivo está en la fila 3 -> header=2
    df = pd.read_excel(SOURCE_XLSX, sheet_name=SHEET, header=2)

    # Quita columnas tipo "Unnamed"
    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", na=False)]

    # Asegura que existan estas columnas (con esos nombres exactos en tu base)
    required = ["Tipo folio", "Mes", "Periodo"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"No encontré la columna requerida: '{col}'. Columnas disponibles: {list(df.columns)[:20]}...")

    # Tipos
    num_cols = df.select_dtypes(include=["number"]).columns.tolist()
    cat_cols = [c for c in df.columns if c not in num_cols]

    # Si tu base solo tiene 3 filas ejemplo, las usamos como “plantillas”
    templates = df.copy()

    def month_list_for(n):
        # reparte meses de forma uniforme (aprox) y luego barajea
        months = (MONTHS * (n // len(MONTHS) + 1))[:n]
        np.random.shuffle(months)
        return months

    out_blocks = []
    for fol in FOLIOS:
        months = month_list_for(ROWS_PER_FOLIO)
        block_rows = []

        for i in range(ROWS_PER_FOLIO):
            r = templates.sample(1, replace=True).iloc[0].copy()

          
            r["Tipo folio"] = fol["tipo_folio"]
            r["Periodo"] = fol["periodo"]
            r["Mes"] = months[i]

            # Variación numérica ligera para que “no sean clones”
            mult = np.random.normal(fol["mult_mu"], fol["mult_sigma"])
            for c in num_cols:
                v = r[c]
                if pd.isna(v):
                    continue

                # ruido proporcional chico
                noise = np.random.normal(0, max(1.0, abs(float(v)) * 0.02))
                new_v = float(v) * mult + noise

                # netos/ajustes pueden ser negativos; el resto clamped a >=0
                c_low = str(c).lower()
                allow_negative = any(k in c_low for k in ["neto", "bkhl", "devo", "li", "ajuste", "diff", "vari"])
                r[c] = new_v if allow_negative else max(0.0, new_v)

            block_rows.append(r)

        block_df = pd.DataFrame(block_rows)

        # Opcional: agrega una fila en blanco “como en Excel”
        if INSERT_BLANK_ROWS_BETWEEN_BLOCKS:
            blank = {c: (np.nan if c in num_cols else None) for c in block_df.columns}
            block_df = pd.concat([block_df, pd.DataFrame([blank])], ignore_index=True)

        out_blocks.append(block_df)

    out_df = pd.concat(out_blocks, ignore_index=True)

    out_df.to_csv("input/raw_dummy.csv", index=False)
    print(f"✅ Generado input/raw_dummy.csv con {out_df.shape[0]} filas y {out_df.shape[1]} columnas")

if __name__ == "__main__":
    main()
