import pandas as pd
import numpy as np

RAW = "input/raw_dummy.csv"

MONTHS = [
    "Enero","Febrero","Marzo","Abril","Mayo","Junio",
    "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"
]

KEEP = ["Business Plan", "Real 2025", "Forecast actual"]

def main():
    df = pd.read_csv(RAW)

    # Normaliza strings clave
    df["Tipo folio"] = df["Tipo folio"].astype(str).str.strip()
    df["Mes"] = df["Mes"].astype(str).str.strip().str.title()

    # Qué folios tengo realmente
    print("Tipos folio (antes):")
    print(df["Tipo folio"].value_counts().head(20), "\n")

    # Deja solo los 3 folios esperados (si hay basura, la quitas)
    df = df[df["Tipo folio"].isin(KEEP)].copy()

    # Fuerza años correctos
    df.loc[df["Tipo folio"] == "Real 2025", "Periodo"] = 2025
    df.loc[df["Tipo folio"].isin(["Business Plan", "Forecast actual"]), "Periodo"] = 2026

    # Si Business Plan existe pero viene “vacío” (ej: Ventas = 0/NaN), lo clonamos desde Forecast
    if "Ventas" in df.columns:
        bp_mask = df["Tipo folio"] == "Business Plan"
        bp_sum = pd.to_numeric(df.loc[bp_mask, "Ventas"], errors="coerce").fillna(0).sum()

        if bp_mask.sum() > 0 and bp_sum == 0:
            print("⚠️ Business Plan tiene Ventas=0 (o no numérico). Clonando valores desde Forecast actual...")
            fcst = df[df["Tipo folio"] == "Forecast actual"].copy()
            if fcst.empty:
                fcst = df[df["Tipo folio"] == "Real 2025"].copy()

            # Igualamos cantidad de filas de BP con sample del FCST
            bp_idx = df.index[bp_mask].tolist()
            sampled = fcst.sample(n=len(bp_idx), replace=True, random_state=42)
            sampled["Tipo folio"] = "Business Plan"
            sampled["Periodo"] = 2026

            # Sustituye las filas BP por las sampleadas (mismos índices para no cambiar tamaño)
            sampled.index = bp_idx
            df.loc[bp_idx, :] = sampled.loc[bp_idx, :]

    # Asegura que cada folio tenga TODOS los meses (al menos distribución)
    rng = np.random.default_rng(42)
    for folio in KEEP:
        idx = df.index[df["Tipo folio"] == folio].tolist()
        if not idx:
            continue
        months = (MONTHS * ((len(idx) // 12) + 1))[:len(idx)]
        rng.shuffle(months)
        df.loc[idx, "Mes"] = months

    # Guardar
    df.to_csv(RAW, index=False)

    print("\nTipos folio (después):")
    print(df["Tipo folio"].value_counts(), "\n")
    print("Conteo por (folio, año):")
    print(df.groupby(["Tipo folio", "Periodo"]).size())

if __name__ == "__main__":
    main()
