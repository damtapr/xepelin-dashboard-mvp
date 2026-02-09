# xepelin-dashboard-mvp
Proyecto de mejora en tiempos de carga y automatización de formulas para la visualización del P&L de transportes dividido por regiones con gráfica de waterfall para presentaciones ejecutivas con directores de centros de distribución.

Instrucciones de uso para ejecutar adecuadamente demo:

> Para que la app muestre datos, **primero hay que cargar el archivo de ejemplo** incluido en este repo. (En la carpeta de samples, usar el de nombre "Base_xepelin_sintetico_vf").

### Paso a paso (2 minutos)
1) Abre el link del prototipo (Streamlit): https://xepelin-dashboard-mvp-24nzjnapp6klikbfskw86wm.streamlit.app/

2) En el menú izquierdo, entra a **“Cargar base”**.

3) Sube el archivo de ejemplo que viene en este repo en la carpeta:
   - `samples/Base_xepelin_sintetico_vf.xlsx`

4) Da click en el botón  **Procesar**.

5) Cuando termine, ve a:
   - **Summary** (comparativos Real 2025 vs BP 2026 vs Forecast 2026 por periodo y región)
   - **Bridge** (waterfall de drivers vs BP hasta Forecast)

✅ **Listo:** ya podrás usar todos los filtros (Periodo / Año / Región / Mes, etc.)

### Nota importante
- Si la app se reinicia o se borra el cache del servidor, puede ser necesario **volver a cargar el archivo** desde “Cargar base”.
