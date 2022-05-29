# Scripts de descarga datos Global Forecast System

Este script automatiza la descarga de los productos de NCEP de pronostico e hincast, GFS y FNL.

## Requerimientos

 - python 3
 - netCDF4 library
 - configparser library

## Quickstart

1. Configurar archivo `gfsconfig.cfg` para seleccionar area de interes y listado de variables a descargar.
2. Configurar rutas particulares en archivo `raw_download_daily.py`, o `raw_download_daily_fnl.py` y
   `raw_download_daily_gfs.py`

```
python raw_download_daily.py
```



