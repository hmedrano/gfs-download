'''
Herramienta de linea de comandos para convertir archivos netcdf con unidades
de tiempo "days since 0001-01-01 00:00:00" a "seconds since 1970-01-01 00:00:00"

Uso:

 > python fixtimeunits.py out/20220528/crudosGFS_0P25_2022-05-28_00z.nc

'''

import sys
import netCDF4 as nc
import datetime as dt

def fixtimeunits(ncFile):
  '''
  Los datos descargados de GFS, vienen con la variable de tiempo con unidades "days since 0001-01-01 00:00:00"
  que es una unidad no valida en el calendario gregoriano. Por lo tanto esta funcion corrige este detalle
  y cambia las unidades a "seconds since 1970-01-01 00:00:00", y tambien los valores.
  '''

  dst = nc.Dataset(ncFile,'a')
  if dst.variables['time'].units == 'seconds since 1970-01-01 00:00:00':
    dst.close()
    return
  else:
    timedts = nc.num2date(dst.variables['time'][:], 'days since 0001-01-01 00:00:00', calendar='gregorian')
    # fix 1 day offset
    timedts = [ tdt - dt.timedelta(days=1) for tdt in timedts ]
    newnumdates = nc.date2num(timedts, 'seconds since 1970-01-01 00:00:00', calendar='gregorian')
    # Update units and values
    dst.variables['time'][:] = newnumdates
    dst.variables['time'].units = 'seconds since 1970-01-01 00:00:00'
    dst.variables['time'].calendar = 'gregorian'
    dst.close()
    print ('Updated time values and units, to "seconds since 1970-01-01 00:00:00" ')
    print ('Times: ' + str(timedts))



if __name__ == "__main__":
  if len(sys.argv) > 1:
    ncFile = sys.argv[1]
  else:
    print('Incluya el nombre del archivo a arreglar')

  fixtimeunits(ncFile)
