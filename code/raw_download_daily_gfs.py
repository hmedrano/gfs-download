import os, sys
import shutil
import logging as log
import numpy as np
import netCDF4 as nc
import gfsDownload as gD
import datetime as dt
# import notification

"""
 Script de ejecucion diario para la descarga de forzamientos meteorologicos de Nomads GFS.
 Se descargan el pronostico de 6 dias del dataset GFS_HD.
 La informacion se almacena en archivos netcdf.

"""


def main():

    sWorkingDir = './'
    sOutDir = './out'
    sLogFile = 'rawdownload_gfs.log'

    os.chdir(sWorkingDir)
    log.basicConfig(filename=sLogFile, level=log.DEBUG,)

    if len(sys.argv) > 1:
        print (sys.argv[1])
        dToday = dt.datetime.strptime(sys.argv[1],'%Y%m%d')
    else:
        dToday = dt.datetime.today()

    sDirName = dToday.strftime('%Y%m%d')
    myData = gD.gfsData()

    log.info('-----------------------------------------------')
    log.info('Iniciando descarga de la fecha: ' + str(dToday))

    ## Descargamos datos y generamos archivos crudos.
    fileGFS = myData.downloadGFS('gfs_0p25', dToday - dt.timedelta(days=1))
    log.info('Se descargo datos del catalogo GFS en el archivo :: ' + str(fileGFS))

    # Verificar que la informacion exista en sitio
    if fileGFS == None:
        log.error('Fallo la descarga de GFS')
    elif not os.path.exists(fileGFS):
        log.error('Fallo la descarga de GFS, no existe la ruta: ' + str(fileGFS))
    else:
        # Salvar archivos en sOutDir
        if os.path.exists(os.path.join(sOutDir,sDirName)):
            shutil.copy(fileGFS,os.path.join(sOutDir,sDirName))
            os.remove(fileGFS)
        else:
            os.makedirs(os.path.join(sOutDir,sDirName))
            shutil.move(fileGFS,os.path.join(sOutDir,sDirName))

    dProcessTime = dt.datetime.today() - dToday
    log.info('Termino el proceso de descarga, proceso tardo :: ' + str(dProcessTime.seconds/60) + ' minutos ' + str(dProcessTime.seconds % 60) + ' segundos' )
    log.info('-----------------------------------------------')
    return


main()
