import os, sys
import shutil
import logging as log
import numpy as np
import netCDF4 as nc
import gfsDownload as gD
import datetime as dt

"""
 Script de ejecucion diario para la descarga de forzamientos meteorologicos de Nomads GFS.
 Se descargan 6 dias de datos reanalizados FNL
"""


def main():

    sWorkingDir = './'
    sOutDir = './out'
    sLogFile = 'rawdownload_fnl.log'

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
    fileFNL = myData.downloadFNL(dToday - dt.timedelta(days=2))
    log.info('Se descargo datos del catalogo FNL en el archivo ::: ' + str(fileFNL))

    # Verificar que la informacion exista en sitio
    if fileFNL == None:
        log.error('Fallo la descarga de FNL')
    elif not os.path.exists(fileFNL):
        log.error('Fallo la descarga de FNL, no existe la ruta: ' + str(fileFNL))
    else:
        # Salvar archivos en sOutDir
        if os.path.exists(os.path.join(sOutDir,sDirName)):
            shutil.copy(fileFNL,os.path.join(sOutDir,sDirName))
            os.remove(fileFNL)
        else:
            os.makedirs(os.path.join(sOutDir,sDirName))
            shutil.move(fileFNL,os.path.join(sOutDir,sDirName))

    dProcessTime = dt.datetime.today() - dToday
    log.info('Termino el proceso de descarga, proceso tardo :: ' + str(dProcessTime.seconds/60) + ' minutos ' + str(dProcessTime.seconds % 60) + ' segundos' )
    log.info('-----------------------------------------------')
    return


main()
