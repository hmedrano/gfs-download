import os, sys
import shutil 
import logging as log
import numpy as np
import netCDF4 as nc
import gfsDownload as gD
import datetime as dt
import notification

""" 
 Script de ejecucion diario para la descarga de forzamientos meteorologicos de Nomads GFS.
 Se descargan 6 dias de datos reanalizados FNL y el pronostico de 6 dias del dataset GFS_HD.
 La informacion se almacena en archivos netcdf, uno para FNL y otro para GFS_HD, cada uno con 
 distinta resolucion espacial y temporal. 

 By Favio Medrano Julio 2014.
"""

def failNotification():
    msg = 'Fecha ' + str(sDirName) + '\nAlguno de los siguientes archivos no se generaron:\n' + fileFNL + '\n' + fileGFS + '\n'
    log.warning('Enviando notificacion: ' + msg)
    notification.send('Kanik2 GFS Download Status',msg)

def main():

    sWorkingDir = '/LUSTRE/hmedrano/SCRIPTS/forzamientos/gfs-download/code'
    sOutDir = '/LUSTRE/hmedrano/STOCK/FORCING-RAW/GFS_RAW'
    sLogFile = 'rawdownload.log'

    os.chdir(sWorkingDir)
    log.basicConfig(filename=sLogFile, level=log.INFO,)

    if len(sys.argv) > 1:
        print sys.argv[1]
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

    fileGFS = myData.getGFS('gfs_0p25', dToday - dt.timedelta(days=1))
    log.info('Se descargo datos del catalogo GFS en el archivo :: ' + str(fileGFS))

    # Verificar que la informacion exista en sitio
    if fileFNL == None or fileGFS == None:
        failNotification()
    elif ((not os.path.exists(fileFNL)) or (not os.path.exists(fileGFS))):
        failNotification()
    else:
        # Salvar archivos en sOutDir
        if os.path.exists(os.path.join(sOutDir,sDirName)):
            shutil.copy(fileFNL,os.path.join(sOutDir,sDirName))
            shutil.copy(fileGFS,os.path.join(sOutDir,sDirName))
            os.remove(fileFNL)
            os.remove(fileGFS)
        else:
            os.makedirs(os.path.join(sOutDir,sDirName))
            shutil.move(fileFNL,os.path.join(sOutDir,sDirName))
            shutil.move(fileGFS,os.path.join(sOutDir,sDirName))

    dProcessTime = dt.datetime.today() - dToday
    log.info('Termino el proceso de descarga, proceso tardo :: ' + str(dProcessTime.seconds/60) + ' minutos ' + str(dProcessTime.seconds % 60) + ' segundos' )
    log.info('-----------------------------------------------')
    return


main()
