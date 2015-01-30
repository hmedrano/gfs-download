"""
 Clases: 
  Auxiliares:
  netcdfFile : Clase que se encarga de la creacion de archivos netcdf, recibiendo como parametros python diccionarios con las dimensiones
               variables y atributos para su creacion.
               Cuenta con metodos para crear archivo, crear dimensiones, crear variables y salvar datos.


 Descarga datos GFS:
 El siguiente grupo de clases, son las encargadas de realizar la operacion de descarga de variables de GFS y FNL
 
  gfsConfig  : Clase encargada de leer las entradas al archivo de configuracion para la descarga de GFS y FNL ("gfsconfig.cfg") 
  gfsName    : Esta clase contiene el metodo para crear la URL del dataset FNL o GFS_HD que se intenta acceder en base a la fecha y run_time
  gfsSubgrid : Esta clase se encarga de obtener los indices junto longitudes,latitudes que corresponden a los parametros especificados para la malla
               que se va a descargar
  gfsData    : Esta clase contiene los metodos para descargar variables junto a sus variables de dimension, para los datasets FNL y GFS_HD.
               Contiene metodos para guardar esta informacion en archivos netcdf con convensiones correctas.


 Favio Medrano hmedrano@cicese.mx 
 Ultima actualizacion: 10Oct2014

 31 Julio 2014 : Se incluyo un sistema para controlar el fallo de la descarga de la informacion, aun despues de 10
                 intentos de descarga. En estos casos, los metodos downloadFNL y downloadGFS_HD en lugar de regresar el nombre
                 del archivo descargado, retornan None.
 10 Octu 2014  : Revision del codigo que descarga variables "getData" y cambios en el tipo de salida logging.

 22 Jan 2015   : Se cambio funcion downloadGFS_HD por getGFS, con extra parametro offset y s_dataset.
                 
"""
__author__ =  'Favio Medrano'
__version__=  '1.0'

from ConfigParser import ConfigParser
import os
import logging as log
import datetime as dt
import netCDF4 as nc 
import numpy as np
import time


class gfsConfig:
        """
         Clase padre, que se encarga de cargar el archivo de configuracion gfsconfig.cfg
         Contiene metodos para acceder a las entradas del archivo de configuracion.
        """
        configfile = 'gfsconfig.cfg'
        configData = None        

        def __init__(self):
                # Leer archivo de configuracion
                self.readConfig()

        def readConfig(self):
                """
                 Funcion que lee el archivo de configuracion, llamada desde el constructor de la clase.
                """
                self.configData = ConfigParser()
                if os.path.exists(self.configfile):
                        self.configData.read(self.configfile)
                        return 1
                else:
                        log.warning('Archivo de configuracion no existe! (' + self.configfile + ')')
                        return -1
                        
        def getKeyValue(self,key,value):
                try:
                    if self.configData != None:
                        rvalue = self.configData.get(key,value)
                        log.debug('Llave : ' + value + ' Atributo leido: ' + str(rvalue))
                        return rvalue
                    else:
                        log.warning('Configuracion: ' + value + ', no existe en el archivo de configuracion!')
                        return ''
                except:
                    return ''
                                             
        def getConfigValue(self,value):
                """ 
                Devuelve la los datos de la llave "value", para el grupo 'gfs_data'    
                """  
                return self.getKeyValue('gfs_data',value)
             
        def getConfigValueV(self,value):
                """ 
                Devuelve la los datos de la llave "value", para el grupo 'variables'    
                """   
                return self.getKeyValue('variables',value)   
        

        def getConfigValueVL(self,value):
                """ 
                Devuelve los datos de la llave "value", que vienen en formato de lista separada por comas
                regresa, un <python list> con los datos.    
                """            
                raw = self.getKeyValue('variables',value)
                return [ e.strip() for e in raw.replace('\n','').split(',') ]
             
        def datasetExists(self,fname):
                try:
                    dst = nc.Dataset(fname,'r') 
                    dst.close() 
                    return True
                except:
                    return False   
                

class netcdfFile():
        """
         Clase netcdfFile
         Se encarga de crear rapidamente archivos netcdf, enviandole como parametros datos 
         de dimensiones y variables en formato de python dictionary.                
        """
        fileHandler = None 
        fileName = None 

        def __del__(self):
            # Nos aseguramos que el archivo se cierre correctamente.
            self.closeFile()

        def readFile(self,filename,path=''):
            """
             Funcion que lee el contenido de un archivo netCDF, y lo regresa en formato <python dict>
             Limitamos el tamano del archivo a leer a MB
             Formato salida:
              { 'dimensions' : { 'dim1' : value1 , 'dim2' : value2 ... } 
                'variables'  : {}
              }
            """
            if self.fileHandler != None:
                log.warning('readFile: Actualmente se encuentre un archivo netcdf abierto : ' + self.fileName)
                return None 
            if os.path.getsize(os.path.join(path,filename)) > 100000000L:
                log.warning('readFile: Archivo que se quiere leer es demasiado grande, para leerse completo.')
                return None 
            
            try:
                self.fileHandler = nc.Dataset(os.path.join(path,filename),'r')
            except Exception, e:
                log.warning('readFile: Se detecto un error al leer el archivo ' + filename)
                log.warning('readFile: ' + str(e))
                return None 
            self.fileName = os.path.join(path,filename) 
            self.closeFile()
            

        def createFile(self,filename,path='',filetype='NETCDF4'):
            """
             Recibe como diccionario los datos de las dimensiones
             Formato: {'dim' : value , 'dim2' : value2 .... }
            """            
            self.fileName = os.path.join(path,filename) 
            try:
                self.fileHandler = nc.Dataset(filename,'w',filetype)   
            except Exception, e:
                log.warning('Se detecto un error al crear el archivo: ' + filename )
                log.warning(str(e))
                return -1
            
        def closeFile(self):
            """
             Funcion que cierra el archivo netcdf, si es que ya se creo.
             Agrega un atributo global "history" donde indica la fecha de creacion.
            """
            if self.fileHandler == None:
                return -1
            #self.fileHandler.history = 'File created ' + dt.datetime.today().strftime('%Y-%m-%d %I:%M:%S %p') + '.'
            self.fileHandler.close() 
            self.fileHandler = None
            self.fileName = None 
            return 0 
                     
        def createDims(self,dimDict):
            """
             Funcion para crear dimensiones del archivo netcdf
             Recibe como parametro dimDict tipo <python dic>, con el siguiente formato:
              {'dimension' : 10 , 'dimension2' : 40 , 'dimension3' : None } 
             Donde "None" hace a una dimension "unlimited"
            """
            if self.fileHandler == None:
                log.warning('Primero es necesario crear el archivo, con el metodo .create')
                return -1
            if dimDict != None:
                for d in dimDict.keys():
                    self.fileHandler.createDimension(d,dimDict[d]) 
            return 0
        

        def createVars(self,varsDict):
            """
             Recibe como <python dict> los datos de las variables, nombres y atributos
             Formato: { 'varName1' : { 'dimensions' : ['dim1','dim2'...] , 'attributes' : {'atribute1':value,'atribute2':'value2'}, 'dataType' : value }  , 
                        'varName2' : { 'dimensions' : ['dim1','dim2'...] , 'attributes' : {'atribute3':value,'atribute4':'value2'}, 'dataType' : value }  ..... }
            """            
            def cleanVar(v):
                if type(v) == type('str'):
                    return v.strip() 
                else:
                    return (v) 
            
            if self.fileHandler == None:
                log.warning('createVars: Primero es necesario crear el archivo, con el metodo .create')
                return -1
            if varsDict != None:
                for v in varsDict.keys():
                    log.debug('createVars: procesando variable: ' + v)
                    dimtuple = tuple(varsDict[v]['dimensions'])
                    try: 
                        # Crear variable
                        try: 
                            fillv = cleanVar(varsDict[v]['attributes']['_FillValue'])
                        except:
                            fillv = None
                        varH = self.fileHandler.createVariable(v.strip(),varsDict[v]['dataType'],dimtuple,fill_value=fillv)
                        # Agregar los atributos
                        for att in varsDict[v]['attributes'].keys():
                            if att == 'units':
                                varH.units = cleanVar(varsDict[v]['attributes']['units'])
                            elif att == 'long_name':
                                varH.long_name =  cleanVar(varsDict[v]['attributes']['long_name'])
                            elif att == 'time_origin':
                                varH.time_origin =  cleanVar(varsDict[v]['attributes']['time_origin'])  
                            elif att == 'missing_value':
                                varH.missing_value =  (cleanVar(varsDict[v]['attributes']['missing_value'])) 
                            elif att == 'add_offset':
                                varH.add_offset = (cleanVar(varsDict[v]['attributes']['add_offset']))
                            elif att == 'calendar':
                                varH.calendar = cleanVar(varsDict[v]['attributes']['calendar'])
                            elif att == '_FillValue':
                                pass
                            else:
                                log.warning('crateVars: Atributo ' + att + ', no es valido')
                        log.debug('createVars: Variable ' + v + ' , creada con todos sus atributos')
                    except Exception, e:
                        log.warning('createVars: Fallo al crear la variable : ' + v)
                        log.warning('createVars: Archivo netcdf: ' + self.fileName)
                        log.warning(str(e))
                        return -1 
            return 0
        

        def saveData(self,varDataDict):
            """
             Se encarga de guardar los arreglos de datos a sus variables correspondientes en el archivo netcdf.
             Formato: 
              {'varname1' : np.arrray[:,:,:] , 'varname2' : np.arrray[:,:,:] , ... }
            """            
            if self.fileHandler == None:
                log.warning('saveData: Primero es necesario crear el archivo, con el metodo .create')
                return -1
            if varDataDict != None:
                for v in varDataDict.keys():
                    log.debug('saveData: Intento de salvar datos de variable : ' + v)
                    try:
                        varH = self.fileHandler.variables[v] 
                        varH[:] = varDataDict[v][:] 
                        log.debug('saveData: Exitoso!')        
                    except Exception, e:
                        log.warning('saveData: Fallo al intentar salvar datos en variable: ' + v)
                        log.warning('saveData: ' + str(e))
                        return -1   
            
            return 0
        
        def saveDataS(self,varName,data,indexs):
            """
             Se encarga de guardar los datos "data" en la variable "varName" en los indices marcados por "indexs"
             
            """        
            if self.fileHandler == None:
                log.warning('saveData: Primero es necesario crear el archivo, con el metodo .create')
                return -1
            # La variable varName existe ?    
            try: 
                log.debug('saveData: Intento de salvar datos de variable : ' + varName)
                varH = self.fileHandler.variables[varName] 
                varH[indexs] = data 
                log.debug('saveDataS: Existoso!')
            except Exception, e:
                log.warning('saveDataS: Fallo al intentar salvar datos en variable: ' + varName)
                log.warning('saveDataS: ' + str(e))
                return -1
                
            return 0
                        
      
                
                

class gfsName(gfsConfig):
        """ 
         Clase gfsName hija de gfsConfig
         Se encarga de generar el url con el nombre del dataset gfs o fnl en base a la 
         fecha y el numero de corrida de GFS.
        """    
     
        def getrootURL(self):
                return self.getConfigValue('url')
        
        
        def getURLName(self, time, gfs_run_time, gfstype):
                """
                 Time es un dato fecha <python datetime>, gfs_run_time especifica que dataset runtime (0 = 00z, 6 =06z, etc)
                 gfstype, es el dataset que se busca, puede ser 'gfs_hd' o 'fnl'   
                """              
                gfsname = ''
                if gfstype == 'gfs_hd':  # 1
                    gfsname = 'gfs_hd/gfs_hd'
                    gfsname1='gfs_hd'
                elif gfstype == 'gfs_0p50':
                    gfsname = 'gfs_0p50/gfs'
                    gfsname1 = 'gfs_0p50'
                elif gfstype == 'gfs_0p25':
                    gfsname = 'gfs_0p25/gfs'
                    gfsname1 = 'gfs_0p25'
                else:
                    gfsname = 'fnl/fnl'  # 0
                    gfsname1='fnlflx'
                strdate = "%d%02d%02d" % (time.year,time.month,time.day)
                gfsdir = self.getrootURL() + '/dods/' + gfsname + strdate + '/' 
                fname = gfsdir + gfsname1 + '_' + ("%02d"%gfs_run_time) + 'z'
                return fname 
                


class gfsSubgrid(gfsConfig):
        """
         Clase gfsSubgrid hija de gfsConfig
         Esta clase se encarga de obtener los indices que corresponden a los parametros
         latmin, lonmin, latmax, lonmax. 
         Tambien obtiene las latitudes y longitudes de uno de los datasets que se provean.
        """    
        longitudes = None 
        irange = None
        
        latitudes = None
        jrange = None
        
 
        def getGFSgrid_default(self,fname):
            """
             Regresa los indices para los valores lonmin lonmax latmin latmax
             leidos del archivo de configuracion (DEFAULT)  
            """ 
            lonmin = float(self.getConfigValue('lonmin'))
            lonmax = float(self.getConfigValue('lonmax'))
            latmin = float(self.getConfigValue('latmin'))
            latmax = float(self.getConfigValue('latmax')) 
            self.getGFSgrid(fname, lonmin, lonmax, latmin, latmax)   
                    
        def getGFSgrid(self,fname,lonmin,lonmax,latmin,latmax):
            dl=1;
            lonmin=lonmin-dl
            lonmax=lonmax+dl
            latmin=latmin-dl
            latmax=latmax+dl
            try:
                dataset = nc.Dataset(fname,'r')
            except Exception, e:
                log.warning('getGFSgrid: No se encontro el dataset: ' + fname) 
                log.warning('getGFSgrid: ' + str(e))
                return 0 
            
            lon = dataset.variables['lon'][:]
            lat = dataset.variables['lat'][:] 
            dataset.close()
            
            # Obtener la sub malla
            # 1. Longitud, (lidiar con greenwitch)

            i1 = np.where((lon-360>=lonmin) & (lon-360<=lonmax))[0]
            i2 = np.where((lon>=lonmin) & (lon<=lonmax))[0]
            i3 = np.where((lon+360>=lonmin) & (lon+360<=lonmax))[0]

            self.longitudes=np.concatenate(((lon[i1]-360 if i1.size>0 else []) ,
                                            (lon[i2] if i2.size>0 else []) ,
                                            (lon[i3]+360) if i3.size>0 else [] ),axis=0)
            
            if i1.size > 0:
                i1min=np.min(i1)
                i1max=np.max(i1)
            else:
                i1min=np.array([])
                i1max=np.array([])
                
            if i2.size > 0:
                i2min=np.min(i2)
                i2max=np.max(i2)
            else:
                i2min=np.array([])
                i2max=np.array([])
                
            if i3.size > 0:
                i3min=np.min(i3)
                i3max=np.max(i3)
            else:
                i3min=np.array([])
                i3max=np.array([])
                
            if i1min.size > 0:
                self.irange = np.array([i1min,i1max+1])
            elif i2min.size > 0:
                self.irange = np.array([i2min,i2max+1])
            elif i3min.size > 0:
                self.irange = np.array([i3min,i3max+1]) 
                
            # 2. Latitud 
            j = np.where((lat>=latmin)& (lat<=latmax))
            self.latitudes = lat[j]
             
            jmin=np.min(j)
            jmax=np.max(j)
            self.jrange = np.array([jmin,jmax+1])
            

class gfsData(gfsConfig):
            """
             Esta clase contiene los metodos para consultar y descargar variables de los datasets FNL y GFS_HD junto a sus 
             variables de dimension.
             Contiene metodos para guardar esta informacion en archivos netcdf con la convencion CF.
             Los metodos principales para descargar FNL y GFS, usan la clase gfsConfig para leer del archivo de configuracion
             la region, y variables que interesa descargar.
             Metodos : downloadFNL y downloadGFS_HD
            """
            
            gridFNL = gfsSubgrid() 
            gridGFS_HD = gfsSubgrid()
            gfs_run_time_GFS = None 
            gfs_date_GFS = None 
            

            def findForecast(self):
                """
                 TODO: Esta funcion tiene alguna utilidad? 
                 Verifica si esta disponible un forecast
                """ 
                dataURL = gfsName()
                today = dt.datetime.today()
                
                # Run time que se busca idealmente
                gfs_run_time0=18
                gfs_date0 = today - dt.timedelta(days=0.5) 
                gfs_run_time= gfs_run_time0
                gfs_date= gfs_date0 ;
                foundfile=0 
                dst = None
                while foundfile==0:
                    fname = dataURL.getURLName(gfs_date, gfs_run_time, 'gfs_hd')
                    log.info('Leyendo el catalogo: ' + fname)
                    try:
                        dst = nc.Dataset(fname,'r')
                        foundfile=1
                    except Exception, e:
                        log.warning('Error al tratar de acceder al dataset: ' + str(e) )
                        foundfile=0
                    
                    if foundfile and dst != None:
                        log.info('Se encontro el dataset! (' + fname + ')')
                    else:
                        foundfile=0 
                        log.info('No se encontro el dataset: ' + fname) 
                        gfs_run_time=gfs_run_time-6 
                        if gfs_run_time < 0:
                            gfs_date = gfs_date - dt.timedelta(days=1)
                            gfs_run_time=18
                            if gfs_date < (gfs_date0 - dt.timedelta(days=8)):
                                log.warning('No se encontro ningun dataset de GFS!')
                                return ''
                    
                return fname
            
            def getDataVector(self,fname,var,attemps=10):
                """
                 Funcion que hace la conexion al dataset remoto fname, para descargar una variable 1D 
                """    
                # Por default 10 intentos para descargar datos                       
                for attemp in range(0,attemps):
                    try:
                        dst = nc.Dataset(fname,'r')
                        rawdata = dst.variables[var][:] 
                        dst.close()
                        return rawdata
                    except Exception, e:
                        log.warning('getDataVector: Fallo al acceder a los datos ' + fname + ', var: ' + var)
                        log.warning('getDataVector: Error: ' + str(e))
                        log.warning('getDataVector: Reintentando la descarga. attemp: ' + str(attemp))
                        if attemp > 6:
                            # En los ultimos tres intentos dormir el proceso diez segundos
                            time.sleep(10)
                        continue    
                log.error('getDataVector: No se pudo realizar la descarga de datos : ' + fname + ', var: ' + var)
                raise
                return None                     


            def getData(self,fname,var,trange,irange,jrange, attemps=10):
                """
                 Funcion que hace la conexion al dataset remoto fname, para descargar una variable 3D con los rangos
                 trange (tiempo) , irange (longitud) , jrange (latitud)
                """
                # Por default 10 intentos para descargar datos
                for attemp in range(0,attemps):
                    try:
                        # Tratar de obtener datos del dataset remoto "fname" 
                        # Nota: el orden de las dimensiones son time,   latitudes, longitudes
                        #                                       trange  jrange     irange
                        dst = nc.Dataset(fname,'r')
                        if (var in dst.variables):
                            rawdata = dst.variables[var][trange[0]:trange[1],jrange[0]:jrange[1],irange[0]:irange[1]]
                            log.debug('getData: Variable con shape: ' + str(rawdata.shape))
                            dst.close()
                        else:
                            log.warning('getData: Variable ' + var + ' no se encuentra en el dataset: ' + fname)
                            dst.close()
                            return None
                        return rawdata 
                    except Exception ,e:
                        log.warning('getData: Fallo al acceder a los datos ' + fname + ', var: ' + var)
                        log.warning('getData: Error: ' + str(e))
                        log.warning('getData: Reintentando la descarga. attemp: ' + str(attemp))
                        if attemp > 6:
                            # En los ultimos tres intentos dormir el proceso diez segundos
                            time.sleep(10) 
                        continue 
                log.error('getData: No se pudo realizar la descarga de datos : ' + fname + ', var: ' + var)
                raise 
                return None
                        
            

            def downloadFNL(self,lastDownloadDate=None,hdays=None,saveData=True):
                """
                 Descarga datos del dataset FNL, de la fecha [ today-1day-hdays : today-1day ]
                 El codigo recorre las corridas diarias de las 00Z 06z 12z y 18z descargando los datos de cada uno de estos datasets 
                  ---
                 Si se especifica el parametro lastDownloadDate (fecha tipo <python datetime>), la fecha "today" se cambia por la dada en esta variable. 
                 Si se especifica el parametro hdays, en lugar de leer el dato del archivo de configuracion, se usa este.
                """                   
                # Obtener la submalla 
                dataURL = gfsName()
                today = dt.datetime.today()
                # Hasta esta fecha se descargaran datos de FNL, empezando de 
                # (today - hdays)  :  lastFNLdate
                if lastDownloadDate==None:
                    lastFNLdate = today - dt.timedelta(days=1)
                else:
                    lastFNLdate = lastDownloadDate
                # Construimos el nombre del primer dataset
                run_time = 0  
                if hdays==None:
                    hdays0 = int(self.getConfigValue('hdays'))
                else:
                    hdays0 = int(hdays)
                fnl_date_start = lastFNLdate - dt.timedelta(days=hdays0)
                fname = dataURL.getURLName(fnl_date_start, run_time, 'fnl')
                
                # El siguiente ciclo busca el primer dataset disponible, empezando el ciclo
                # desde (lastFNLdate-hdays), recorriendo los run times 0, 6, 12, 18 
                while True:
                    if self.datasetExists(fname):
                        break 
                    else:
                        if fnl_date_start >= lastFNLdate:
                            log.warning('FNL: No se encontro ningun dataset FNL, errores probables: (No hay red, falla sistema opendap de nomads)')
                            return 0 
                        run_time = run_time + 6 
                        if run_time > 18:
                            fnl_date_start = fnl_date_start + dt.timedelta(days=1)
                            run_time=0
                        fname = dataURL.getURLName(fnl_date_start, run_time, 'fnl')
                
                # En este punto obtenemos el primer dataset disponible desde today-hdays
                # y aseguramos que de aqui podemos obtener los datos de la submalla
                log.info('FNL: Obteniendo el tamano de la malla FNL del dataset: ' + str(fname))
                self.gridFNL.getGFSgrid_default(fname) 
                
                # Definir espacios para variables a descargar
                lVars = self.getConfigValueVL('vars')
                dimTimeSize = (((lastFNLdate - fnl_date_start).days + 1) * 4) - (int(run_time/6))
                varShape = [dimTimeSize, self.gridFNL.jrange[1]-self.gridFNL.jrange[0] , self.gridFNL.irange[1]-self.gridFNL.irange[0]]
                log.debug('FNL: var grid size: ' + str(varShape))
                log.debug('FNL: irange ' + str(self.gridFNL.irange))
                log.debug('FNL: jrange ' + str(self.gridFNL.jrange))
                varlist= {}  
                for v in lVars:
                    varlist[v] =  np.zeros( (varShape[0] , varShape[1], varShape[2]) ) 
                    
                # Recorrer datasets y descargar variables que se solicitan en el archivo de 
                # configuracion 
                run_time_fnl0 = run_time 
                date_fnl0 = fnl_date_start 
                timecnt = 0
                fnlTimeVar = np.zeros((dimTimeSize))
                
                while True:
                    log.info('FNL: Leyendo del dataset : ' + fname)
                    # Obtener primero el valor de la dimension temporal del dataset fname
                    try:
                        fnlTimeVar[timecnt] = self.getDataVector(fname, 'time')   
                    except Exception ,e:
                        log.error('FNL: Fallo la descarga de la variable time del dataset: ' + str(fname))
                        return None 
                    log.info('FNL: Tiempo FNL : ' + str(fnlTimeVar[timecnt]))
                    for vn in range(len(lVars)):
                        try:
                            varlist[lVars[vn]][timecnt,:,:] = self.getData(fname, lVars[vn], [0,1], self.gridFNL.irange, self.gridFNL.jrange)
                            log.info('FNL: Se descargo la variable ' + lVars[vn] + ', shape: '  + str(varlist[lVars[vn]][timecnt,:,:].shape))
                        except Exception ,e:
                            log.error('FNL: Fallo la descarga de una seccion del dataset: ' + str(fname))
                            return None

                        
                    # Avanzar en el siguiente paso de tiempo, construyendo el nombre del siguiente dataset de fnl     
                    timecnt = timecnt + 1
                    run_time_fnl0 = run_time_fnl0 + 6
                    if run_time_fnl0 > 18:
                        date_fnl0 = date_fnl0 + dt.timedelta(days=1) 
                        run_time_fnl0 = 0
                    # Extraer datos hasta la fecha lastFNLdate
                    if date_fnl0 > lastFNLdate:
                        break  
                    fname = dataURL.getURLName(date_fnl0, run_time_fnl0, 'fnl')
                    
                # Una vez descargados todas las variables en la lista de np.arrays varlist
                # decidimos que hacer con la informacion, la regresamos o la salvamos. 
                if saveData:
                    # Dimensiones time(unlimited),   lat,                       lon 
                    #             None               gridFNL.latitudes.size     gridFNL.longitudes.size 
                    
                    dimsA = {'time': None , 'lat': self.gridFNL.latitudes.size, 'lon': self.gridFNL.longitudes.size }
                    dimVars = { 'time' : { 'dimensions': ['time']  , 'attributes' : {'units':'days since 0000-01-01 00:00:00', 'time_origin' : '0000-01-01 00:00:00', 'calendar' : 'ISO_GREGORIAN'} , 'dataType' : 'f8' }  
                               ,'lat' :  { 'dimensions': ['lat']   , 'attributes' : {'units':'degree_north'} , 'dataType' : 'f8' }  
                               ,'lon' :  { 'dimensions': ['lon']   , 'attributes' : {'units':'degree_east'}  , 'dataType' : 'f8' }  }
                    dimVarData = {'time' : fnlTimeVar , 'lat' : self.gridFNL.latitudes , 'lon' : self.gridFNL.longitudes }
                    dataVars= {}
                    # Sacar informacion de unidades y nombres largos del archivo de configuracion.
                    vUnit = self.getConfigValueVL('units')
                    vLN = self.getConfigValueVL('longnames')
                    for vi in range(len(lVars)): 
                        dataVars[lVars[vi]] =  {'dimensions': ['time','lat','lon'] , 'attributes' : {'units' : vUnit[vi], 'long_name' : vLN[vi] , '_FillValue' : 9.999e+20 } , 'dataType' : 'f4' } 
                    
                    myfile = netcdfFile()
                    netcdfFilename = 'crudosFNL_' + fnl_date_start.strftime('%Y-%m-%d') + '__' + lastFNLdate.strftime('%Y-%m-%d') + '.nc'
                    myfile.createFile(netcdfFilename)
                    myfile.createDims(dimsA)
                    myfile.createVars(dimVars)
                    myfile.createVars(dataVars)
                    myfile.saveData(dimVarData)
                    myfile.saveData(varlist)
                    myfile.closeFile()
                else:
                    # TODO: Regresar las variables descargadas en formato diccionario
                    pass

                return netcdfFilename
            

            def getGFS(self, s_dataset, gfs_hd_date=None, run_time=0, offset=-1, saveData=True):
                """                  
                 Descarga datos del dataset especificado en s_dataset (gfs_hd, gfs_0p25 o gfs_0p50), por default toda la dimension 
                 temporal del dataset (today-1), runtime=00z.  
                 El codigo recorre la dimension temporal del dataset, haciendo la descarga en pasos. 
                  ---
                 Recibe parametros opcionales gfs_hd_date (fecha tipo <python datetime>) que es la fecha del dataset del que se intentara hacer la descarga
                 En el parametro run_time se especifica el dataset run_time, default 0, puede ser 0 , 6 , 12 , 18
                """
                """                  
                 Descarga datos del dataset especificado en s_dataset (gfs_hd, gfs_0p25 o gfs_0p50), por default toda la dimension 
                 temporal del dataset (today-1), runtime=00z.  
                 El codigo recorre la dimension temporal del dataset, haciendo la descarga en pasos. 
                  ---
                 Recibe parametros opcionales gfs_hd_date (fecha tipo <python datetime>) que es la fecha del dataset del que se intentara hacer la descarga
                 En el parametro run_time se especifica el dataset run_time, default 0, puede ser 0 , 6 , 12 , 18
                """
                # Si no se especifica gfs_hd_date, usar hoy-1
                if gfs_hd_date==None:
                    gfs_hd_date = dt.datetime.today() - dt.timedelta(days=1)
                # Construir el nombre del dataset que buscamos
                dataURL = gfsName()
                fname = dataURL.getURLName(gfs_hd_date, run_time, s_dataset) # Instead of gfs_hd
                if not self.datasetExists(fname):
                    log.warning('GFS_HD:: Dataset: ' + fname + ' no se encuentra.')
                
                # Obtenemos el tamano de la malla de lo que vamos a descargar, por default los datos de la malla se obtienen del archivo de 
                # configuracion
                log.info('GFS_HD: Obteniendo el tamano de la malla GFS_HD del dataset: ' + str(fname))
                if offset > 0:
                    log.info('GFS_HD: Descargando hasta el registro: ' + str(offset))

                self.gridGFS_HD.getGFSgrid_default(fname) 
                try:
                    gfsTimeVar = self.getDataVector(fname, 'time')
                except Exception, e:
                    log.error('GFS_HD: Fallo la descarga de la variable time seccion del dataset: ' + str(fname))
                    return None
                
                if not (gfsTimeVar is None):                
                    # Definir espacios para variables a descargar
                    lVars = self.getConfigValueVL('vars')   
                    if (offset > 0) and (offset < gfsTimeVar.size):                
                        dimTimeSize = offset
                    else:
                        dimTimeSize = gfsTimeVar.size 
                    varShape = [dimTimeSize, self.gridGFS_HD.jrange[1]-self.gridGFS_HD.jrange[0] , self.gridGFS_HD.irange[1]-self.gridGFS_HD.irange[0]]                    
                    log.debug('GFS_HD: var grid size: ' + str(varShape))
                    log.debug('GFS_HD: irange ' + str(self.gridGFS_HD.irange))
                    log.debug('GFS_HD: jrange ' + str(self.gridGFS_HD.jrange))
                    # Creamos el diccionario de las variables con sus tamanos listos
                    varlist= {}  
                    for v in lVars:
                        varlist[v] =  np.zeros( (varShape[0] , varShape[1], varShape[2]) )                
                        
                    for t in range(gfsTimeVar.size):
                        if (offset < 0) or (t < offset):
                            for vn in range(len(lVars)):
                                try:
                                    varlist[lVars[vn]][t,:,:] = self.getData(fname, lVars[vn], [t,t+1], self.gridGFS_HD.irange, self.gridGFS_HD.jrange)
                                    log.info('GFS_HD: Se descargo la variable ' + lVars[vn] + ', shape: '  + str(varlist[lVars[vn]][t,:,:].shape) + ' , Time step : ' + str(t)) 
                                except Exception ,e:
                                    log.error('GFS_HD: Fallo la descarga de una seccion del dataset: ' + str(fname))
                                    return None
                        else: 
                            log.info('GFS_HD: Downloading only at offset limit: ' + str(offset))
                            break 

                    # Una vez descargados todas las variables en la lista de np.arrays varlist
                    # decidimos que hacer con la informacion, la regresamos o la salvamos. 
                    
                    if saveData:
                        # Dimensiones time(unlimited),   lat,                       lon 
                        #             None               gridFNL.latitudes.size     gridFNL.longitudes.size 
                        dimsA = {'time': None , 'lat': self.gridGFS_HD.latitudes.size, 'lon': self.gridGFS_HD.longitudes.size }
                        dimVars = { 'time' : { 'dimensions': ['time']  , 'attributes' : {'units':'days since 0000-01-01 00:00:00', 'time_origin' : '0000-01-01 00:00:00', 'calendar' : 'ISO_GREGORIAN'} , 'dataType' : 'f8' }  
                                   ,'lat' :  { 'dimensions': ['lat']   , 'attributes' : {'units':'degree_north'} , 'dataType' : 'f8' }  
                                   ,'lon' :  { 'dimensions': ['lon']   , 'attributes' : {'units':'degree_east'}  , 'dataType' : 'f8' }  }
                        dimVarData = {'time' : gfsTimeVar , 'lat' : self.gridGFS_HD.latitudes , 'lon' : self.gridGFS_HD.longitudes }
                        dataVars= {}
                        # Sacar informacion de unidades y nombres largos del archivo de configuracion.
                        vUnit = self.getConfigValueVL('units')
                        vLN = self.getConfigValueVL('longnames')
                        for vi in range(len(lVars)): 
                            dataVars[lVars[vi]] =  {'dimensions': ['time','lat','lon'] , 'attributes' : {'units' : vUnit[vi], 'long_name' : vLN[vi] , '_FillValue' : 9.999e+20 } , 'dataType' : 'f4' } 
                        
                        myfile = netcdfFile()
                        netcdfFilename = 'crudos' + s_dataset.upper() + '_' + gfs_hd_date.strftime('%Y-%m-%d') + '_' + ("%02d"%run_time) + 'z.nc'
                        myfile.createFile(netcdfFilename)
                        myfile.createDims(dimsA)
                        myfile.createVars(dimVars)
                        myfile.createVars(dataVars)
                        myfile.saveData(dimVarData)
                        myfile.saveData(varlist)
                        myfile.closeFile()      
                    else:
                        #TODO: Regresar las variables descargadas en formato python diccionario
                        pass              

                return netcdfFilename                                


                
                

  
