import includes.LogClass as Log

import mysql.connector as MySql
import meli as Meli
from meli.rest import ApiException
from decimal import Decimal
import configparser
import time
import sys
import os

# Instanciamos el Objeto de manejo de archivo log
Log = Log.Log(os.path.splitext(os.path.basename(sys.argv[0]))[0])
Log.Informacion("Inicio de la Sincronizacion de MercadoLibre")

# Leemos el archivo de la configuración
Config = configparser.ConfigParser()
Config.read('ini\config.ini')

if (Config.get('GENERAL','SELLER_ID2')==""):
 print('Vacio')

# Conexion a la Base de Datos Web
try:
 ConexionWeb = MySql.connect(host=Config.get('SERVER_WEB','BD_HOST'),
                             user=Config.get('SERVER_WEB','BD_USER'),
                             password=Config.get('SERVER_WEB','BD_PASSWORD'),
                             database=Config.get('SERVER_WEB','BD_NAME'))
except MySql.Error as Error:
 Log.Error("Error de Conexion Web ({0})" + format(Error))
 sys.exit()

# Obtenemos el Token de MercadoLibre1
if (Config.get('GENERAL','SELLER_ID1')!=""):
 CursorWeb = ConexionWeb.cursor()
 try:
  CursorWeb.execute("SELECT MeliSesiones.MeliToken FROM MeliSesiones WHERE (MeliSesiones.MeliId='" + Config.get('GENERAL','SELLER_ID1') + "')")
  ResultadoWeb = CursorWeb.fetchone()
  MeliToken1 = ResultadoWeb[0]
 except MySql.Error as Error:
  Log.Error("Error de Consulta SQL Web ({0})" + format(Error))
  sys.exit()

 # Instancio la conexion con mercadolibre1
 Meli.Configuration(host = "https://api.mercadolibre.com.ve")
 ApiMeli1 = Meli.RestClientApi(Meli.ApiClient())

 # Obtengo las ordenes de mercadolibre pendientes (Comprometido1)
 try:
  Respuesta1 = ApiMeli1.resource_get("/orders/search?seller=" + Config.get('GENERAL','SELLER_ID1') + "&feedback.status=pending", MeliToken1)
  Log.Informacion("Procesada exitosamente (Lectura de Ordenes1)")
 except ApiException as Error:
  Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (Lectura de Ordenes1)")

# Obtenemos el Token de MercadoLibre2
if (Config.get('GENERAL','SELLER_ID2')!=""):
 CursorWeb = ConexionWeb.cursor()
 try:
  CursorWeb.execute("SELECT MeliSesiones.MeliToken FROM MeliSesiones WHERE (MeliSesiones.MeliId='" + Config.get('GENERAL','SELLER_ID2') + "') ORDER BY MeliSesiones.MeliNickname")
  ResultadoWeb = CursorWeb.fetchone()
  MeliToken2 = ResultadoWeb[0]
 except MySql.Error as Error:
  Log.Error("Error de Consulta SQL Web ({0})" + format(Error))
  sys.exit()

 # Instancio la conexion con mercadolibre2
 Meli.Configuration(host = "https://api.mercadolibre.com.ve")
 ApiMeli2 = Meli.RestClientApi(Meli.ApiClient())

 # Obtengo las ordenes de mercadolibre pendientes (Comprometido2)
 try:
  Respuesta2 = ApiMeli2.resource_get("/orders/search?seller=" + Config.get('GENERAL','SELLER_ID2') + "&feedback.status=pending", MeliToken2)
  Log.Informacion("Procesada exitosamente (Lectura de Ordenes2)")
 except ApiException as Error:
  Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (Lectura de Ordenes2)")

# Conexion a la Base de Datos Local
try:
 ConexionLocal = MySql.connect(host=Config.get('SERVER_LOCAL','BD_HOST'),
                               user=Config.get('SERVER_LOCAL','BD_USER'),
                               password=Config.get('SERVER_LOCAL','BD_PASSWORD'),
                               database=Config.get('SERVER_LOCAL','BD_NAME'))
except MySql.Error as Error:
 Log.Error("Error de Conexion Local ({0})" + format(Error))
 sys.exit()

# Consulta a la Base de Datos Local
CursorLocal = ConexionLocal.cursor()
try:
 CursorLocal.execute("SELECT DPINV.INV_CODIGO,DPINV.INV_DESCRI,DPINV.INV_NUMMER,DPINV.INV_NUMME2,"\
                            "(SELECT PRE_PRECIO FROM DPPRECIOS WHERE DPPRECIOS.PRE_CODIGO=DPINV.INV_CODIGO AND DPPRECIOS.PRE_CODMON='$' AND DPPRECIOS.PRE_LISTA='A' AND DPPRECIOS.PRE_UNDMED='Unidad') AS INV_PREDOL,"\
                            "SUM(IFNULL(DPMOVINV.MOV_CANTID,0)*IFNULL(DPMOVINV.MOV_FISICO,0)*IFNULL(DPMOVINV.MOV_CXUND,0)) AS INV_EXISTE "\
                     "FROM DPINV "\
                     "LEFT JOIN DPMOVINV ON DPINV.INV_CODIGO=DPMOVINV.MOV_CODIGO "\
                     "WHERE ((DPINV.INV_GRUPO<>'0000000000') AND (DPINV.INV_ESTADO='A') AND ((DPMOVINV.MOV_CODALM='001') OR (DPMOVINV.MOV_CODALM IS NULL)) AND ((DPMOVINV.MOV_INVACT=1) OR (DPMOVINV.MOV_INVACT IS NULL))) "\
                     "GROUP BY DPINV.INV_CODIGO,DPINV.INV_DESCRI,DPINV.INV_NUMMER,DPINV.INV_NUMME2")
 ResultadoLocal = CursorLocal.fetchall()
except MySql.Error as Error:
 Log.Error("Error de Consulta SQL Local ({0})" + format(Error))
 sys.exit()

for Productos in ResultadoLocal:
 # Verificamos y actualizamos el Precio y la existencia del mercadolibre1
 if ((Config.get('GENERAL','SELLER_ID1')!="") and (Productos[2]!=None) and (Productos[2]!="")):
  # Publicaciones relacionadas
  Publicaciones = Productos[2].split(sep=',')

  # Calculamos el Precio
  Precio = round(Productos[4],2)
  Precio = Precio if Precio>2 else 2

  # Calculamos la existencia restando lo comprometido
  Existencia = Productos[5]
  for Publicacion in Publicaciones:
   for Ordenes in Respuesta1["results"]:
    for Items in Ordenes["order_items"]:
     if ((Items["item"]["id"])==(Config.get('GENERAL','SITE_ID') + Publicacion)):
      Existencia = (Existencia - Items["quantity"])
  Existencia = int(Existencia) if Existencia>0 else 0

  # Recorremos las publicaciones y actualizamos si es necesario
  for Publicacion in Publicaciones:
   try:
    Respuesta3 = ApiMeli1.resource_get("/items/MLV" + Publicacion, MeliToken1)
   except ApiException as Error:
    Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (Lectura de Publicacion)")

   if (len(Respuesta3["variations"])>0):
    for Variacion in Respuesta3["variations"]:
     if ((Precio!=Decimal(str(Variacion["price"]))) or (Existencia!=Variacion["available_quantity"])):
      try:
       Respuesta4 = ApiMeli1.resource_put("/items/MLV" + Publicacion, MeliToken1, {'variations' : [{'id' : Variacion["id"], 'price' : str(Precio), 'available_quantity' : str(Existencia)}]})
       Log.Informacion("Procesada exitosamente la actualizacion de Precio y existencia del articulo (" + Publicacion + "-" + Productos[0] + ")")
       time.sleep(1)
      except ApiException as Error:
       Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (" + Publicacion + "-" + Productos[0] + ")")
   else:
    if ((Precio!=Decimal(str(Respuesta3["price"]))) or (Existencia!=Respuesta3["available_quantity"])):
     try:
      Respuesta4 = ApiMeli1.resource_put("/items/MLV" + Publicacion, MeliToken1, {'price' : str(Precio), 'available_quantity' : str(Existencia)})
      Log.Informacion("Procesada exitosamente la actualizacion de Precio y existencia del articulo (" + Publicacion + "-" + Productos[0] + ")")
      time.sleep(1)
     except ApiException as Error:
      Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (" + Publicacion + "-" + Productos[0] + ")")

 # Verificamos y actualizamos el Precio y la existencia del mercadolibre2
 if ((Config.get('GENERAL','SELLER_ID2')!="") and (Productos[3]!=None) and (Productos[3]!="")):
  # Publicaciones relacionadas
  Publicaciones = Productos[3].split(sep=',')

  # Calculamos el Precio
  Precio = round(Productos[4],2)
  Precio = Precio if Precio>2 else 2

  # Calculamos la existencia restando lo comprometido
  Existencia = Productos[5]
  for Publicacion in Publicaciones:
   for Ordenes in Respuesta2["results"]:
    for Items in Ordenes["order_items"]:
     if ((Items["item"]["id"])==(Config.get('GENERAL','SITE_ID') + Publicacion)):
      Existencia = (Existencia - Items["quantity"])
  Existencia = int(Existencia) if Existencia>0 else 0

  # Recorremos las publicaciones y actualizamos si es necesario
  for Publicacion in Publicaciones:
   try:
    Respuesta5 = ApiMeli1.resource_get("/items/MLV" + Publicacion, MeliToken2)
   except ApiException as Error:
    Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (Lectura de Publicacion)")

   if (len(Respuesta5["variations"])>0):
    for Variacion in Respuesta5["variations"]:
     if ((Precio!=Decimal(str(Variacion["price"]))) or (Existencia!=Variacion["available_quantity"])):
      try:
       Respuesta6 = ApiMeli1.resource_put("/items/MLV" + Publicacion, MeliToken2, {'variations' : [{'id' : Variacion["id"], 'price' : str(Precio), 'available_quantity' : str(Existencia)}]})
       Log.Informacion("Procesada exitosamente la actualizacion de Precio y existencia del articulo (" + Publicacion + "-" + Productos[0] + ")")
       time.sleep(1)
      except ApiException as Error:
       Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (" + Publicacion + "-" + Productos[0] + ")")
   else:
    if ((Precio!=Decimal(str(Respuesta5["price"]))) or (Existencia!=Respuesta5["available_quantity"])):
     try:
      Respuesta6 = ApiMeli1.resource_put("/items/MLV" + Publicacion, MeliToken2, {'price' : str(Precio), 'available_quantity' : str(Existencia)})
      Log.Informacion("Procesada exitosamente la actualizacion de Precio y existencia del articulo (" + Publicacion + "-" + Productos[0] + ")")
      time.sleep(1)
     except ApiException as Error:
      Log.Error("Error de Procesamiento ({0})" + format(Error) + " - (" + Publicacion + "-" + Productos[0] + ")")

CursorLocal.close()
ConexionLocal.close()
CursorWeb.close()
ConexionWeb.close()
Log.Informacion("Fin de la Sincronizacion de Mercadolibre")