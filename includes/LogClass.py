#
# @author  Oscar Londoño
# @date    09/02/2019
# @descrip Simple clase PHP para el envio de mensajes a archivos log
# @version 2.0.0
#
class Log:
 #
 # Metodo Constructor
 # @param string archivo
 #
 def __init__(self, archivo):
  self.archivo = archivo
  self.archivolog = archivo + ".log"
  self.mensajes = ""

  # Abrimos el archivo log si no existe
  try:
   self.manejadorlog = open(self.archivolog, 'a')
  except OSError as err:
   print("Error: {0}".format(err))
  return

 #
 # Metodo Informacion (Escribir Mensaje de Informacion)
 # @param string mensaje
 # @return void
 #
 def Informacion(self, mensaje):
  self.AgregarLog(mensaje,'INFORMACION')

 #
 # Metodo Depuracion (Escribir Mensaje de Depuracion)
 # @param string mensaje
 # @return void
 #
 def Depuracion(self, mensaje):
  self.AgregarLog(mensaje,'DEPURACION')

 #
 # Metodo Advertencia (Escribir Mensaje de Advertencia)
 # @param string mensaje
 # @return void
 #
 def Advertencia(self, mensaje):
  self.AgregarLog(mensaje,'ADVERTENCIA')

 #
 # Metodo Error (Escribir Mensaje de Error)
 # @param string mensaje
 # @return void
 #
 def Error(self, mensaje):
  self.AgregarLog(mensaje,'ERROR')

 #
 # Metodo AgregarLog (Escribir el Mensaje al Archivo Log)
 # @param string mensaje
 # @param string gravedad
 # @return void
 #
 def AgregarLog(self, mensaje, gravedad):
  import time
  # Se prepara la linea del Log
  self.mensajes = self.mensajes + "[" + time.strftime("%d/%m/%Y %H:%M:%S") + "] [" + self.archivo + "]: [" + gravedad + "] - " + mensaje + ".\n"

 #
 # Metodo Destructor
 #
 def __del__(self):
  self.manejadorlog.write(self.mensajes)
  self.manejadorlog.close()
