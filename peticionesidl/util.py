import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json


def creaConexion():
    cx = {}
    cx["conn"] = conectaBd()
    cx["cur"] = cx["conn"].cursor(cursor_factory=RealDictCursor)

    return cx


def conectaBd():
    try:
        connect_str = "user='elganso' password='elganso' dbname='elganso'"
        connect_str += " host='172.26.13.14' port='5432'"

        return psycopg2.connect(connect_str)

    except Exception as e:
        print("No pudo conectar con BBDD")
        print(e)
        return False


def cierraConexion(cx):
    if cx["cur"]:
        cx["cur"].close()
    if cx["conn"]:
        cx["conn"].close()


def dameDatosConexion(conexion, cx):
    cx["cur"].execute("select valor from param_parametros where nombre = '" + conexion + "'")
    row = cx["cur"].fetchall()
    valor = row[0]["valor"]
    datosCX = json.loads(valor)

    return datosCX


def registraLog(tipo, envio, res, cx):
    envio = formateaCadenaLog(envio)
    res[1] = formateaCadenaLog(str(res[1]))

    cx["cur"].execute("INSERT INTO idl_log (fecha,hora,estado,tipo,envio,respuesta) values (CURRENT_DATE,CURRENT_TIME," + str(res[0]) + ",'" + str(tipo) + "',E'" + str(envio) + "',E'" + str(res[1]) + "')")
    cx["conn"].commit()

    cx["cur"].execute("SELECT id FROM idl_log WHERE tipo = '" + str(tipo) + "' ORDER BY fecha desc, hora desc limit 1")
    row = cx["cur"].fetchall()
    idlog = row[0]["id"]

    return idlog


def registraError(tipo, clave, motivo, cx):
    motivo = formateaCadenaLog(motivo)
    
    cx["cur"].execute("INSERT INTO idl_erroneos (fecha,hora,tipo,clave,motivo) values (CURRENT_DATE,CURRENT_TIME,'" + str(tipo) + "','" + str(clave) + "','" + str(motivo) + "')")
    cx["conn"].commit()

    return True


def formateaCadenaLog(cadena):
    return cadena.replace("'", "\\'")


def quitaIntros(cadena):
    cadena = cadena.replace("\r\n", " ")
    cadena = cadena.replace("\r", " ")
    cadena = cadena.replace("\n", " ")
    cadena = cadena.replace("\t", " ")
    return cadena


def formateaCadena(cadena):
    oCaracteres = {}
    oCaracteres["Á"] = "A"
    oCaracteres["É"] = "E"
    oCaracteres["Í"] = "I"
    oCaracteres["Ó"] = "O"
    oCaracteres["Ú"] = "U"
    oCaracteres["á"] = "a"
    oCaracteres["é"] = "e"
    oCaracteres["í"] = "i"
    oCaracteres["ó"] = "o"
    oCaracteres["ú"] = "u"
    oCaracteres["ñ"] = "n"
    oCaracteres["Ñ"] = "N"
    oCaracteres["À"] = "A"
    oCaracteres["È"] = "E"
    oCaracteres["Ì"] = "I"
    oCaracteres["Ò"] = "O"
    oCaracteres["Ù"] = "U"
    oCaracteres["à"] = "a"
    oCaracteres["è"] = "e"
    oCaracteres["ì"] = "i"
    oCaracteres["ò"] = "o"
    oCaracteres["ù"] = "u"
    oCaracteres["Â"] = "A"
    oCaracteres["Ê"] = "E"
    oCaracteres["Î"] = "I"
    oCaracteres["Ô"] = "O"
    oCaracteres["Û"] = "U"
    oCaracteres["â"] = "a"
    oCaracteres["ê"] = "e"
    oCaracteres["î"] = "i"
    oCaracteres["ô"] = "o"
    oCaracteres["û"] = "u"
    oCaracteres["Ä"] = "A"
    oCaracteres["Ë"] = "E"
    oCaracteres["Ï"] = "I"
    oCaracteres["Ö"] = "O"
    oCaracteres["Ü"] = "U"
    oCaracteres["ä"] = "a"
    oCaracteres["ë"] = "e"
    oCaracteres["ï"] = "i"
    oCaracteres["ö"] = "o"
    oCaracteres["ü"] = "u"
    oCaracteres["ü"] = "u"
    oCaracteres["ç"] = "c"
    oCaracteres["Ç"] = "C"
    oCaracteres[";"] = ","
    oCaracteres["'"] = " "
    oCaracteres["\""] = ""
    oCaracteres["\r\n"] = " "
    oCaracteres["\r"] = " "
    oCaracteres["\n"] = " "
    oCaracteres["\t"] = " "

    for c in oCaracteres:
        cadena = cadena.replace(c, oCaracteres[c])

    return cadena


def formateaCadenaEcommerce(cadena):
    oCaracteres = {}
    oCaracteres["\""] = ""
    oCaracteres["\r\n"] = " "
    oCaracteres["\r"] = " "
    oCaracteres["\n"] = " "
    oCaracteres["\t"] = " "
    oCaracteres["'"] = ""
    oCaracteres[";"] = ","

    for c in oCaracteres:
        cadena = cadena.replace(c, oCaracteres[c])

    # validos = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚáéíóúñÑÀÈÌÒÙàèìòùÂÊÎÔÛâêîôûÄËÏÖÜäëïöüüçÇ /-?+:,.()"

    # cOut = ""
    # i = 0

    # while i < len(cadena):
        # if validos.find(cadena[i]) >= 0:
            # cOut += cadena[i]
        # i = i + 1

    # return cOut
    return cadena


def post_request(url, header, data):
    try:
        response = requests.post(url, data=data, headers=header)
        response.raise_for_status()
    except Exception as e:
        print("Error de comunicacion")
        print(e)
        return False

    return response.text.encode("utf-8").decode("ISO8859-15")


def truncarDireccion(direccion, pos, longitud):
    linea = ""
    lineaTemp = ""
    continuar = True
    for pos in range(pos, len(direccion)):
        lineaTemp = linea + direccion[pos]
        if((len(lineaTemp) < longitud) and continuar):
            linea = lineaTemp + " "
        else:
            continuar = False

    return linea


def cerosIzquierda(numero, totalCifras):

    numero = str(numero)

    while totalCifras > len(numero):
        numero = "0" + numero

    return numero
