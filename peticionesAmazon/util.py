import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json
from datetime import date
from datetime import datetime


def creaConexion():
    cx = {}
    cx["conn"] = conectaBd()
    cx["cur"] = cx["conn"].cursor(cursor_factory=RealDictCursor)

    return cx


def conectaBd():
    try:
        # connect_str = "user='elganso' password='elganso' dbname='elganso'"
        # connect_str += " host='172.26.13.14' port='5432'"
        connect_str = "user='lorena' password='555zapato' dbname='elganso_ctr'"
        connect_str += " host='localhost' port='5432'"

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
    res[1] = formateaCadenaLog(res[1])

    cx["cur"].execute("INSERT INTO idl_log (fecha,hora,estado,tipo,envio,respuesta) values (CURRENT_DATE,CURRENT_TIME," + str(res[0]) + ",'" + str(tipo) + "',E'" + str(envio) + "',E'" + str(res[1]) + "')")
    cx["conn"].commit()

    cx["cur"].execute("SELECT id FROM idl_log WHERE tipo = '" + str(tipo) + "' ORDER BY fecha desc, hora desc limit 1")
    row = cx["cur"].fetchall()
    idlog = row[0]["id"]

    return idlog


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

    for c in oCaracteres:
        cadena = cadena.replace(c, oCaracteres[c])

    return cadena


def formateaCadenaEcommerce(cadena):
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


def get_request(url, header, data, params):
    try:
        response = requests.request("GET", url, data=data, headers=header, params=params)
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


def put_request(url, header, data={}):
    try:
        response = requests.request("PUT", url, data=data, headers=header)
        print(response.text)
        response.raise_for_status()
    except Exception as e:
        print("Error de comunicacion")
        print(e)
        return False

    return response.text.encode("utf-8").decode("ISO8859-15")


def formatea_json(jsDatos):
    jsDatos = jsDatos.replace("'", "\"")
    jsDatos = jsDatos.replace("None", "null")
    jsDatos = jsDatos.replace("False", "false")
    jsDatos = jsDatos.replace("True", "true")

    return jsDatos