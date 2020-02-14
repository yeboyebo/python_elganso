from util import *
import json


def dameArticulos():

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'EW_ARTICULOS_PUBLICADOS'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'EW_ARTICULOS_PUBLICADOS','EW_ARTICULOS_PUBLICADOS',CURRENT_DATE)")
    cx["conn"].commit()

    try:

        datosCX = dameDatosConexion("WSEW_ARTPUBLICADOS", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        maxi = datosCX["max"]
        offset = dameDatosConexion("WSEW_ARTPUB_OFFSET", cx)
        
        params = {"max":maxi,"offset":offset}
        header = {
            'Authorization': auth
            }
        
        data = ""
        result = get_request(url, header, data, params)
        if not result:
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'EW_ARTICULOS_PUBLICADOS'")
            cx["conn"].commit()
            return False
        else:
            guardarResultado(result, cx)
            actualizarOffset(int(offset), result, cx)
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'EW_ARTICULOS_PUBLICADOS'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'EW_ARTICULOS_PUBLICADOS'")
    cx["conn"].commit()
    cierraConexion(cx)

    return True


def guardarResultado(result, cx):
    cx["cur"].execute("INSERT INTO ew_jsonarticulosactivos (codalmacen,fecha,hora,json) VALUES ('AWEB',CURRENT_DATE,CURRENT_TIME,'" + result + "')")
    cx["conn"].commit()

    return True


def actualizarOffset(offset, result, cx):
    jSResult = json.loads(result)
    totalCount = jSResult["total_count"]
    
    offset = offset + 100
    if offset >= totalCount:
        offset = 0

    cx["cur"].execute("UPDATE param_parametros SET valor = " + str(offset) + " WHERE nombre = 'WSEW_ARTPUB_OFFSET'")
    cx["conn"].commit()

    return True
