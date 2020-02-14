from util import *
import json


def dameUrlsImagenes():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'MGT_URLS_IMAGENES'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'MGT_URLS_IMAGENES','MGT_URLS_IMAGENES',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSMGT_URLSIMAGENES", cx)   
        url = datosCX["url"]
        header = datosCX["header"]
        offset = dameDatosConexion("WSMGT_URLSIMG_OFFSET", cx)
        url = url + str(offset)
        
        data = ""
        result = get_request(url, header, data, {})
        if not result:
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MGT_URLS_IMAGENES'")
            cx["conn"].commit()
            return False
        else:
            guardarResultado(result, cx)
            actualizarOffset(int(offset), result, cx)
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MGT_URLS_IMAGENES'")
            cx["conn"].commit()
            # return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MGT_URLS_IMAGENES'")
    cx["conn"].commit()

    

    offset = dameDatosConexion("WSMGT_URLSIMG_OFFSET", cx)
    cierraConexion(cx)
    if offset != 1:
        dameUrlsImagenes();

    return True


def guardarResultado(result, cx):
    cx["cur"].execute("INSERT INTO eg_jsonurlsimagenesarticulos (fecha,hora,json) VALUES (CURRENT_DATE,CURRENT_TIME,'" + result + "')")
    cx["conn"].commit()

    return True


def actualizarOffset(offset, result, cx):
    jSResult = json.loads(result)
    totalPages = jSResult["pages"]
    
    offset = offset + 1
    if offset > totalPages:
        offset = 1

    cx["cur"].execute("UPDATE param_parametros SET valor = " + str(offset) + " WHERE nombre = 'WSMGT_URLSIMG_OFFSET'")
    cx["conn"].commit()

    return True
