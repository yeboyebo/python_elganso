from util import *

# NO UTILIZAR QUE PARA LAS RECOGIDAS DE MERCANCIA DE VALDEMORO AVISAN A LA EMPRESA AZKAR DESDE LA CENTRAL EN LUGAR DE TRABAJAR CON SEUR 

def trackingRefunds():
    return True
"""
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING_REFUNDS_VALDEMORO'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_TRACKING_REFUNDS_VALDEMORO','ECI_TRACKING_REFUNDS_VALDEMORO',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_TRACKINGREFUND", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}

        cx["cur"].execute("SELECT codasociacion AS codasociacion FROM ew_devolucioneseciweb WHERE informadatransportista = FALSE AND valdemoro = TRUE AND codasociacion IS NOT NULL AND codasociacion <> '' GROUP BY codasociacion ORDER BY codasociacion")
        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for d in rows:
                print("//d: ", d["codasociacion"])
                cx["cur"].execute("SELECT ew.id as id, ew.codasociacion as codigo, ew.cifnif as cifnif, ew.nombre as nombrecliente, ew.direccion as direccion, ew.dirnum as dirnum, ew.codpostal as codpostal, ew.ciudad as ciudad, ew.telefono as telefono, ew.email as email, ew.provincia as provincia, ew.codpais as codpais, CURRENT_DATE + 1 as fecha FROM ew_devolucioneseciweb ew WHERE (ew.informadatransportista = FALSE OR ew.informadatransportista IS NULL) AND ew.revisada = TRUE AND codasociacion = '" + str(d["codasociacion"]) + "' LIMIT 1")

                rows = cx["cur"].fetchall()
                if len(rows) > 0:
                    for p in rows:
                        dataJson = creaJsonTrackingRefund(p)
                        print(dataJson)
                        
                        #result = post_request(url, header, dataJson)
                        result = True
                        aResult = procesarRespuesta(result)
                        print(aResult)
                        if aResult == "ok":
                            cx["cur"].execute("UPDATE ew_devolucioneseciweb SET informadatransportista = TRUE WHERE codasociacion = '" + str(d["codasociacion"]) + "'")

                        cx["conn"].commit()

        else:
            print("No hay devoluciones que informar tracking number en Magento")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING_REFUNDS_VALDEMORO'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING_REFUNDS_VALDEMORO'")
    cx["conn"].commit()
    cierraConexion(cx)
    trackingRefunds()

    return True


def creaJsonTrackingRefund(p):
    
    dataJson = {
        "rma_id": p["codigo"],
        "sourceAddress": {
            "taxvat": p["cifnif"],
            "contact": p["nombrecliente"],
            "address": p["direccion"],
            "number": p["dirnum"],
            "postCode": p["codpostal"],
            "city": p["ciudad"],
            "email": p["email"],
            "region": p["provincia"],
            "country": p["codpais"]
        },
        "date": str(p["fecha"])
    }

    aDatos = []
    aDatos.append(dataJson)
    aDatos = json.dumps(aDatos)
    return aDatos

def procesarRespuesta(result):
    return "ok"
    oCaracteres = {}
    oCaracteres["\""] = ""
    oCaracteres["{"] = ""
    oCaracteres["}"] = ""

    for c in oCaracteres:
        result = result.replace(c, oCaracteres[c])

    aRespuesta = result.split(":")
    return aRespuesta[1]
"""