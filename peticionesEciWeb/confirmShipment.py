from util import *


def confirmShipment():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_SHIP'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_SHIP','ECI_SHIP',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_PEDIDOSWEB", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT id, idweb, datosventa FROM ew_ventaseciweb WHERE estado = 'TRACKING_INFO' AND trackinginformado = TRUE AND envioinformado = FALSE")
        rows = cx["cur"].fetchall()

        if len(rows) > 0:
            for p in rows:
                jsDatos = formatea_json(p["datosventa"])
                # urlShip = dameUrlConfirmShip(url, jsDatos)

                urlShip = url + "/api/orders/" + p["idweb"] + "/ship"
                result = put_request(urlShip, header)

                # if not result:
                #    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'ERROR_SHIP' WHERE id = " + str(p["id"]))
                #else:
                cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'SHIPPED', envioinformado = true WHERE id = " + str(p["id"]))

                cx["conn"].commit()

        else:
            print("No hay pedidos que confirmar envio en ECI WEB")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_SHIP'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_SHIP'")
    cx["conn"].commit()
    cierraConexion(cx)
    confirmShipment()

    return True


def dameUrlConfirmShip(url, jsDatos):

    orderId = jsDatos["order_id"]

    urlShip = False
    if orderId:
        urlShip = url + "/api/orders/" + orderId + "/ship"

    return urlShip
