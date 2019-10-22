from util import *


def aceptarPedidosEci():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_ACEPTAR_WEB'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_ACEPTAR_WEB','ECI_ACEPTAR_WEB',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_PEDIDOSWEB", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT id, datosventa FROM ew_ventaseciweb WHERE estado = 'WAITING_ACCEPTANCE' AND aceptado = FALSE")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                jsDatos = json.loads(formatea_json(p["datosventa"]))
                urlPedido = dameUrlPedido(url, jsDatos)
                dataJson = creaJsonPedidoAceptado(jsDatos)

                result = put_request(urlPedido, header, dataJson)

                if not result and result != "":
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'ERROR_AL_ACEPTAR' WHERE id = " + str(p["id"]))
                else:
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'WAITING_DEBIT', aceptado = true WHERE id = " + str(p["id"]))

                cx["conn"].commit()

        else:
            print("No hay pedidos que aceptar en ECI WEB")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_ACEPTAR_WEB'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_ACEPTAR_WEB'")
    cx["conn"].commit()
    cierraConexion(cx)
    aceptarPedidosEci()

    return True


def creaJsonPedidoAceptado(jsDatos):

    aLineas = jsDatos["order_lines"]

    dataJson = False

    if len(aLineas) > 0:
        aDatos = []
        for i in range(len(aLineas)):
            aDatos.append({"accepted": True, "id": aLineas[i]["order_line_id"]})

        dataJson = {"order_lines": aDatos}
        dataJson = json.dumps(dataJson)

    return dataJson


def dameUrlPedido(url, jsDatos):

    orderId = jsDatos["order_id"]

    urlPedido = False
    if orderId:
        urlPedido = url + "/api/orders/" + orderId + "/accept"

    return urlPedido
