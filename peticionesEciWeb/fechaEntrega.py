from util import *


def fechaEntrega():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_FECHA_WEB'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_FECHA_WEB','ECI_FECHA_WEB',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_PEDIDOSWEB", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT v.idweb as idweb, v.id AS id, v.datosventa AS datosventa FROM ew_ventaseciweb v INNER JOIN idl_ecommerce e ON v.idtpv_comanda = e.idtpv_comanda WHERE v.estado = 'SHIPPED' AND v.envioinformado = TRUE AND v.fechaentregainformada = FALSE AND e.fechamagento < CURRENT_DATE-2")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                jsDatos = json.loads(formatea_json(p["datosventa"]))
                # urlFechaEntrega = dameUrlPedido(url, jsDatos)
                urlFechaEntrega = url + "/api/orders/" + str(p["idweb"]) + "/additional_fields"
                dataJson = creaJsonFechaEntrega()

                result = put_request(urlFechaEntrega, header, dataJson)

                if not result:
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'ERROR_FECHAWEB' WHERE id = " + str(p["id"]))
                else:
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'DELIVERED_DATE', fechaentregainformada = true WHERE id = " + str(p["id"]))

                cx["conn"].commit()

        else:
            print("No hay pedidos que informar fecha entrega en ECI WEB")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_FECHA_WEB'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_FECHA_WEB'")
    cx["conn"].commit()
    cierraConexion(cx)
    fechaEntrega()

    return True


def creaJsonFechaEntrega():

    dataJson = {"order_additional_fields": [{"code": "fecha-entrega", "value": "{:%Y-%m-%dT%H:%M:%SZ%Z}".format(datetime.utcnow())}]}
    dataJson = json.dumps(dataJson)

    return dataJson


def dameUrlPedido(url, jsDatos):

    orderId = jsDatos["order_id"]

    urlPedido = False
    if orderId:
        urlPedido = url + "/api/orders/" + orderId + "/additional_fields"

    return urlPedido
