from util import *


def trackingInfo():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_TRACKING','ECI_TRACKING',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_PEDIDOSWEB", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT e.idtpv_comanda AS idtpv_comanda, e.numseguimiento AS trackingnumber, v.id AS id, v.idweb as idweb, v.datosenvio AS datosenvio, e.transportista AS transportista, e.metodoenvioidl AS metodoenvio FROM ew_ventaseciweb v INNER JOIN idl_ecommerce e ON v.idtpv_comanda = e.idtpv_comanda WHERE v.estado = 'SHIPPING' AND v.aceptado = TRUE AND v.infoclienterecibida = TRUE AND v.trackinginformado = FALSE AND e.eseciweb = TRUE AND e.numseguimiento IS NOT NULL AND e.numseguimiento <> '' AND e.numseguimientoinformado = FALSE")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                # jsDatos = json.loads(formatea_json(p["datosenvio"]))
                urlTracking = url + "/api/orders/" + p["idweb"] + "/tracking"
                # urlTracking = dameUrlTracking(url, jsDatos)
                dataJson = creaJsonTrackingInfo(p)
                result = put_request(urlTracking, header, dataJson)

                if not result and result != "":
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'ERROR_TRACKING' WHERE id = " + str(p["id"]))
                else:
                    cx["cur"].execute("UPDATE ew_ventaseciweb SET estado = 'TRACKING_INFO', trackinginformado = true WHERE id = " + str(p["id"]))
                    cx["conn"].commit()
                    cx["cur"].execute("UPDATE idl_ecommerce SET fechamagento = CURRENT_DATE, horamagento = CURRENT_TIME, numseguimientoinformado = true WHERE idtpv_comanda = " + str(p["idtpv_comanda"]))

                cx["conn"].commit()
        else:
            print("No hay pedidos que informar tracking number en ECI WEB")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)
    
    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_TRACKING'")
    cx["conn"].commit()
    
    cierraConexion(cx)
    trackingInfo()
    return True

def creaJsonTrackingInfo(p):
    transportista = str(p["transportista"])
    trackingNumber = str(p["trackingnumber"])

    dataJson = {"carrier_code": transportista, "carrier_name": transportista, "tracking_number": trackingNumber + " Consulte los detalles del envío en www.seur.com"}
    dataJson = json.dumps(dataJson)

    return dataJson

def dameUrlTracking(url, jsDatos):

    orderId = jsDatos["order_id"]

    urlPedido = False
    if orderId:
        urlPedido = url + "/api/orders/" + orderId + "/tracking"

    return urlPedido
