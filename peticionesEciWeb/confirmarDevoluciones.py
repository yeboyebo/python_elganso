from util import *
import xml.etree.ElementTree as ET


def confirmarDevoluciones():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'ECI_DEVOLUCION'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'ECI_DEVOLUCION','ECI_DEVOLUCION',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("WSECI_PEDIDOSWEB", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT de.codcomanda as codcomanda, de.idtpv_comanda AS idcomanda, ew.datosdevol as datosdevol, ew.id AS idew FROM idl_ecommercedevoluciones de INNER JOIN ew_devolucioneseciweb ew ON de.idtpv_comanda = ew.idtpv_comanda WHERE de.confirmacionrecepcion = 'Si' AND (de.informadomagento IS NULL OR de.informadomagento = false) AND de.codcomanda like 'EDV%' AND de.eseciweb = true AND ew.estado = 'RECIBIDA' AND ew.recibida = true ORDER BY de.codcomanda")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                xmlDevol = p["datosdevol"]
                urlPedido = dameUrlPedido(url)
                dataJson = creaJsonDevolucion(xmlDevol, p["idcomanda"], cx)
                result = put_request(urlPedido, header, dataJson)

                if not result:
                    cx["cur"].execute("UPDATE ew_devolucioneseciweb SET estado = 'ERROR_CONFIRMACION' WHERE id = " + str(p["idew"]))
                else:
                    cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET informadomagento = true, fechamagento = CURRENT_DATE, horamagento = CURRENT_TIME WHERE idtpv_comanda = " + str(p["idcomanda"]))

                cx["conn"].commit()

        else:
            print("No hay devoluciones que aceptar en ECI WEB")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_DEVOLUCION'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'ECI_DEVOLUCION'")
    cx["conn"].commit()
    cierraConexion(cx)
    confirmarDevoluciones()

    return True


def creaJsonDevolucion(xmlDevol, idComanda, cx):
    xmlDevol = ET.fromstring(xmlDevol)

    orderLine = ""
    barCode = ""
    importe = 0
    cantidad = 1
    aLineas = []
    for devolucion in xmlDevol.findall('Devolucion'):
        orderLine = devolucion.find('lineaPedido').text
        barCode = str(int(devolucion.find('EAN').text))[-13:]
        cantidad = int(devolucion.find('unidades').text)

        cx["cur"].execute("SELECT pvpunitarioiva FROM tpv_lineascomanda WHERE idtpv_comanda = " + str(idComanda) + " AND barcode = '" + str(barCode) + "' AND cantidad <= " + str(cantidad * (-1)) + " AND cantidad < 0 LIMIT 1")
        rows = cx["cur"].fetchall()
        if len(rows) <= 0:
            return False

        for lc in rows:
            importe = int(float(lc["pvpunitarioiva"]) * cantidad)

        aLineas.append({"amount": importe, "order_line_id": orderLine, "quantity": cantidad, "reason_code": "17", "shipping_amount": 0})

    dataJson = {"refunds": aLineas}
    dataJson = json.dumps(dataJson)

    return dataJson


def dameUrlPedido(url):

    urlPedido = url + "/api/orders/refund"

    return urlPedido
