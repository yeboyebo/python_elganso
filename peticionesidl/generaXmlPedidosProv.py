import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlPedidos():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "IDL_PEDIDOS_PROV"
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        recOrd = ET.Element("reception_orders")
        int15 = ET.SubElement(recOrd, "int15")

        cx["cur"].execute("SELECT pp.codigo AS codigo, pp.idpedido AS idpedido, pp.fechaentrada AS fechaentrada, pp.fecha AS fecha, pp.enviomodificado AS enviomodificado, ai.codalmacenidl AS codalmacen, ai.nombre AS nombrealmacen, pp.codproveedor AS codproveedor, MAX(pr.numenvio) AS numenvio FROM pedidosprov pp INNER JOIN almacenesidl ai ON pp.codalmacen = ai.codalmacen INNER JOIN idl_proveedores ip ON (pp.codproveedor = ip.codproveedor AND ip.ok = true) LEFT OUTER JOIN eg_pedidosrecibidos pr ON pp.idpedido = pr.idpedido WHERE (pp.enviado = true AND pp.fichero is NULL AND pp.codproveedor IS NOT NULL AND pp.idpedido IN (SELECT lpp.idpedido FROM lineaspedidosprov lpp WHERE lpp.idpedido = pp.idpedido)) AND CAST(pp.idpedido AS VARCHAR) NOT IN (SELECT clave FROM idl_erroneos WHERE tipo = '" + tipo + "') GROUP BY pr.idpedido, pp.codigo, pp.idpedido, pp.fechaentrada, pp.fecha, pp.enviomodificado, ai.codalmacenidl, ai.nombre, pp.codproveedor ORDER BY pp.fecha ASC LIMIT 1")
        # and p.codigo in ('20130A000769','20170A001096')

        rows = cx["cur"].fetchall()
        idPedido = False
        if len(rows) > 0:
            for p in rows:
                idPedido = p["idpedido"]

                faltaArticulos = False
                cx["cur"].execute("SELECT lpp2.referencia as reflinea, ia.referencia as refidl FROM lineaspedidosprov lpp2 LEFT JOIN idl_articulos ia ON lpp2.referencia = ia.referencia WHERE lpp2.idpedido = " + str(idPedido) + " AND ((ia.referencia IS NULL) OR (ia.referencia IS NOT NULL AND ia.ok = false)) GROUP BY lpp2.referencia, ia.referencia")
                rowsArt = cx["cur"].fetchall()
                if len(rowsArt) > 0:
                    faltaArticulos = True
                    for art in rowsArt:
                        print(art)
                        if not art["refidl"] or art["refidl"] == "":
                            cx["cur"].execute("INSERT INTO idl_articulos (referencia,ok,idlog,fecha,hora,error) values ('" + art["reflinea"] + "',false,NULL,CURRENT_DATE,CURRENT_TIME,'')")
                        else:
                            cx["cur"].execute("UPDATE idl_articulos SET ok = false, idlog = NULL, fecha = CURRENT_DATE, hora = CURRENT_TIME, error = '' WHERE referencia = '" + str(art["reflinea"]) + "'")
                        cx["conn"].commit()
                        registraError(tipo, idPedido, art["reflinea"], cx)

                if faltaArticulos:
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()
                    return True

                if not creaXmlRecepcionPedido(p, int15, cx):
                    print("No hay pedidos con líneas que enviar")
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()

                    registraError(tipo, idPedido, "No hay pedidos con líneas que enviar", cx)
                    return False

            tree = ET.ElementTree(recOrd)
            tree.write("./recepciones/xmlPedidos_" + p["codigo"] + ".xml")

            xmlstring = tostring(recOrd, 'utf-8', method="xml").decode("ISO8859-15")
            # datosCX = dameDatosConexion("WSIDL_ENVREC_TEST", cx)
            datosCX = dameDatosConexion("WSIDL_ENVREC", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            # result = "<recepOrders_response><int15><rub110><activity_code>GNS</activity_code><physical_depot_code>GNS</physical_depot_code><status>OK</status><error_descriptions><error_description></error_description></error_descriptions></rub110></int15></recepOrders_response>"
            status = False
            print(result)

            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido")
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                cx["conn"].commit()
                return False
            else:
                res[0] = True
                res[1] = result

                root = ET.fromstring(result)
                child = root.find('int15/rub110')
                if child:
                    status = child.find("status").text

                tree = ET.ElementTree(root)
                tree.write("./recepciones/resPedidos_" + p["codigo"] + ".xml")

            idlog = registraLog("ENV_RECEPCIONES", xmlstring, res, cx)
            if status:
                if status == "OK":
                    cx["cur"].execute("UPDATE pedidosprov SET fichero = '" + str(idlog) + "' where idpedido = " + str(idPedido))
                    cx["conn"].commit()
                else:
                    error = child.find("error_descriptions/error_description").text
                    print(error)
                    cx["cur"].execute("UPDATE pedidosprov SET fichero = 'ERROR: " + str(idlog) + "' where idpedido = " + str(idPedido))
                    cx["conn"].commit()
                    registraError(tipo, idPedido, "Error al envair el pedido", cx)
        else:
            print("No hay pedidos que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
        cx["conn"].commit()
        registraError(tipo, "Exception", e, cx)
        return False

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlPedidos()

    return True


def creaXmlRecepcionPedido(p, int15, cx):
    cx["cur"].execute("SELECT barcode, cantidad-totalenalbaran as cantidad, idlinea, descripcion FROM lineaspedidosprov WHERE cantidad-totalenalbaran > 0 and idpedido = " + str(p["idpedido"]))
    rows = cx["cur"].fetchall()
    numL = 1
    if len(rows) <= 0:
        return False

    sufijo = "01"
    if p["numenvio"] is not None:
        p["numenvio"] += 1
        if len(str(p["numenvio"])) == 1:
            sufijo = "0" + str(p["numenvio"])
        else:
            sufijo = str(p["numenvio"])

    rub110 = ET.SubElement(int15, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "receipt_reference").text = "P" + p["codigo"] + sufijo
    ET.SubElement(rub110, "receipt_type").text = "010"
    ET.SubElement(rub110, "receipt_reason_code").text = "PRO"
    ET.SubElement(rub110, "work_mode_code").text = "REC"
    ET.SubElement(rub110, "original_code").text = p["codproveedor"]

    ET.SubElement(rub110, "carrier_arrival_date_century").text = str(p["fechaentrada"])[0:2]
    ET.SubElement(rub110, "carrier_arrival_date_year").text = str(p["fechaentrada"])[2:4]
    ET.SubElement(rub110, "carrier_arrival_date_month").text = str(p["fechaentrada"])[5:7]
    ET.SubElement(rub110, "carrier_arrival_date_day").text = str(p["fechaentrada"])[8:10]
    ET.SubElement(rub110, "carrier_arrival_time").text = "0"

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "receipt_reference").text = "P" + p["codigo"] + sufijo
    ET.SubElement(rub119, "comment_line_no").text = "001"
    ET.SubElement(rub119, "comment_group").text = "OWN"

    if p["nombrealmacen"]:
        ET.SubElement(rub119, "comment").text = formateaCadena(p["nombrealmacen"])[0:70]
    else:
        ET.SubElement(rub119, "comment").text = ""

    for l in rows:
        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "receipt_reference").text = "P" + p["codigo"] + sufijo
        ET.SubElement(rub120, "receipt_reference_line_no").text = str(numL)
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity").text = str(int(l["cantidad"]))
        ET.SubElement(rub120, "owner_code").text = p["codalmacen"]
        numL += 1

    return True
