import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlPedidosCli():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "IDL_PEDIDOS_CLI"

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        prepOrd = ET.Element("preparation_orders")
        int16 = ET.SubElement(prepOrd, "int16")

        cx["cur"].execute("SELECT p.codigo AS codigo, a.codalmacenidl AS codalmacen, p.nombrecliente AS nombrecliente, p.fecha AS fecha, p.fechasalida AS fechasalida, p.codcliente AS codcliente, p.direccion AS direccion, p.codpostal AS codpostal, p.ciudad AS ciudad, p.provincia AS provincia, pa.codiso3 AS codpais, p.idpedido AS idpedido, max(e.numenvio) AS numenvio, ag.nombre AS agenciatransidl, ag.descripcion AS commentidl FROM pedidoscli p INNER JOIN almacenesidl a ON p.codalmacen = a.codalmacen INNER JOIN clientes c ON c.codcliente = p.codcliente INNER JOIN agenciastrans_idl ag ON c.codagenciaidl = ag.codagenciaidl INNER JOIN idl_clientes i ON p.codcliente = i.codcliente AND ok = true LEFT OUTER JOIN paises pa ON p.codpais = pa.codpais LEFT OUTER JOIN eg_pedidosenviados e ON p.idpedido = e.idpedido  WHERE p.enviado = true AND p.fichero IS NULL AND p.ciudad IS NOT NULL AND p.direccion IS NOT NULL AND p.provincia IS NOT NULL AND p.codpostal IS NOT NULL AND p.codcliente IS NOT NULL AND p.fecha >= '2018-01-01' AND p.idpedido IN (SELECT idpedido FROM lineaspedidoscli WHERE idpedido = p.idpedido AND incluidoenfichero) AND pa.codiso3 IS NOT NULL AND CAST(p.idpedido AS VARCHAR) NOT IN (SELECT clave FROM idl_erroneos WHERE tipo = '" + tipo + "') GROUP BY p.codigo, a.codalmacenidl, p.nombrecliente, p.fecha, p.fechasalida, p.codcliente, p.direccion, p.codpostal, p.ciudad, p.provincia, pa.codiso3, p.idpedido, ag.nombre, ag.descripcion ORDER BY p.codigo LIMIT 1")

        rows = cx["cur"].fetchall()
        idPedido = False
        if len(rows) > 0:
            for p in rows:
                idPedido = p["idpedido"]

                faltaArticulos = False
                cx["cur"].execute("SELECT l.referencia as reflinea, ia.referencia as refidl FROM lineaspedidoscli l LEFT JOIN idl_articulos ia ON l.referencia = ia.referencia WHERE l.idpedido = " + str(idPedido) + " AND ((ia.referencia IS NULL) OR (ia.referencia IS NOT NULL AND ia.ok = false)) GROUP BY l.referencia, ia.referencia")
                rowsArt = cx["cur"].fetchall()
                if len(rowsArt) > 0:
                    faltaArticulos = True
                    for art in rowsArt:
                        print(art)
                        if not art["refidl"] or art["refidl"] == "":
                            cx["cur"].execute("INSERT INTO idl_articulos (referencia,ok,idlog,fecha,hora,error) values ('" + art["reflinea"] + "',false,NULL,CURRENT_DATE,CURRENT_TIME,'')")
                            cx["conn"].commit()
                        else:
                            cx["cur"].execute("UPDATE idl_articulos SET ok = false, idlog = NULL, fecha = CURRENT_DATE, hora = CURRENT_TIME, error = '' WHERE referencia = '" + str(art["reflinea"]) + "'")
                            cx["conn"].commit()
                        registraError(tipo, idPedido, art["reflinea"], cx)

                if faltaArticulos:
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()
                    return True

                if not creaXmlEnvioPedidoCli(p, int16, cx):
                    print("No hay pedidos con líneas que enviar")
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()

                    registraError(tipo, idPedido, "No hay pedidos con líneas que enviar", cx)
                    return False

            tree = ET.ElementTree(prepOrd)
            tree.write("./preparaciones/xmlPedidosCli.xml")

            xmlstring = tostring(prepOrd, 'utf-8', method="xml").decode("ISO8859-15")
            datosCX = dameDatosConexion("WSIDL_ENVPREP", cx)
            # datosCX = dameDatosConexion("WSIDL_ENVPREP_TEST", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            # result = "<recepOrders_response><int16><rub110><activity_code>GNS</activity_code><physical_depot_code>GNS</physical_depot_code><status>OK</status><error_descriptions><error_description></error_description></error_descriptions></rub110></int16></recepOrders_response>"
            print(result)

            status = False
            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido")
            else:
                res[0] = True
                res[1] = result

                root = ET.fromstring(result)
                child = root.find('int16/rub110')
                if child:
                    status = child.find("status").text

                tree = ET.ElementTree(root)
                tree.write("./preparaciones/resPedidosCli.xml")

                idlog = registraLog("ENV_SOLENVIOS", xmlstring, res, cx)
                if status:
                    if status == "OK":
                        cx["cur"].execute("UPDATE pedidoscli SET fichero = '" + str(idlog) + "' where idpedido = " + str(idPedido))
                        cx["conn"].commit()
                    else:
                        error = child.find("error_descriptions/error_description").text
                        print(error)
                        cx["cur"].execute("UPDATE pedidoscli SET fichero = 'ERROR: " + str(idlog) + "' where idpedido = " + str(idPedido))
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
        registraError(tipo, "Exception", e, cx)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlPedidosCli()

    return True


def creaXmlEnvioPedidoCli(p, int16, cx):
    cx["cur"].execute("SELECT barcode, cantidad-totalenalbaran AS cantidad, idlinea, descripcion, numlinea, cantidadenfichero FROM lineaspedidoscli WHERE cantidad-totalenalbaran > 0 AND incluidoenfichero AND idpedido = " + str(p["idpedido"]))
    rows = cx["cur"].fetchall()
    if len(rows) <= 0:
        return False

    sufijo = "01"
    if p["numenvio"] is not None:
        p["numenvio"] += 1
        if len(str(p["numenvio"])) == 1:
            sufijo = "0" + str(p["numenvio"])
        else:
            sufijo = str(p["numenvio"])

    codcliente = str(p["codcliente"])
    """codtienda = p["codtienda"]
    if codtienda:
        codcliente = codtienda + "_" + codcliente"""

    codcliente = codcliente[0:13]

    rub110 = ET.SubElement(int16, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "originator_reference").text = "T" + p["codigo"] + sufijo
    ET.SubElement(rub110, "preparation_type_code").text = "010"
    ET.SubElement(rub110, "end_consignee_code").text = codcliente
    ET.SubElement(rub110, "planned_final_delivery_date_century").text = str(p["fechasalida"])[0:2]
    ET.SubElement(rub110, "planned_final_delivery_date_year").text = str(p["fechasalida"])[2:4]
    ET.SubElement(rub110, "planned_final_delivery_date_month").text = str(p["fechasalida"])[5:7]
    ET.SubElement(rub110, "planned_final_delivery_date_day").text = str(p["fechasalida"])[8:10]

    rub111 = ET.SubElement(rub110, "rub111")
    ET.SubElement(rub111, "activity_code").text = "GNS"
    ET.SubElement(rub111, "physical_depot_code").text = "GNS"
    ET.SubElement(rub111, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub111, "originator_reference").text = "T" + p["codigo"] + sufijo
    ET.SubElement(rub111, "preparation_order_reason_code").text = "B2C"
    ET.SubElement(rub111, "load_grouping").text = str(p["agenciatransidl"])

    rub11A = ET.SubElement(rub110, "rub11A")
    ET.SubElement(rub11A, "activity_code").text = "GNS"
    ET.SubElement(rub11A, "physical_depot_code").text = "GNS"
    ET.SubElement(rub11A, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub11A, "originator_reference").text = "T" + p["codigo"] + sufijo
    ET.SubElement(rub11A, "address_type_code").text = "010"

    ET.SubElement(rub11A, "name_or_company_name_in_address").text = formateaCadena(str(p["nombrecliente"]))[0:35]

    direccion = formateaCadena(str(p["direccion"]))
    direccion1 = truncarDireccion(str(direccion).split(" "), 0, 35)
    direccion2 = truncarDireccion(str(direccion).split(" "), len(direccion1.split(" "))-1, 35)

    ET.SubElement(rub11A, "street_and_number_and_or_po_box").text = direccion1
    if len(direccion) > 35:
        ET.SubElement(rub11A, "additional_addres_data_1").text = direccion2

    ET.SubElement(rub11A, "additional_address_data_2").text = formateaCadena(str(p["provincia"]))[0:35]
    ET.SubElement(rub11A, "post_code_area_name").text = formateaCadena(str(p["ciudad"]))[0:35]
    ET.SubElement(rub11A, "postal_code").text = str(p["codpostal"])[0:9]
    ET.SubElement(rub11A, "iso_country_code").text = str(p["codpais"])

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "originator_reference").text = "T" + p["codigo"] + sufijo
    ET.SubElement(rub119, "comment_line_no").text = "1"
    ET.SubElement(rub119, "comment_group").text = "TRA"
    ET.SubElement(rub119, "comment").text = str(p["commentidl"])
    for l in rows:
        cantidadFichero = int(l["cantidadenfichero"])
        if int(l["cantidadenfichero"]) <= 0:
            cantidadFichero = str(int(l["cantidad"]))

        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "originator_reference").text = "T" + p["codigo"] + sufijo
        ET.SubElement(rub120, "originator_reference_line_no").text = str(int(l["numlinea"]))
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity_to_prepare").text = str(cantidadFichero)
        ET.SubElement(rub120, "owner_code_to_prepare").text = str(p["codalmacen"])
        ET.SubElement(rub120, "grade_code_to_prepare").text = "STD"

    return True
