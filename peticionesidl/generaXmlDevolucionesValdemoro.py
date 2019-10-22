import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import time


def generaXmlDevolucionesValdemoro():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_DEV_VALDEMORO','IDL_DEV_VALDEMORO',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        recOrd = ET.Element("reception_orders")
        int15 = ET.SubElement(recOrd, "int15")

        cx["cur"].execute("SELECT codcomanda FROM idl_ecommercedevoluciones WHERE envioidl = FALSE AND codcomanda LIKE 'VAL%' AND (idlogrecepcion IS NULL OR idlogrecepcion = 0) LIMIT 1")
        rows = cx["cur"].fetchall()
        if len(rows) == 0:
            print("No hay devoluciones de Valdemoro que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
            cx["conn"].commit()
            return True

        ids_comandas = ""
        oAlmacenEcommerce = dameAlmacenEcommerce(cx)
        for d in rows:
            cx["cur"].execute("SELECT idtpv_comanda as idtpv_comanda FROM ew_devolucioneseciweb WHERE codasociacion = '" + str(d["codcomanda"]) + "'")
            rows_ids_comandas = cx["cur"].fetchall()
            for i in rows_ids_comandas:
                if ids_comandas == "":
                    ids_comandas = str(i["idtpv_comanda"])
                else:
                    ids_comandas += "," + str(i["idtpv_comanda"])

            if ids_comandas == "":
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
                cx["conn"].commit()
                return False

            oParam = {}
            oParam["int15"] = int15
            oParam["oAlmacenEcommerce"] = oAlmacenEcommerce
            oParam["idsComanda"] = ids_comandas
            oParam["cx"] = cx
            oParam["codAsociacion"] = str(d["codcomanda"])
            oParam["fechaEntrada"] = str(time.strftime("%Y/%m/%d"))
            creaXmlDevolucionEcommerce(oParam)

            tree = ET.ElementTree(recOrd)
            tree.write("./devValdemoro/xmlDevValdemoro_" + str(d["codcomanda"]) + ".xml")

            xmlstring = tostring(recOrd, 'utf-8', method="xml").decode("ISO8859-15")
            # datosCX = dameDatosConexion("WSIDL_ENVRECECO_TEST", cx)
            datosCX = dameDatosConexion("WSIDL_ENVRECECO", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            """result = True
            status = "OK"""

            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido")
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
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
                tree.write("./devValdemoro/resDevValdemoro_" + str(d["codcomanda"]) + ".xml")

            idlog = registraLog("ENV_DEV_ECOM", xmlstring, res, cx)
            if status:
                if status == "OK":
                    cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET envioidl = TRUE, idlogrecepcion = " + str(idlog) + " WHERE codcomanda = '" + str(d["codcomanda"]) + "'")
                else:
                    error = child.find("error_descriptions/error_description").text
                    print(error)
                    cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET envioidl = TRUE, idlogrecepcion = 0 WHERE codcomanda = '" + str(d["codcomanda"]) + "'")
                cx["conn"].commit()
        else:
            print("No hay devoluciones ecommerce que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
            cx["conn"].commit()
            return True

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
        cx["conn"].commit()
        return False

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_VALDEMORO'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlDevolucionesEcommerce()

    return True


def creaXmlDevolucionEcommerce(oParam):
    oParam["cx"]["cur"].execute("SELECT l.barcode as barcode, l.cantidad as cantidad, l.idtpv_linea as idlinea, l.descripcion as descripcion FROM tpv_lineascomanda l INNER JOIN articulos a ON l.referencia = a.referencia WHERE l.idtpv_comanda IN (" + str(oParam["idsComanda"]) + ") AND l.cantidad < 0 AND a.nostock = FALSE")
    rows = oParam["cx"]["cur"].fetchall()
    numL = 1
    if len(rows) <= 0:
        return False

    rub110 = ET.SubElement(oParam["int15"], "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "receipt_reference").text = "V" + oParam["codAsociacion"]
    ET.SubElement(rub110, "receipt_type").text = "010"
    ET.SubElement(rub110, "receipt_reason_code").text = "DEV"
    ET.SubElement(rub110, "work_mode_code").text = "DEV"
    ET.SubElement(rub110, "original_code").text = "502815"

    ET.SubElement(rub110, "carrier_arrival_date_century").text = str(oParam["fechaEntrada"])[0:2]
    ET.SubElement(rub110, "carrier_arrival_date_year").text = str(oParam["fechaEntrada"])[2:4]
    ET.SubElement(rub110, "carrier_arrival_date_month").text = str(oParam["fechaEntrada"])[5:7]
    ET.SubElement(rub110, "carrier_arrival_date_day").text = str(oParam["fechaEntrada"])[8:10]
    ET.SubElement(rub110, "carrier_arrival_time").text = "0"

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "receipt_reference").text = "V" + oParam["codAsociacion"]
    ET.SubElement(rub119, "comment_line_no").text = "001"
    ET.SubElement(rub119, "comment_group").text = "OWN"

    if oParam["oAlmacenEcommerce"]["nombre"]:
        ET.SubElement(rub119, "comment").text = formateaCadena(str(oParam["oAlmacenEcommerce"]["nombre"]))[0:70]
    else:
        ET.SubElement(rub119, "comment").text = "E-Commerce"

    for l in rows:
        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "receipt_reference").text = "V" + oParam["codAsociacion"]
        ET.SubElement(rub120, "receipt_reference_line_no").text = str(numL)
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity").text = str(int(l["cantidad"]) * (-1))
        ET.SubElement(rub120, "owner_code").text = str(oParam["oAlmacenEcommerce"]["codalmacen"])

        numL += 1

    return True


def dameAlmacenEcommerce(cx):
    oAlmacenEcommerce = {}
    cx["cur"].execute("SELECT codalmacenidl as codalmacenidl, nombre as nombre FROM almacenesidl WHERE codalmacen = 'AWEB'")
    row = cx["cur"].fetchall()
    oAlmacenEcommerce["codalmacen"] = row[0]["codalmacenidl"]
    oAlmacenEcommerce["nombre"] = row[0]["nombre"]
    return oAlmacenEcommerce
