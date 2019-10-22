import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlDevolucionesEcommerce():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_ECOMMERCE'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_DEV_ECOMMERCE','IDL_DEV_ECOMMERCE',CURRENT_DATE)")
    cx["conn"].commit()
    # idl_ecommercedevoluciones

    try:
        recOrd = ET.Element("reception_orders")
        int15 = ET.SubElement(recOrd, "int15")

        cx["cur"].execute("SELECT ecodev.id AS idecommercedev, c.idtpv_comanda AS idtpv_comanda, c.codigo AS codigo, c.fecha AS fechaentrada, (SELECT codalmacenidl FROM almacenesidl WHERE codalmacen = 'AWEB') AS codalmacen , (SELECT nombre FROM almacenesidl WHERE codalmacen = 'AWEB') AS nombrealmacen FROM idl_ecommercedevoluciones ecodev INNER JOIN tpv_comandas c ON ecodev.idtpv_comanda = c.idtpv_comanda WHERE ecodev.envioidl = false AND (ecodev.idlogrecepcion IS NULL OR ecodev.idlogrecepcion = 0) AND ecodev.codcomanda NOT LIKE 'VAL%' ORDER BY c.fecha ASC, c.hora ASC LIMIT 1")

        # cx["cur"].execute("SELECT ecodev.id AS idecommercedev, c.idtpv_comanda AS idtpv_comanda, c.codigo AS codigo, c.fecha AS fechaentrada, (SELECT codalmacenidl FROM almacenesidl WHERE codalmacen = 'AWEB') AS codalmacen , (SELECT nombre FROM almacenesidl WHERE codalmacen = 'AWEB') AS nombrealmacen FROM idl_ecommercedevoluciones ecodev INNER JOIN tpv_comandas c ON ecodev.idtpv_comanda = c.idtpv_comanda WHERE ecodev.envioidl = false AND (ecodev.idlogrecepcion IS NULL OR ecodev.idlogrecepcion = 0) and ecodev.codcomanda IN ('WDV000000744','WDV000000756','WDV000000755','WDV000000757','WDV000000761') ORDER BY c.fecha, c.hora LIMIT 1")

        rows = cx["cur"].fetchall()
        idEcommerceDev = False
        codComanda = False

        if len(rows) > 0:
            for q in rows:
                codComanda = q["codigo"]
                idEcommerceDev = int(q["idecommercedev"])
                creaXmlDevolucionEcommerce(q, int15, cx)

            tree = ET.ElementTree(recOrd)
            tree.write("./devecommerce/xmlDevEcommerce_" + codComanda + ".xml")

            xmlstring = tostring(recOrd, 'utf-8', method="xml").decode("ISO8859-15")
            #datosCX = dameDatosConexion("WSIDL_ENVRECECO_TEST", cx)
            datosCX = dameDatosConexion("WSIDL_ENVRECECO", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            # result = False

            status = False
            print(result)

            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido")
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_ECOMMERCE'")
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
                tree.write("./devecommerce/resDevEcommerce_" + codComanda + ".xml")

            idlog = registraLog("ENV_DEV_ECOM", xmlstring, res, cx)
            if status:
                if status == "OK":
                    cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET envioidl = TRUE, idlogrecepcion = " + str(idlog) + " WHERE id = " + str(idEcommerceDev))
                else:
                    error = child.find("error_descriptions/error_description").text
                    print(error)
                    cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET envioidl = TRUE, idlogrecepcion = 0 WHERE id = " + str(idEcommerceDev))
                cx["conn"].commit()
        else:
            print("No hay devoluciones ecommerce que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_ECOMMERCE'")
            cx["conn"].commit()
            return True

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_ECOMMERCE'")
        cx["conn"].commit()
        return False

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEV_ECOMMERCE'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlDevolucionesEcommerce()

    return True


def creaXmlDevolucionEcommerce(q, int15, cx):
    cx["cur"].execute("SELECT l.barcode as barcode, l.cantidad as cantidad, l.idtpv_linea as idlinea, l.descripcion as descripcion FROM tpv_lineascomanda l INNER JOIN articulos a ON l.referencia = a.referencia WHERE l.idtpv_comanda = " + str(q["idtpv_comanda"]) + " AND l.cantidad < 0 AND a.nostock = FALSE")
    rows = cx["cur"].fetchall()
    numL = 1
    if len(rows) <= 0:
        return False

    rub110 = ET.SubElement(int15, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "receipt_reference").text = "V" + q["codigo"]
    ET.SubElement(rub110, "receipt_type").text = "010"
    ET.SubElement(rub110, "receipt_reason_code").text = "DEV"
    ET.SubElement(rub110, "work_mode_code").text = "DEV"
    ET.SubElement(rub110, "original_code").text = "502815"

    ET.SubElement(rub110, "carrier_arrival_date_century").text = str(q["fechaentrada"])[0:2]
    ET.SubElement(rub110, "carrier_arrival_date_year").text = str(q["fechaentrada"])[2:4]
    ET.SubElement(rub110, "carrier_arrival_date_month").text = str(q["fechaentrada"])[5:7]
    ET.SubElement(rub110, "carrier_arrival_date_day").text = str(q["fechaentrada"])[8:10]
    ET.SubElement(rub110, "carrier_arrival_time").text = "0"

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "receipt_reference").text = "V" + q["codigo"]
    ET.SubElement(rub119, "comment_line_no").text = "001"
    ET.SubElement(rub119, "comment_group").text = "OWN"

    if q["nombrealmacen"]:
        ET.SubElement(rub119, "comment").text = formateaCadena(q["nombrealmacen"])[0:70]
    else:
        ET.SubElement(rub119, "comment").text = "E-Commerce"

    for l in rows:
        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "receipt_reference").text = "V" + q["codigo"]
        ET.SubElement(rub120, "receipt_reference_line_no").text = str(numL)
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity").text = str(int(l["cantidad"]) * (-1))
        ET.SubElement(rub120, "owner_code").text = q["codalmacen"]

        numL += 1

    return True
