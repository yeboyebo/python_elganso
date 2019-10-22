import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlViajes():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "IDL_VIAJES_DEST"
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

        cx["cur"].execute("SELECT v.codmultitransstock AS codigo, v.idviajemultitrans AS idviaje, v.tiempotransito AS fechaentrada, v.fecha AS fecha, a.codalmacenidl AS codalmacen, a.nombre AS nombrealmacen, e.codcomanda as codpedidoweb FROM tpv_viajesmultitransstock v INNER JOIN almacenesidl a ON v.codalmadestino = a.codalmacen INNER JOIN almacenes al ON a.codalmacen = al.codalmacen LEFT JOIN idl_ecommercefaltante f ON v.idviajemultitrans = f.idviajemultitrans LEFT JOIN idl_ecommerce e ON f.idecommerce = e.id WHERE v.codalmaorigen NOT IN (SELECT codalmacen FROM almacenesidl) AND (v.estado = 'EN TRANSITO' OR v.estado = 'RECIBIDO PARCIAL') AND v.enviocompletado = true AND v.azkarok = false AND v.fecha >= '2018-09-01' AND v.idviajemultitrans IN (SELECT idviajemultitrans FROM tpv_lineasmultitransstock WHERE idviajemultitrans = v.idviajemultitrans) AND v.idviajemultitrans NOT IN (SELECT clave FROM idl_erroneos WHERE tipo = '" + tipo + "') ORDER BY v.fecha ASC LIMIT 1")
        #cx["cur"].execute("SELECT v.codmultitransstock AS codigo, v.idviajemultitrans AS idviaje, v.tiempotransito AS fechaentrada, v.fecha AS fecha, a.codalmacenidl AS codalmacen, a.nombre AS nombrealmacen, e.codcomanda as codpedidoweb FROM tpv_viajesmultitransstock v INNER JOIN almacenesidl a ON v.codalmadestino = a.codalmacen INNER JOIN almacenes al ON a.codalmacen = al.codalmacen  LEFT JOIN idl_ecommercefaltante f ON v.idviajemultitrans = f.idviajemultitrans LEFT JOIN idl_ecommerce e ON f.idecommerce = e.id WHERE v.idviajemultitrans = '0000174109' AND (v.estado = 'EN TRANSITO' OR v.estado = 'RECIBIDO PARCIAL') AND v.enviocompletado = true AND v.azkarok = false AND v.fecha >= '2018-09-01' AND v.idviajemultitrans IN (SELECT idviajemultitrans FROM tpv_lineasmultitransstock WHERE idviajemultitrans = v.idviajemultitrans) ORDER BY v.fecha ASC LIMIT 1")

        rows = cx["cur"].fetchall()
        idViaje = False
        if len(rows) > 0:
            for p in rows:
                idViaje = str(p["idviaje"])
                print(idViaje)

                faltaArticulos = False
                cx["cur"].execute("SELECT l.referencia as reflinea, ia.referencia as refidl FROM tpv_lineasmultitransstock l LEFT JOIN idl_articulos ia ON l.referencia = ia.referencia WHERE l.idviajemultitrans = '" + str(idViaje) + "' AND ((ia.referencia IS NULL) OR (ia.referencia IS NOT NULL AND ia.ok = false)) GROUP BY l.referencia, ia.referencia")
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
                        registraError(tipo, idViaje, art["reflinea"], cx)

                if faltaArticulos:
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()
                    return True

                if not creaXmlRecepcionViaje(p, int15, cx):
                    print("No hay Viajes con líneas que enviar")
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
                    cx["conn"].commit()
                    registraError(tipo, idViaje, "No hay Viajes con líneas que enviar", cx)
                    return False

            tree = ET.ElementTree(recOrd)
            tree.write("./recepciones/xmlViajes" + str(idViaje) + ".xml")

            xmlstring = tostring(recOrd, 'utf-8', method="xml").decode("ISO8859-15")
            #xmlstring = tostring(recOrd, 'utf-8', method="xml")
            # datosCX = dameDatosConexion("WSIDL_ENVREC_TEST", cx)
            datosCX = dameDatosConexion("WSIDL_ENVREC", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            #result = "<recepOrders_response><int15><rub110><activity_code>GNS</activity_code><physical_depot_code>GNS</physical_depot_code><status>OK</status><error_descriptions><error_description></error_description></error_descriptions></rub110></int15></recepOrders_response>"
            status = False

            #print(xmlstring)
            #print(result)
            #print(header)
            #print(url)
            #cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            #cx["conn"].commit()
            #return False

            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando Viaje")

                cx["cur"].execute("UPDATE tpv_viajesmultitransstock SET estado = 'ERROR IDL' where idviajemultitrans = '" + str(idViaje) + "'")
                cx["conn"].commit()
            else:
                res[0] = True
                res[1] = result

                root = ET.fromstring(result)
                child = root.find('int15/rub110')
                if child:
                    status = child.find("status").text

                tree = ET.ElementTree(root)
                tree.write("./recepciones/resViajes.xml")

            idlog = registraLog("ENV_RECEPCIONES", xmlstring, res, cx)
            if status:
                if status == "OK":
                    print("ok")
                    cx["cur"].execute("UPDATE tpv_viajesmultitransstock SET azkarok = true where idviajemultitrans = '" + str(idViaje) + "'")
                    cx["conn"].commit()
                else:
                    cx["cur"].execute("UPDATE tpv_viajesmultitransstock SET estado = 'ERROR IDL' where idviajemultitrans = '" + str(idViaje) + "'")
                    cx["conn"].commit()
                    error = child.find("error_descriptions/error_description").text
                    print(error)
                    registraError(tipo, idViaje, error, cx)
        else:
            print("No hay viajes que recibir")
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
    generaXmlViajes()
    return True


def creaXmlRecepcionViaje(p, int15, cx):
    cx["cur"].execute("SELECT barcode, cantenviada, idlinea, descripcion FROM tpv_lineasmultitransstock WHERE cantenviada > 0 and idviajemultitrans = '" + str(p["idviaje"] + "'"))
    rows = cx["cur"].fetchall()
    numL = 1
    if len(rows) <= 0:
        return False

    rub110 = ET.SubElement(int15, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "receipt_reference").text = "V" + p["idviaje"] + "01"
    ET.SubElement(rub110, "receipt_type").text = "010"
    ET.SubElement(rub110, "receipt_reason_code").text = "DEV"

    if p["codpedidoweb"]:
        ET.SubElement(rub110, "work_mode_code").text = "REP"
    else:
        ET.SubElement(rub110, "work_mode_code").text = "REC"

    ET.SubElement(rub110, "original_code").text = dameProveedorIDL(cx)
    ET.SubElement(rub110, "carrier_arrival_date_century").text = str(p["fechaentrada"])[0:2]
    ET.SubElement(rub110, "carrier_arrival_date_year").text = str(p["fechaentrada"])[2:4]
    ET.SubElement(rub110, "carrier_arrival_date_month").text = str(p["fechaentrada"])[5:7]
    ET.SubElement(rub110, "carrier_arrival_date_day").text = str(p["fechaentrada"])[8:10]
    ET.SubElement(rub110, "carrier_arrival_time").text = "0"

    if p["codpedidoweb"]:
        ET.SubElement(rub110, "flag_receipt_in_cross-docking").text = "1"

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "receipt_reference").text = "V" + p["idviaje"] + "01"
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
        ET.SubElement(rub120, "receipt_reference").text = "V" + p["idviaje"] + "01"
        ET.SubElement(rub120, "receipt_reference_line_no").text = str(numL)
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity").text = str(int(l["cantenviada"]))
        ET.SubElement(rub120, "owner_code").text = p["codalmacen"]

        if p["codpedidoweb"]:
            rub122 = ET.SubElement(rub120, "rub122")

            ET.SubElement(rub122, "activity_code").text = "GNS"
            ET.SubElement(rub122, "physical_depot_code").text = "GNS"
            ET.SubElement(rub122, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub122, "receipt_reference").text = "V" + p["idviaje"] + "01"
            ET.SubElement(rub122, "receipt_reference_line_no").text = str(numL)
            ET.SubElement(rub122, "pro_reservation_reference").text = "T" + p["codpedidoweb"]
        numL += 1

    return True


def dameProveedorIDL(cx):
    cx["cur"].execute("select valor from param_parametros where nombre = 'PROV_IDL'")
    row = cx["cur"].fetchall()
    valor = row[0]["valor"]

    return valor
