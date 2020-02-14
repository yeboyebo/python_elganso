import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlViajesCross():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_VIAJES_CRO','IDL_VIAJES_CRO',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        prepOrd = ET.Element("preparation_orders")
        int16 = ET.SubElement(prepOrd, "int16")

        cx["cur"].execute("SELECT v.idviajemultitrans AS idviaje, a.codalmacenidl AS codalmacen, t.codtienda AS codtienda, c.nombre AS nombrecliente, v.fecha AS fecha, c.codcliente AS codcliente, d.direccion AS direccion, d.codpostal AS codpostal, d.poblacion AS ciudad, d.provincia AS provincia, pa.codiso3 AS codpais, ag.nombre AS agenciatransidl, ag.descripcion AS commentidl, v.codalbarancd FROM tpv_viajesmultitransstock v LEFT JOIN tpv_multitransstock m ON (v.codmultitransstock = m.codmultitransstock AND m.estado = 'Aceptado') INNER JOIN almacenesidl a ON v.codalmaorigen = a.codalmacen INNER JOIN almacenes d ON v.codalmadestino = d.codalmacen LEFT OUTER JOIN tpv_tiendas t ON d.codalmacen = t.codalmacen INNER JOIN clientes c ON c.codcliente = t.codcliente LEFT OUTER JOIN paises pa ON d.codpais = pa.codpais INNER JOIN agenciastrans_idl ag ON c.codagenciaidl = ag.codagenciaidl WHERE codalmadestino NOT IN (SELECT codalmacen FROM almacenesidl) AND v.fecha >= '2018-07-18' AND v.idviajemultitrans = '0000167645'")

        rows = cx["cur"].fetchall()
        idViaje = False
        if len(rows) > 0:
            for p in rows:
                idViaje = p["idviaje"]

                faltaArticulos = False
                cx["cur"].execute("SELECT l.referencia as reflinea, ia.referencia as refidl FROM tpv_lineasmultitransstock l LEFT JOIN idl_articulos ia ON l.referencia = ia.referencia WHERE l.idviajemultitrans = '" + str(idViaje) + "' AND ((ia.referencia IS NULL) OR (ia.referencia IS NOT NULL AND ia.ok = false))")
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
                        cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
                        cx["conn"].commit()
                        return True
                if faltaArticulos:
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
                    cx["conn"].commit()
                    return True

                if not creaXmlEnvioViajeCli(p, int16, cx):
                    print("No hay viajes con lineas que enviar")
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
                    cx["conn"].commit()
                    return False

            tree = ET.ElementTree(prepOrd)
            tree.write("./preparaciones_test/xmlViajesCross_" + idViaje + ".xml")

            xmlstring = tostring(prepOrd, 'utf-8', method="xml").decode("ISO8859-15")
            print(xmlstring)
            datosCX = dameDatosConexion("WSIDL_ENVPREP_TEST", cx)
            #datosCX = dameDatosConexion("WSIDL_ENVPREP", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            result = post_request(url, header, xmlstring)
            # result = "<recepOrders_response><int15><rub110><activity_code>GNS</activity_code><physical_depot_code>GNS</physical_depot_code><status>OK</status><error_descriptions><error_description></error_description></error_descriptions></rub110></int15></recepOrders_response>"

            print(result)

            status = False
            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido")
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
                cx["conn"].commit()
                return False
            else:
                res[0] = True
                res[1] = result

                root = ET.fromstring(result)
                child = root.find('int16/rub110')
                if child:
                    status = child.find("status").text

                tree = ET.ElementTree(root)
                tree.write("./preparaciones/resViajesCli" + idViaje + ".xml")

                idlog = registraLog("ENV_SOLENVIOSV", xmlstring, res, cx)
                if status:
                    if status == "OK":
                        cx["cur"].execute("UPDATE tpv_viajesmultitransstock SET azkarok = true where idviajemultitrans = '" + str(idViaje) + "'")
                        cx["conn"].commit()
                #    else:
                #        error = child.find("error_descriptions/error_description").text
                #        print(error)
                #        cx["cur"].execute("UPDATE pedidoscli SET fichero = 'ERROR: " + str(idlog) + "' where idpedido = " + str(idPedido))
        else:
            print("No hay viajes que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
            cx["conn"].commit()
            return True

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
        cx["conn"].commit()
        return False

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_VIAJES_CRO'")
    cx["conn"].commit()
    cierraConexion(cx)
    #generaXmlViajesCross()
    return True


def creaXmlEnvioViajeCli(p, int16, cx):
    cx["cur"].execute("SELECT barcode, cantidad AS cantidad, idlinea, descripcion, numlinea FROM tpv_lineasmultitransstock WHERE cantidad > 0 and idviajemultitrans = '" + str(p["idviaje"]) + "'")
    rows = cx["cur"].fetchall()
    if len(rows) <= 0:
        return False

    sufijo = "02"

    codcliente = str(p["codcliente"])
    codtienda = str(p["codtienda"])
    if(codtienda):
        codcliente = codtienda + "_" + codcliente

    codcliente = codcliente[0:13]

    rub110 = ET.SubElement(int16, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "originator_reference").text = "R" + p["idviaje"] + sufijo
    ET.SubElement(rub110, "preparation_type_code").text = "010"
    ET.SubElement(rub110, "end_consignee_code").text = codcliente
    ET.SubElement(rub110, "planned_final_delivery_date_century").text = str(p["fecha"])[0:2]
    ET.SubElement(rub110, "planned_final_delivery_date_year").text = str(p["fecha"])[2:4]
    ET.SubElement(rub110, "planned_final_delivery_date_month").text = str(p["fecha"])[5:7]
    ET.SubElement(rub110, "planned_final_delivery_date_day").text = str(p["fecha"])[8:10]
    if str(p["agenciatransidl"]) == "T&W":
        ET.SubElement(rub110, "transport_grade_code").text = str(p["commentidl"])[30:33]
    ET.SubElement(rub110, "flag_automatic_generation").text = "1"

    rub111 = ET.SubElement(rub110, "rub111")
    ET.SubElement(rub111, "activity_code").text = "GNS"
    ET.SubElement(rub111, "physical_depot_code").text = "GNS"
    ET.SubElement(rub111, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub111, "originator_reference").text = "R" + p["idviaje"] + sufijo
    ET.SubElement(rub111, "preparation_order_reason_code").text = "CRO"
    ET.SubElement(rub111, "load_grouping").text = str(p["agenciatransidl"])

    rub11A = ET.SubElement(rub110, "rub11A")
    ET.SubElement(rub11A, "activity_code").text = "GNS"
    ET.SubElement(rub11A, "physical_depot_code").text = "GNS"
    ET.SubElement(rub11A, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub11A, "originator_reference").text = "R" + p["idviaje"] + sufijo
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
    ET.SubElement(rub11A, "iso_country_code").text = str(p["codpais"])[0:3]

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "originator_reference").text = "R" + p["idviaje"] + sufijo
    ET.SubElement(rub119, "comment_line_no").text = "1"
    ET.SubElement(rub119, "comment_group").text = "TRA"

    comentIdl = str(p["commentidl"])
    codclienteIdl = str(p["codcliente"]).ljust(13, " ")

    if str(p["agenciatransidl"]) == "T&W":
        comentIdl = comentIdl + codclienteIdl

    ET.SubElement(rub119, "comment").text = comentIdl

    """cx["cur"].execute("SELECT MAX(r.numenvio) AS numeroenvio FROM albaranescd cd LEFT OUTER JOIN eg_pedidosrecibidos r ON cd.codpedido = r.codpedido  WHERE cd.codigo = '" + str(p["codalbarancd"]) + "'")
    srow = cx["cur"].fetchall()
    sufijoAlbaran = "01"
    if len(srow) > 0:
        for s in srow:
            if s["numeroenvio"] is not None:
                s["numeroenvio"] += 1
                if len(str(s["numeroenvio"])) == 1:
                    sufijoAlbaran = "0" + str(s["numeroenvio"])
                else:
                    sufijoAlbaran = str(s["numeroenvio"])"""
    sufijoAlbaran = "01"
    for l in rows:
        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "originator_reference").text = "R" + p["idviaje"] + sufijo
        ET.SubElement(rub120, "originator_reference_line_no").text = str(int(l["numlinea"]))
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity_to_prepare").text = str(int(l["cantidad"]))
        ET.SubElement(rub120, "owner_code_to_prepare").text = str(p["codalmacen"])[0:3]
        ET.SubElement(rub120, "grade_code_to_prepare").text = "STD"
        rub121 = ET.SubElement(rub120, "rub121")
        ET.SubElement(rub121, "activity_code").text = "GNS"
        ET.SubElement(rub121, "physical_depot_code").text = "GNS"
        ET.SubElement(rub121, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub121, "originator_reference").text = "R" + p["idviaje"] + sufijo
        ET.SubElement(rub121, "originator_reference_line_no").text = str(int(l["numlinea"]))
        ET.SubElement(rub121, "flag_cross-docking").text = "1"
        ET.SubElement(rub121, "flag_cross-docking_even_if_pick_run").text = "1"
        ET.SubElement(rub121, "flag_line_in_cross-docking_allocation").text = "1"
        ET.SubElement(rub121, "pro_reservation_reference").text = "K" + str(p["codalbarancd"]) + sufijoAlbaran

    return True
