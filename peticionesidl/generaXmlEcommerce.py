import base64
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlEcommerce():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_ECOMMERCE','IDL_ECOMMERCE',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        prepOrd = ET.Element("preparation_orders")
        int16 = ET.SubElement(prepOrd, "int16")

        #Metodo envio idl APLAZADO% es para cuando se quite el estado de Alarma. No quitaremos el código en las tablas para que quede constancia de qué pedidos son. Luego habrá que quitarlo de la consulta. En el nodo para IDL ya está controlado.

        cx["cur"].execute("SELECT eco.id AS idecommerce, c.idtpv_comanda AS idtpv_comanda, c.codigo AS codigo, c.egcodfactura AS codfactura, (SELECT codalmacenidl FROM almacenesidl WHERE codalmacen = 'AWEB') AS codalmacen, de.mg_nombreenv AS nombrecliente, de.mg_apellidosenv AS apellidoscliente, c.fecha AS fecha, c.fecha AS fechasalida, de.mg_dirtipoviaenv AS dirtipovia, de.mg_direccionenv AS direccion, de.mg_dirnumenv AS dirnum, de.mg_dirotrosenv AS dirotros, de.mg_codpostalenv AS codpostal, de.mg_ciudadenv AS ciudad, de.mg_provinciaenv AS provincia, c.recogidatienda AS recogidatienda, c.codtiendarecogida AS codtiendarecogida, de.mg_email AS email, de.mg_telefonoenv AS telefono, c.cifnif AS cifnif, pa.codiso3 AS codpais, i.textoalbaranidl AS textoalbaranidl, eco.esregalo AS esregalo, eco.imprimiralbaran AS imprimiralbaran, eco.imprimirfactura AS imprimirfactura, eco.imprimirdedicatoria AS imprimirdedicatoria, eco.emisor AS emisor, eco.receptor as receptor, eco.mensajededicatoria AS mensajededicatoria, eco.transportista AS transportista, eco.metodoenvioidl AS metodoenvio, eco.tipo AS tipoenvio, eco.eseciweb AS eseciweb, t.codcliente AS clientetienda, t.descripcion AS nombretienda, t.dirtipovia AS dirtipoviatienda, t.direccion AS direcciontienda, t.dirnum AS dirnumtienda, t.dirotros AS dirotrostienda, t.codpostal AS codpostaltienda, t.ciudad AS ciudadtienda, t.provincia AS provinciatienda, pat.codiso3 AS codpaistienda, it.textoalbaranidl AS textoalbaranidltienda, c.nombrecliente AS nombreclientecomanda, c.dirtipovia AS dirtipoviacomanda, c.direccion AS direccioncomanda, c.dirnum AS dirnumcomanda, c.dirotros AS dirotroscomanda, c.codpostal AS codpostalcomanda, c.ciudad AS ciudadcomanda, c.provincia AS provinciacomanda, c.email AS emailcomanda, c.telefono1 AS telefonocomanda, pac.codiso3 AS codpaiscomanda, ic.textoalbaranidl AS textoalbaranidlcomanda FROM idl_ecommerce eco INNER JOIN tpv_comandas c ON (eco.idtpv_comanda = c.idtpv_comanda AND eco.codcomanda = c.codigo) LEFT OUTER JOIN mg_datosenviocomanda de ON c.idtpv_comanda = de.idtpv_comanda LEFT OUTER JOIN paises pa ON de.mg_paisenv = pa.codpais LEFT OUTER JOIN idiomas i ON pa.codidioma = i.codidioma LEFT OUTER JOIN paises pac ON c.codpais = pac.codpais LEFT OUTER JOIN idiomas ic ON pac.codidioma = ic.codidioma LEFT OUTER JOIN tpv_tiendas t ON c.codtiendarecogida = t.codtienda LEFT OUTER JOIN paises pat ON t.codpais = pat.codpais LEFT OUTER JOIN idiomas it ON pat.codidioma = it.codidioma WHERE eco.envioidl = false AND (eco.idlogenvio IS NULL OR eco.idlogenvio = 0) AND (eco.imprimirfactura = false OR (eco.imprimirfactura = true AND eco.facturaimpresa = true)) AND (c.egcodpedidoweb is null or c.egcodpedidoweb <> 'Pendiente') AND eco.codcomanda IS NOT NULL AND (eco.metodoenvioidl NOT LIKE 'APLAZADO%' OR (eco.metodoenvioidl LIKE 'APLAZADO%' AND fechaprevistaenvio <= CURRENT_DATE)) ORDER BY c.fecha ASC, c.hora ASC LIMIT 1")

        rows = cx["cur"].fetchall()
        idEcommerce = False
        codComanda = False

        if len(rows) > 0:
            for q in rows:
                codComanda = q["codigo"]
                idEcommerce = int(q["idecommerce"])
                if not creaXmlEnvioEcommerce(q, int16, cx):
                    cx["cur"].execute("UPDATE idl_ecommerce SET envioidl = TRUE, idlogenvio = 0, confirmacionenvio = 'Si' WHERE id = " + str(idEcommerce))
                    cx["conn"].commit()
                    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
                    cx["conn"].commit()
                    cierraConexion(cx)
                    generaXmlEcommerce()
                    return True

            tree = ET.ElementTree(prepOrd)
            tree.write("./ecommerce/xmlEcommerce_" + codComanda + ".xml")

            xmlstring = tostring(prepOrd, 'utf-8', method="xml")
            datosCX = dameDatosConexion("WSIDL_ENVECO", cx)
            # datosCX = dameDatosConexion("WSIDL_ENVECO_TEST", cx)
            header = datosCX["header"]
            url = datosCX["url"]
            # result = False
            result = post_request(url, header, xmlstring)
            print(codComanda)
            
            # print(xmlstring)
            # print(result)
            # print(header)
            # print(url)
            # cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
            # cx["conn"].commit()
            # return False

            status = False
            if not result:
                res[0] = False
                res[1] = result
                print(result)
                print("Error enviando pedido ecommerce")
                cx["cur"].execute("UPDATE idl_ecommerce SET envioidl = TRUE, idlogenvio = 0 WHERE id = " + str(idEcommerce))
                cx["conn"].commit()
                cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
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
                tree.write("./ecommerce/resEcommerce_" + codComanda + ".xml")

                idlog = registraLog("ENV_ECOMMERCE", xmlstring.decode("ISO8859-15"), res, cx)

                if status:
                    if status == "OK":
                        cx["cur"].execute("UPDATE idl_ecommerce SET envioidl = TRUE, idlogenvio = " + str(idlog) + " WHERE id = " + str(idEcommerce))
                    else:
                        error = child.find("error_descriptions/error_description").text
                        print(error)
                        cx["cur"].execute("UPDATE idl_ecommerce SET envioidl = TRUE, idlogenvio = 0 WHERE id = " + str(idEcommerce))
                    cx["conn"].commit()
        else:
            print("No hay pedidos ecommerce que enviar")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
            cx["conn"].commit()
            return True

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ECOMMERCE'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlEcommerce()

    return True


def creaXmlEnvioEcommerce(q, int16, cx):
    # Jesús. Añado una comprobación para que no se envien líneas que estén en eg_lineasecommerceexcluidas
    cx["cur"].execute("SELECT l.barcode AS barcode, l.cantidad AS cantidad, l.idtpv_linea AS idtpv_linea, l.descripcion AS descripcion FROM tpv_lineascomanda l INNER JOIN articulos a ON l.referencia = a.referencia LEFT OUTER JOIN eg_lineasecommerceexcluidas le ON l.idtpv_linea = le.idtpv_linea WHERE l.idtpv_comanda = " + str(q["idtpv_comanda"]) + " AND l.cantidad > 0 AND a.nostock = FALSE AND le.id IS NULL")
    rows = cx["cur"].fetchall()
    if len(rows) <= 0:
        return False

    esContraReembolso = False
    recogidaTienda = False

    cx["cur"].execute("SELECT SUM(p.importe) AS importe FROM tpv_pagoscomanda p WHERE p.idtpv_comanda = " + str(q["idtpv_comanda"]) + " AND p.codpago = 'CREE' AND (p.codcomanda like 'WDV%' OR p.codcomanda like 'WEB%') GROUP BY p.codpago, p.idtpv_comanda HAVING SUM(p.importe) > 0")
    rowsPago = cx["cur"].fetchall()

    if len(rowsPago) > 0:
        esContraReembolso = True

    if q["recogidatienda"]:
        cx["cur"].execute("SELECT codtienda AS codtienda FROM tpv_tiendas t WHERE t.codtienda = '" + str(q["codtiendarecogida"]) + "'")
        rowsTiendaRecogida = cx["cur"].fetchall()
        if len(rowsTiendaRecogida) > 0:
            recogidaTienda = True

    direccion = ""
    codPostal = ""
    ciudad = ""
    provincia = ""
    codPais = ""
    destino = ""
    textoAlbaranIdl = ""

    tipoVenta = ""

    if str(q["tipoenvio"]) != "VENTA":
        tipoVenta = "comanda"

    if recogidaTienda:
        if q["dirtipoviatienda"]:
            direccion += str(q["dirtipoviatienda"]) + " "

        direccion += str(q["direcciontienda"])

        if q["dirnumtienda"]:
            direccion += ", " + str(q["dirnumtienda"])
        if q["dirotrostienda"]:
            direccion += ", " + str(q["dirotrostienda"])

        destino = "EL GANSO " + formateaCadenaEcommerce(str(q["nombretienda"]))[0:35]
        codPostal = str(q["codpostaltienda"])[0:9]
        ciudad = formateaCadenaEcommerce(str(q["ciudadtienda"]))[0:35]
        provincia = formateaCadenaEcommerce(str(q["provinciatienda"]))[0:35]
        codPais = str(q["codpaistienda"])
        textoAlbaranIdl = formateaCadenaEcommerce(str(q["textoalbaranidltienda"]))[0:70]

    else:
        if q["dirtipovia" + tipoVenta]:
            direccion += str(q["dirtipovia" + tipoVenta]) + " "

        direccion += str(q["direccion" + tipoVenta])

        if q["dirnum" + tipoVenta]:
            direccion += ", " + str(q["dirnum" + tipoVenta])
        if q["dirotros" + tipoVenta]:
            direccion += ", " + str(q["dirotros" + tipoVenta])

        if tipoVenta == "comanda":
            destino = formateaCadenaEcommerce(str(q["nombreclientecomanda"]))[0:35]
        else:
            destino = str(q["nombrecliente"]) + " " + str(q["apellidoscliente"])
            destino = formateaCadenaEcommerce(destino)[0:35]

        codPostal = str(q["codpostal" + tipoVenta])[0:9]
        ciudad = formateaCadenaEcommerce(str(q["ciudad" + tipoVenta]))[0:35]
        provincia = formateaCadenaEcommerce(str(q["provincia" + tipoVenta]))[0:35]

        if (provincia == "None" or provincia == "") and tipoVenta == "":
            provincia = formateaCadenaEcommerce(str(q["provinciacomanda"]))[0:35]

        codPais = str(q["codpais" + tipoVenta])
        textoAlbaranIdl = formateaCadenaEcommerce(str(q["textoalbaranidl" + tipoVenta]))[0:70]

    rub110 = ET.SubElement(int16, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "physical_depot_code").text = "GNS"
    ET.SubElement(rub110, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub110, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub110, "preparation_type_code").text = "010"
    ET.SubElement(rub110, "end_consignee_code").text = "ECOMMERCE"

    if esContraReembolso:
        for p in rowsPago:
            ET.SubElement(rub110, "end_consignee_reference").text = dameImporteDecimal(str(p["importe"]))
            continue

    ET.SubElement(rub110, "planned_final_delivery_date_century").text = str(q["fechasalida"])[0:2]
    ET.SubElement(rub110, "planned_final_delivery_date_year").text = str(q["fechasalida"])[2:4]
    ET.SubElement(rub110, "planned_final_delivery_date_month").text = str(q["fechasalida"])[5:7]
    ET.SubElement(rub110, "planned_final_delivery_date_day").text = str(q["fechasalida"])[8:10]
    ET.SubElement(rub110, "flag_automatic_generation").text = "1"

    if recogidaTienda:
        ET.SubElement(rub110, "flag_intermediate_consignee1").text = "1"
        tiendaRecogida = str(q["codtiendarecogida"]) + "_" + str(q["clientetienda"])
        ET.SubElement(rub110, "intermediate_consignee_code").text = str(tiendaRecogida)[0:13]
        ET.SubElement(rub110, "intermediate_delivery_date_century").text = str(q["fechasalida"])[0:2]
        ET.SubElement(rub110, "intermediate_delivery_date_year").text = str(q["fechasalida"])[2:4]
        ET.SubElement(rub110, "intermediate_delivery_date_month").text = str(q["fechasalida"])[5:7]
        ET.SubElement(rub110, "intermediate_delivery_date_day").text = str(q["fechasalida"])[8:10]
    else:
        ET.SubElement(rub110, "flag_intermediate_consignee1").text = "0"

    rub111 = ET.SubElement(rub110, "rub111")
    ET.SubElement(rub111, "activity_code").text = "GNS"
    ET.SubElement(rub111, "physical_depot_code").text = "GNS"
    ET.SubElement(rub111, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub111, "originator_reference").text = "T" + q["codigo"]

    if str(q["tipoenvio"]) == "CAMBIO":
        ET.SubElement(rub111, "preparation_order_reason_code").text = "DEV"
    else:
        ET.SubElement(rub111, "preparation_order_reason_code").text = "ECO"

    transportista = str(q["transportista"])

    # if transportista != "SEUR" and (str(q["tipoenvio"]) == "CAMBIO" or codPais == "RUS"):
        # transportista = "UPS"

    ET.SubElement(rub111, "load_grouping").text = transportista

    rub11A = ET.SubElement(rub110, "rub11A")
    ET.SubElement(rub11A, "activity_code").text = "GNS"
    ET.SubElement(rub11A, "physical_depot_code").text = "GNS"
    ET.SubElement(rub11A, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub11A, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub11A, "address_type_code").text = "010"

    direccion = formateaCadenaEcommerce(direccion)
    direccion1 = truncarDireccion(str(direccion).split(" "), 0, 35)

    direccion1 = direccion1[0:(len(direccion1) - 1)]

    ET.SubElement(rub11A, "name_or_company_name_in_address").text = destino

    ET.SubElement(rub11A, "street_and_number_and_or_po_box").text = direccion1
    if len(direccion) >= 35:
        direccion2 = truncarDireccion(str(direccion).split(" "), len(direccion1.split(" ")), 35)
        direccion2 = direccion2[0:(len(direccion2) - 1)]
        ET.SubElement(rub11A, "additional_address_data_1").text = direccion2

    ET.SubElement(rub11A, "additional_address_data_2").text = provincia
    ET.SubElement(rub11A, "post_code_area_name").text = ciudad
    ET.SubElement(rub11A, "postal_code").text = codPostal
    ET.SubElement(rub11A, "iso_country_code").text = codPais

    rub114 = ET.SubElement(rub110, "rub114")
    ET.SubElement(rub114, "activity_code").text = "GNS"
    ET.SubElement(rub114, "physical_depot_code").text = "GNS"
    ET.SubElement(rub114, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub114, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub114, "contact_type_code").text = "010"
    ET.SubElement(rub114, "title_code").text = "1"

    nombre = formateaCadenaEcommerce(str(q["nombrecliente" + tipoVenta]))
    apellidos = formateaCadenaEcommerce(str(q["apellidoscliente"]))

    # nombre1 = str(nombre).split(" ")[0][0:35]
    # nombre2 = truncarDireccion(str(nombre).split(" "), 1, 35)
    # nombre2 = nombre2[0:(len(nombre2) - 1)]

    if len(nombre) < 1:
        nombre = "."
    if len(apellidos) < 1:
        nombre2 = truncarDireccion(str(nombre).split(" "), 1, 35)
        apellidos = nombre2[0:(len(nombre2) - 1)]
        if len(apellidos) < 1:
            apellidos = "."

    ET.SubElement(rub114, "contact_first_name").text = nombre[0:35]
    ET.SubElement(rub114, "contact_last_name").text = apellidos[0:35]
    ET.SubElement(rub114, "contact_address_1").text = str(q["email" + tipoVenta])[0:140]

    rub115 = ET.SubElement(rub110, "rub115")
    ET.SubElement(rub115, "activity_code").text = "GNS"
    ET.SubElement(rub115, "physical_depot_code").text = "GNS"
    ET.SubElement(rub115, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub115, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub115, "contact_type_code").text = "010"
    ET.SubElement(rub115, "mobile_phone_number").text = formateaCadenaEcommerceTelefono(str(q["telefono" + tipoVenta])[0:20])

    if str(q["tipoenvio"]) == "CAMBIO":
        ET.SubElement(rub115, "land_line_phone_number").text = "S"
    else:
        ET.SubElement(rub115, "land_line_phone_number").text = "N"

    ET.SubElement(rub115, "fax_number").text = str(q["cifnif"])[0:20]

    contador = 1

    if q["eseciweb"] and str(q["eseciweb"]) != "None" and str(q["eseciweb"]) != "":
        rub119 = ET.SubElement(rub110, "rub119")
        ET.SubElement(rub119, "activity_code").text = "GNS"
        ET.SubElement(rub119, "physical_depot_code").text = "GNS"
        ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
        ET.SubElement(rub119, "comment_group").text = "INS"
        ET.SubElement(rub119, "comment").text = "EL CORTE INGLES"
        contador += 1

    if q["esregalo"]:
        rub119 = ET.SubElement(rub110, "rub119")
        ET.SubElement(rub119, "activity_code").text = "GNS"
        ET.SubElement(rub119, "physical_depot_code").text = "GNS"
        ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
        ET.SubElement(rub119, "comment_group").text = "INS"
        ET.SubElement(rub119, "comment").text = "REGALO"
        contador += 1

    if esContraReembolso:
        rub119 = ET.SubElement(rub110, "rub119")
        ET.SubElement(rub119, "activity_code").text = "GNS"
        ET.SubElement(rub119, "physical_depot_code").text = "GNS"
        ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
        ET.SubElement(rub119, "comment_group").text = "INS"
        ET.SubElement(rub119, "comment").text = "REEMBOLSO 1 SOLO PACK"
        contador += 1

    if recogidaTienda:
        rub119 = ET.SubElement(rub110, "rub119")
        ET.SubElement(rub119, "activity_code").text = "GNS"
        ET.SubElement(rub119, "physical_depot_code").text = "GNS"
        ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
        ET.SubElement(rub119, "comment_group").text = "INS"
        ET.SubElement(rub119, "comment").text = "ALBARAN POR FUERA"
        contador += 1

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
    ET.SubElement(rub119, "comment_group").text = "ALB"

    if q["imprimiralbaran"]:
        ET.SubElement(rub119, "comment").text = "1"
    else:
        ET.SubElement(rub119, "comment").text = "0"
    contador += 1

    if q["imprimirdedicatoria"] and str(q["mensajededicatoria"]) != "None" and str(q["receptor"]) != "None" and str(q["emisor"]) != "None" and str(q["mensajededicatoria"]) != "" and str(q["receptor"]) != "" and str(q["emisor"]) != "":
        dedicatoria = formateaCadenaEcommerce(str(q["mensajededicatoria"]))

        if len(dedicatoria) > 0:
            me1 = truncarDireccion(str(dedicatoria).split(" "), 0, 70)

            rub119 = ET.SubElement(rub110, "rub119")
            ET.SubElement(rub119, "activity_code").text = "GNS"
            ET.SubElement(rub119, "physical_depot_code").text = "GNS"
            ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
            ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
            ET.SubElement(rub119, "comment_group").text = "ME1"
            ET.SubElement(rub119, "comment").text = me1
            contador += 1

            if len(dedicatoria) > 70:
                me2 = truncarDireccion(str(dedicatoria).split(" "), len(me1.split(" ")) - 1, 70)

                rub119 = ET.SubElement(rub110, "rub119")
                ET.SubElement(rub119, "activity_code").text = "GNS"
                ET.SubElement(rub119, "physical_depot_code").text = "GNS"
                ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
                ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
                ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
                ET.SubElement(rub119, "comment_group").text = "ME2"
                ET.SubElement(rub119, "comment").text = me2
                contador += 1

            if len(dedicatoria) > 140:
                me3 = truncarDireccion(str(dedicatoria).split(" "), (len(me1.split(" ")) + len(me2.split(" "))) - 2, 70)

                rub119 = ET.SubElement(rub110, "rub119")
                ET.SubElement(rub119, "activity_code").text = "GNS"
                ET.SubElement(rub119, "physical_depot_code").text = "GNS"
                ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
                ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
                ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
                ET.SubElement(rub119, "comment_group").text = "ME3"
                ET.SubElement(rub119, "comment").text = me3
                contador += 1

            receptor = formateaCadenaEcommerce(str(q["receptor"]))[0:70]

            rub119 = ET.SubElement(rub110, "rub119")
            ET.SubElement(rub119, "activity_code").text = "GNS"
            ET.SubElement(rub119, "physical_depot_code").text = "GNS"
            ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
            ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
            ET.SubElement(rub119, "comment_group").text = "PAR"
            ET.SubElement(rub119, "comment").text = receptor
            contador += 1

            emisor = formateaCadenaEcommerce(str(q["emisor"]))

            rub119 = ET.SubElement(rub110, "rub119")
            ET.SubElement(rub119, "activity_code").text = "GNS"
            ET.SubElement(rub119, "physical_depot_code").text = "GNS"
            ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
            ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
            ET.SubElement(rub119, "comment_group").text = "DE"
            ET.SubElement(rub119, "comment").text = emisor
            contador += 1

    if q["imprimirfactura"]:
        rub119 = ET.SubElement(rub110, "rub119")
        ET.SubElement(rub119, "activity_code").text = "GNS"
        ET.SubElement(rub119, "physical_depot_code").text = "GNS"
        ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
        ET.SubElement(rub119, "comment_group").text = "FAC"
        ET.SubElement(rub119, "comment").text = "T" + q["codigo"] + ".pdf"
        contador += 1

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
    ET.SubElement(rub119, "comment_group").text = "TCK"

    if textoAlbaranIdl == "":
        cx["cur"].execute("SELECT textoalbaranidl FROM idiomas WHERE codidioma = 'EN'")
        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for a in rows:
                textoAlbaranIdl = formateaCadenaEcommerce(str(a["textoalbaranidl"]))[0:70]

    ET.SubElement(rub119, "comment").text = textoAlbaranIdl

    contador += 1

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "physical_depot_code").text = "GNS"
    ET.SubElement(rub119, "originator_code").text = "EL_GANSO"
    ET.SubElement(rub119, "originator_reference").text = "T" + q["codigo"]
    ET.SubElement(rub119, "comment_line_no").text = cerosIzquierda(contador, 3)
    ET.SubElement(rub119, "comment_group").text = "TRA"

    if str(q["metodoenvio"])[0:8] == "APLAZADO":
    	metodoEnvio = str(q["metodoenvio"])[8:78]
    else:
    	metodoEnvio = str(q["metodoenvio"])[0:70]

    if metodoEnvio == "None" and transportista == "SEUR":
        metodoEnvio = "B2C/STD"

    if str(q["tipoenvio"]) == "CAMBIO" and metodoEnvio == "B2C/STD":
        metodoEnvio = metodoEnvio + " CAMBIO"

    # if str(q["tipoenvio"]) == "CAMBIO" and transportista != "SEUR":
        # metodoEnvio = "UPS CAMBIO"

    if esContraReembolso and transportista == "SEUR":
        metodoEnvio = metodoEnvio + " R"

    if q["imprimirfactura"] and transportista == "GLS":
        metodoEnvio = "12 3"

    ET.SubElement(rub119, "comment").text = metodoEnvio

    if q["imprimirfactura"]:
        fraPdf = False
        try:
            ruta = "/home/elganso/facturasIdlPdf/" + "T" + q["codigo"] + ".pdf"
            fraPdf = open(ruta, "rb").read()
            fra_base64 = str(base64.b64encode(fraPdf))
            fra_base64 = formateaCadenaEcommerce(fra_base64)[1:len(fra_base64)]
            rub11F = ET.SubElement(rub110, "rub11F")
            ET.SubElement(rub11F, "activity_code").text = "GNS"
            ET.SubElement(rub11F, "physical_depot_code").text = "GNS"
            ET.SubElement(rub11F, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub11F, "originator_reference").text = "T" + q["codigo"]
            ET.SubElement(rub11F, "invoice_data").text = fra_base64
            # print(fra_base64)
        except Exception as e:
            print(e)

    i = 1
    for l in rows:
        rub120 = ET.SubElement(rub110, "rub120")
        ET.SubElement(rub120, "activity_code").text = "GNS"
        ET.SubElement(rub120, "physical_depot_code").text = "GNS"
        ET.SubElement(rub120, "originator_code").text = "EL_GANSO"
        ET.SubElement(rub120, "originator_reference").text = "T" + q["codigo"]
        ET.SubElement(rub120, "originator_reference_line_no").text = str(i)
        ET.SubElement(rub120, "item_code").text = str(l["barcode"])[0:16]
        ET.SubElement(rub120, "item_lv_code").text = "11"
        ET.SubElement(rub120, "level_1_quantity_to_prepare").text = str(int(l["cantidad"]))
        ET.SubElement(rub120, "owner_code_to_prepare").text = "ECO"
        ET.SubElement(rub120, "grade_code_to_prepare").text = "STD"

        if esContraReembolso or transportista != "SEUR" or q["imprimirfactura"] or (q["eseciweb"] and str(q["eseciweb"]) != "None" and str(q["eseciweb"]) != ""):
            rub121 = ET.SubElement(rub120, "rub121")
            ET.SubElement(rub121, "activity_code").text = "GNS"
            ET.SubElement(rub121, "physical_depot_code").text = "GNS"
            ET.SubElement(rub121, "originator_code").text = "EL_GANSO"
            ET.SubElement(rub121, "originator_reference").text = "T" + q["codigo"]
            ET.SubElement(rub121, "originator_reference_line_no").text = str(i)
            ET.SubElement(rub121, "flag_cross-docking").text = "1"
            ET.SubElement(rub121, "flag_cross-docking_even_if_pick_run").text = "1"
            ET.SubElement(rub121, "pro_reservation_reference").text = "T" + q["codigo"]
        i += 1

    return True


def dameImporteDecimal(importe):
    if not importe or importe == "":
        return False

    oImporte = importe.split(".")
    parteEntera = str(oImporte[0])
    parteDecimal = str(oImporte[1])

    while len(parteEntera) < 18:
        parteEntera = "0" + parteEntera

    while len(parteDecimal) < 2:
        parteDecimal = parteDecimal + "0"

    return str(parteEntera + parteDecimal)
