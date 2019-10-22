import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime


def generaXmlClientes():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_CLIENTES'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_CLIENTES','IDL_CLIENTES',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        cx["cur"].execute("SELECT c.tipoimpresionidl AS tipoimpresionidl, idlc.codcliente AS codcliente, c.nombre AS nombrecliente, CASE WHEN (dc.dirtipovia) IS NULL THEN 'N/A' ELSE (dc.dirtipovia) END || ' ' || CASE WHEN (dc.direccion) IS NULL THEN 'N/A' ELSE (dc.direccion) END || ' ' || CASE WHEN (dc.dirnum) IS NULL THEN 'N/A' ELSE (dc.dirnum) END || ' ' || CASE WHEN (dc.dirotros) IS NULL THEN 'N/A' ELSE (dc.dirotros) END AS direccion, dc.ciudad AS ciudad, dc.provincia AS provincia, dc.telefono AS telefono, dc.codpostal AS codpostal, t.codtienda AS codtienda, t.codpais AS codpais, t.outlet AS outlet, t.idempresa AS idempresa, t.servidor AS servidortienda FROM clientes c INNER JOIN idl_clientes idlc ON c.codcliente = idlc.codcliente LEFT JOIN dirclientes dc ON c.codcliente = dc.codcliente LEFT JOIN tpv_tiendas t ON c.codcliente = t.codcliente WHERE idlc.ok = false AND idlc.idlog IS NULL ORDER BY c.codcliente limit 10")
        rows = cx["cur"].fetchall()
        customer = ET.Element("customers")
        clientes = ""
        codCliente = False
        if len(rows) > 0:
            for r in rows:
                if clientes == "":
                    clientes = "'" + r["codcliente"] + "'"
                else:
                    clientes += ",'" + r["codcliente"] + "'"
                print("CODCLIENTE: ", r["codcliente"])
                if codCliente != r["codcliente"]:
                    crearXMLClientes(r, customer, cx)
                    codCliente = r["codcliente"]
        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_CLIENTES'")
            cx["conn"].commit()
            return True

        xmlstring = tostring(customer, 'utf-8', method="xml").decode()
        datosCX = dameDatosConexion("WSIDL_CLI", cx)
        # datosCX = dameDatosConexion("WSIDL_CLI_TEST", cx)
        header = datosCX["header"]
        url = datosCX["url"]
        # result = False
        result = post_request(url, header, xmlstring.encode("utf-8"))

        if not result:
            res[0] = False
            res[1] = result
            print(result)
            print("Error enviando cliente")
            clientes = ""
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_CLIENTES'")
            cx["conn"].commit()
        else:
            res[0] = True
            res[1] = result

            root = ET.fromstring(result)
            for child in root.findall('int08/rub110'):
                codCliente = child.find("consignee_code").text
                status = child.find("status").text
                if codCliente and codCliente != "":
                    if status == "OK":
                        print("")
                        cx["cur"].execute("UPDATE idl_clientes SET ok = true where codcliente = '" + codCliente + "'")
                    else:
                        error = child.find("error_descriptions/error_description").text
                        cx["cur"].execute("UPDATE idl_clientes SET error = '" + error + "' where codcliente = '" + codCliente + "'")
            cx["conn"].commit()

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        clientes = ""

    idlog = registraLog("CLIENTES", xmlstring, res, cx)

    if clientes and idlog:
        cx["cur"].execute("UPDATE idl_clientes SET idlog = " + str(idlog) + ", ok = true where codcliente in (" + clientes + ")")
    cx["conn"].commit()

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_CLIENTES'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlClientes()
    return True


def crearXMLClientes(r, customer, cx):
    codcliente = r["codcliente"]
    nombrecliente = formateaCadena(r["nombrecliente"])

    direccion = formateaCadena(truncarDireccion(str(r["direccion"]).split(" "), 0, 30))
    direccion2 = formateaCadena(truncarDireccion(str(r["direccion"]).split(" "), len(direccion.split(" ")) - 1, 30))
    ciudad = formateaCadena(str(r["ciudad"]))
    provincia = formateaCadena(str(r["provincia"]))
    telefono = r["telefono"]
    codpostal = r["codpostal"]
    codtienda = r["codtienda"]
    codpais = r["codpais"]

    int08 = ET.SubElement(customer, "int08")
    rub110 = ET.SubElement(int08, "rub110")

    if codtienda:
        codcliente = codtienda + "_" + codcliente
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "consignee_code").text = codcliente
    ET.SubElement(rub110, "consignee_designation").text = nombrecliente[0:29]
    ET.SubElement(rub110, "consignee_short_designation").text = nombrecliente[0:14]
    ET.SubElement(rub110, "consignee_keyword").text = "T" + codcliente
    ET.SubElement(rub110, "distribution_channel_code").text = "STD"
    regionCode = "NAC"
    if codpais != "ES":
        regionCode = "INT"
    ET.SubElement(rub110, "region_code").text = regionCode
    ET.SubElement(rub110, "bank_holiday_group_code").text = "ESP"

    if codtienda:
        intermediario = "0"
        if codpais != "CL" and codpais != "MX" and r["servidortienda"] and r["idempresa"] != "15" and r["idempresa"] != "42" and r["idempresa"] != "44":
            intermediario = "1"

        ET.SubElement(rub110, "flag_intermediate_consignee").text = intermediario

    rub111 = ET.SubElement(rub110, "rub111")
    ET.SubElement(rub111, "activity_code").text = "GNS"
    ET.SubElement(rub111, "consignee_code").text = codcliente
    ET.SubElement(rub111, "company_name").text = "El Ganso"

    ET.SubElement(rub111, "consignee_address_1").text = direccion
    if len(direccion2) > 0:
        ET.SubElement(rub111, "consignee_address_2").text = direccion2

    ET.SubElement(rub111, "consignee_address_3").text = ciudad[0:29]
    ET.SubElement(rub111, "consignee_address_4").text = provincia[0:29]
    ET.SubElement(rub111, "consignee_telephone").text = telefono
    ET.SubElement(rub111, "consignee_other_number").text = codpostal

    cx["cur"].execute("SELECT dm.coddivisa AS div, dm.nombre AS nom, dm.separador AS dec FROM idl_divisasmercado dm INNER JOIN tarifas t ON dm.coddivisa = t.coddivisamercado INNER JOIN gruposclientes gc ON t.codtarifa = gc.codtarifa INNER JOIN clientes c ON gc.codgrupo = c.codgrupo WHERE c.codcliente = '" + r["codcliente"] + "'")
    rows = cx["cur"].fetchall()
    commentLine = 1
    commentGroup = ""
    comment = ""

    if len(rows) > 0:
        for d in rows:
            for i in range(3):
                rub119 = ET.SubElement(rub110, "rub119")
                ET.SubElement(rub119, "activity_code").text = "GNS"
                ET.SubElement(rub119, "consignee_code").text = codcliente
                ET.SubElement(rub119, "comment_line_no").text = str(commentLine)
                if str(commentLine) == "1":
                    commentGroup = "DIV"
                    comment = d["div"]
                elif str(commentLine) == "2":
                    commentGroup = "NOM"
                    comment = formateaCadena(d["nom"])
                elif str(commentLine) == "3":
                    commentGroup = "DEC"
                    comment = d["dec"]
                ET.SubElement(rub119, "comment_group").text = commentGroup
                ET.SubElement(rub119, "comment").text = comment
                commentLine = commentLine + 1

    rub117 = ET.SubElement(rub110, "rub117")
    ET.SubElement(rub117, "activity_code").text = "GNS"
    ET.SubElement(rub117, "consignee_code").text = codcliente
    ET.SubElement(rub117, "additional_data_group_code").text = "EL_GANSO"

    rub118_P = ET.SubElement(rub110, "rub118")
    ET.SubElement(rub118_P, "activity_code").text = "GNS"
    ET.SubElement(rub118_P, "consignee_code").text = codcliente
    ET.SubElement(rub118_P, "additional_data_item_code").text = "POR_PREPA"
    # POR_PREPA - Impresión de Albarán por Pedido de Preparación en el último bulto.
    # POR_PACK - Impresión de Albarán por Bulto de Preparación en cada bulto.
    if r["tipoimpresionidl"] == "Por pedido":
        ET.SubElement(rub118_P, "consignee_additional_data_item_value").text = "1"
    else:
        ET.SubElement(rub118_P, "consignee_additional_data_item_value").text = "0"
    # 1 Ejecutar la acción del Albarán.
    # 0 no ejecuta la acción del albarán.

    rub118_K = ET.SubElement(rub110, "rub118")
    ET.SubElement(rub118_K, "activity_code").text = "GNS"
    ET.SubElement(rub118_K, "consignee_code").text = codcliente
    ET.SubElement(rub118_K, "additional_data_item_code").text = "POR_PACK"
    # POR_PREPA - Impresión de Albarán por Pedido de Preparación en el último bulto.
    # POR_PACK - Impresión de Albarán por Bulto de Preparación en cada bulto.
    if r["tipoimpresionidl"] == "Por bulto":
        ET.SubElement(rub118_K, "consignee_additional_data_item_value").text = "1"
    else:
        ET.SubElement(rub118_K, "consignee_additional_data_item_value").text = "0"
    # 1 Ejecutar la acción del Albarán.
    # 0 no ejecuta la acción del albarán.

    tree = ET.ElementTree(customer)
    nombreFichero = dameNombreFichero()
    tree.write("./clientes/" + nombreFichero)


def dameNombreFichero():
    hoy = datetime.datetime.now().strftime("%d%m%Y%H%M")
    print(hoy)
    nombreFichero = "idl_clientes" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero
