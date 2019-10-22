import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def generaXmlProveedores():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "IDL_PROVEEDORES"

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        supReq = ET.Element("suppliers")
        int12 = ET.SubElement(supReq, "int12")
        proveedores = ""
        # insert into idl_proveedores (fecha, hora, codproveedor, ok) select CURRENT_DATE, CURRENT_TIME, p.codproveedor, false from proveedores p where codproveedor in ('10032','10001','502181');
        cx["cur"].execute("SELECT i.codproveedor as codproveedor, p.nombre as nombre, d.dirtipovia as tipovia, d.direccion as direccion, d.dirnum as numero, d.dirotros as otros, d.codpostal as codpostal, d.telefono as telefono, d.provincia as provincia, d.ciudad as poblacion FROM idl_proveedores i inner join proveedores p on i.codproveedor = p.codproveedor INNER JOIN dirproveedores d on p.codproveedor = d.codproveedor and d.direccionppal WHERE i.codproveedor NOT IN (SELECT clave FROM idl_erroneos WHERE tipo = '" + tipo + "') AND (i.idlog = 0 OR i.idlog IS NULL) ORDER BY i.fecha limit 10")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for r in rows:
                if proveedores and len(proveedores) > 0:
                        proveedores += ","
                proveedores += "'" + r["codproveedor"] + "'"

                creaXmlProveedor(r, int12)
        else:
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        tree = ET.ElementTree(supReq)
        tree.write("./proveedores/xmlProveedores.xml")

        xmlstring = tostring(supReq, 'utf-8', method="xml")

        #datosCX = dameDatosConexion("WSIDL_PROV_TEST", cx)
        datosCX = dameDatosConexion("WSIDL_PROV", cx)
        header = datosCX["header"]
        url = datosCX["url"]
        result = post_request(url, header, xmlstring)
        # result = False
        if not result:
            res[0] = False
            res[1] = result
            print(result)
            print("Error enviando proveedor")
            proveedores = ""
        else:
            res[0] = True
            res[1] = result

            root = ET.fromstring(result)
            tree = ET.ElementTree(root)
            tree.write("./proveedores/resProveedores.xml")

            for child in root.findall('int12/rub110'):
                codProv = child.find("supplier_code").text
                status = child.find("status").text
                if codProv and codProv != "":
                    if status == "OK":
                        cx["cur"].execute("UPDATE idl_proveedores SET ok = true where codproveedor = '" + codProv + "'")
                    else:
                        error = child.find("error_descriptions/error_description").text
                        cx["cur"].execute("UPDATE idl_proveedores SET error = '" + error + "' where codproveedor = '" + codProv + "'")
                        registraError(tipo, codProv, "Error enviando proveedor", cx)
            cx["conn"].commit()

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        registraError(tipo, "Exception", e, cx)
        proveedores = ""
        # TODO llamada API diagnosis

    xmlstring = xmlstring.decode("ISO-8859-15")
    idlog = registraLog("PROVEEDORES", xmlstring, res, cx)

    if proveedores and idlog:
        cx["cur"].execute("UPDATE idl_proveedores SET idlog = " + str(idlog) + " where codproveedor in (" + proveedores + ")")
    cx["conn"].commit()

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    cx["conn"].commit()
    cierraConexion(cx)
    generaXmlProveedores()
    return True


def creaXmlProveedor(r, int12):
    rub110 = ET.SubElement(int12, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "supplier_code").text = r["codproveedor"][0:13]
    ET.SubElement(rub110, "supplier_designation").text = formateaCadena(r["nombre"][0:30])
    ET.SubElement(rub110, "supplier_short_designation").text = formateaCadena(r["nombre"][0:15])
    ET.SubElement(rub110, "supplier_keyword").text = "P" + r["codproveedor"][0:15]
    ET.SubElement(rub110, "rhd_holder_code").text = "1"

    rub111 = ET.SubElement(rub110, "rub111")
    ET.SubElement(rub111, "activity_code").text = "GNS"
    ET.SubElement(rub111, "supplier_code").text = r["codproveedor"][0:13]
    ET.SubElement(rub111, "address_code").text = ""
    ET.SubElement(rub111, "company_name").text = formateaCadena(r["nombre"][0:30])

    direccion = quitaIntros(obtenerDireccion(r))
    direccion1 = str(truncarDireccion(str(direccion).split(" "), 0, 30))
    direccion2 = str(truncarDireccion(str(direccion).split(" "), len(direccion1.split(" "))-1, 30))

    if direccion1 and len(direccion1) > 0:
        ET.SubElement(rub111, "address_1").text = direccion1
    else:
        ET.SubElement(rub111, "address_1").text = "N/A"

    if direccion2 and len(direccion2) > 0:
        ET.SubElement(rub111, "address_2").text = direccion2

    if r["provincia"] and len(r["provincia"]) > 0:
        ET.SubElement(rub111, "address_3").text = formateaCadena(r["provincia"])[0:30]
    else:
        ET.SubElement(rub111, "address_3").text = "N/A"

    if r["poblacion"] and len(r["poblacion"]) > 0:
        ET.SubElement(rub111, "address_4").text = formateaCadena(r["poblacion"])[0:30]
    else:
        ET.SubElement(rub111, "address_4").text = "N/A"

    if r["telefono"] and len(r["telefono"]) > 0:
        ET.SubElement(rub111, "telephone").text = r["telefono"][0:15]
    else:
        ET.SubElement(rub111, "telephone").text = "N/A"

    if r["codpostal"] and len(r["codpostal"]) > 0:
        ET.SubElement(rub111, "other_number").text = r["codpostal"][0:10]
    else:
        ET.SubElement(rub111, "other_number").text = "N/A"


def obtenerDireccion(datos):
    tipovia = datos["tipovia"]
    direccion = datos["direccion"]
    numero = datos["numero"]
    otros = datos["otros"]

    dir1 = ""

    if tipovia and len(tipovia) > 0:
        dir1 = tipovia

    if direccion and len(direccion) > 0:
        if dir1 and len(dir1) > 0:
            dir1 += " "
        dir1 += direccion

    if numero and len(numero) > 0:
        if dir1 and len(dir1) > 0:
            dir1 += " "
        dir1 += numero

    if otros and len(otros) > 0:
        if dir1 and len(dir1) > 0:
            dir1 += " "
        dir1 += otros

    res = {}

    return dir1

    if dir1 and len(dir1) > 30:
        res[0] = dir1[:30]
        res[1] = dir1[30:60]
    else:
        res[0] = dir1
        res[1] = ""

    return res
