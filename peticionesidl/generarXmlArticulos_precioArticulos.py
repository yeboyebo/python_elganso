import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime
import math


def generarXmlArticulos():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_ARTICULOS'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_ARTICULOS','IDL_ARTICULOS',CURRENT_DATE)")
    cx["conn"].commit()
    refArticulo = ""
    try:
        cx["cur"].execute("SELECT at.referencia || '-' || at.talla as referencia, a.descripcion as descripcion, a.formatollegada as formatollegada, a.mgcomposicion as composicion, at.barcode as barcode,a.peso as peso, a.alto as alto, a.largo as largo, a.ancho as ancho,a.volumen as volumen, pa.codzona as codzona, at.referencia as refarticulo from articulos a INNER JOIN idl_articulos idla ON (a.referencia = idla.referencia AND idla.referencia IN (SELECT referencia from idl_articulos where ok = false and idlog is null and referencia not like '(C)%' order by referencia limit 20)) INNER JOIN atributosarticulos at ON a.referencia = at.referencia INNER JOIN articulosprov ap ON a.referencia = ap.referencia INNER JOIN proveedores p ON a.codproveedor = p.codproveedor INNER JOIN dirproveedores dp ON p.codproveedor = dp.codproveedor INNER JOIN paises pa ON dp.codpais = pa.codpais WHERE idla.ok = false AND idla.idlog IS NULL GROUP BY at.referencia || '-' || at.talla,a.descripcion,a.formatollegada,a.mgcomposicion,at.barcode,a.peso,a.alto,a.largo,a.ancho,a.volumen,pa.codzona,at.referencia ORDER BY at.referencia,at.talla LIMIT 20")
        rows = cx["cur"].fetchall()
        articles = ET.Element("articles")
        int03 = ET.SubElement(articles, "int03")
        aArticles = ""
        if len(rows) > 0:
            for r in rows:
                print("referencia: ", r["refarticulo"])
                if aArticles == "":
                    aArticles = "'" + r["refarticulo"] + "'"
                else:
                    aArticles += ",'" + r["refarticulo"] + "'"

                if refArticulo != r["referencia"]:
                    crearXMLArticulo(r, articles, int03, cx)
                    refArticulo = r["referencia"]
        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ARTICULOS'")
            cx["conn"].commit()
            cx["cur"].execute("UPDATE idl_articulos SET ok = true WHERE referencia IN (SELECT referencia FROM idl_articulos WHERE ok = false AND idlog IS NULL AND referencia NOT LIKE '(C)%' ORDER BY referencia LIMIT 20)")
            cx["conn"].commit()
            return True

        int22 = ET.SubElement(articles, "int22")
        if len(rows) > 0:
            for r in rows:
                crearInt22Rub110(r, int22, articles, cx)

        int20 = ET.SubElement(articles, "int20")
        if len(rows) > 0:
            for r in rows:
                crearInt20Rub110(r, int20, articles, cx)

        tree = ET.ElementTree(articles)
        nombreFichero = dameNombreFichero()
        tree.write("./articulos/" + nombreFichero)
        xmlstring = tostring(articles, 'utf-8', method="xml").decode()
        # print(xmlstring)
        datosCX = dameDatosConexion("WSIDL_ART", cx)
        # datosCX = dameDatosConexion("WSIDL_ART_TEST", cx)
        header = datosCX["header"]
        url = datosCX["url"]
        # url = "http://ws-reflex-test.id-logistics.com.es:8040/idlservices/masterdata/articles"
        #url = "http://ws-reflex.id-logistics.com.es:8040/idlservices/masterdata/articles"
        result = post_request(url, header, xmlstring.encode("utf-8"))
        # result = False
        print("RESULT: ", result)
        if not result:
            res[0] = False
            res[1] = result
            print(result)
            print("Error enviando artículos")
        else:
            res[0] = True
            res[1] = result
            root = ET.fromstring(result)
            for child in root.findall('int03/rub110'):
                referencia = child.find("item_code").text
                print("REFERENCIA: ", referencia)
                status = child.find("status").text
                print("ESTADO: ", status)
                if referencia and referencia != "":
                    if status == "OK":
                        cx["cur"].execute("UPDATE idl_articulos SET ok = true where referencia IN (SELECT referencia FROM atributosarticulos WHERE barcode = '" + referencia + "')")
                    else:
                        error = child.find("error_descriptions/error_description").text
                        cx["cur"].execute("UPDATE idl_articulos SET error = '" + error + "' where referencia IN (SELECT referencia FROM atributosarticulos WHERE barcode = '" + referencia + "')")
            cx["conn"].commit()

    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        # TODO llamada API diagnosis

    idLog = registraLog("ARTICULOS", xmlstring, res, cx)
    if aArticles != "":
        cx["cur"].execute("UPDATE idl_articulos SET idlog = " + str(idLog) + " WHERE referencia IN (" + aArticles + ")")
        cx["conn"].commit()

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ARTICULOS'")
    cx["conn"].commit()
    cierraConexion(cx)
    generarXmlArticulos()
    return True


def crearXMLArticulo(r, articles, int03, cx):
    rub110 = crearCabeceraXMLArticulo(r, int03, cx)
    #cx["cur"].execute("select dn.coddun14 as coddun, dn.cantidad as cantidad, dn.pesoneto as pesoneto, dn.pesobruto as pesobruto, dn.altura as altura, dn.anchura as anchura, dn.profundidad as profundidad, dn.volumenlogistica as volumenlogistica, at.barcode from atributosarticulos at INNER JOIN articulosprov ap ON at.referencia = ap.referencia LEFT OUTER JOIN eg_dun14prov dn ON ap.codproveedor = dn.codproveedor WHERE at.referencia = '" + r["referencia"][0:11] + "' AND at.talla = '" + r["referencia"][12:len(r["referencia"])] + "' ORDER BY at.referencia,at.barcode,dn.coddun14")
    cx["cur"].execute("SELECT dn.coddun14 AS coddun, dn.cantidad AS cantidad, dn.pesoneto AS pesoneto, dn.pesobruto AS pesobruto, dn.altura AS altura, dn.anchura AS anchura, dn.profundidad AS profundidad, dn.volumenlogistica AS volumenlogistica, at.barcode FROM atributosarticulos at INNER JOIN articulosprov ap ON at.referencia = ap.referencia INNER JOIN eg_dun14prov dn ON ap.codproveedor = dn.codproveedor WHERE at.barcode = '" + str(r["barcode"]) + "' GROUP BY dn.coddun14, dn.cantidad, dn.pesoneto, dn.pesobruto, dn.altura, dn.anchura, dn.profundidad, dn.volumenlogistica, at.barcode ORDER BY at.referencia,at.barcode,dn.coddun14")

    filas = cx["cur"].fetchall()
    if len(filas) > 0:
            for t in filas:
                crearRub120Articulo(r, t, rub110)


def crearCabeceraXMLArticulo(r, int03, cx):
    referencia = str(r["referencia"])
    barCode = str(r["barcode"])
    descripcion = formateaCadena(r["descripcion"])
    formatollegada = str(r["formatollegada"])

    if formatollegada == "Calzado":
        formatollegada = "CAL"
    elif formatollegada == "Doblada":
        formatollegada = "DOB"
    elif formatollegada == "Colgada":
        formatollegada = "COL"
    else:
        formatollegada = "ACC"

    # composicion = r["composicion"]
    rub110 = ET.SubElement(int03, "rub110")

    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "item_code").text = barCode
    ET.SubElement(rub110, "designation").text = descripcion[0:29]
    ET.SubElement(rub110, "short_designation").text = descripcion[0:14]
    ET.SubElement(rub110, "keyword").text = barCode
    ET.SubElement(rub110, "expiry_group_code").text = "STD"

    rub112 = ET.SubElement(rub110, "rub112")
    ET.SubElement(rub112, "activity_code").text = "GNS"
    ET.SubElement(rub112, "item_code").text = barCode
    ET.SubElement(rub112, "item_description").text = descripcion[0:229]

    # rub116 = ET.SubElement(rub110, "rub116")
    # ET.SubElement(rub116, "activity_code").text = "GNS"
    # ET.SubElement(rub116, "item_code").text = referencia

    cx["cur"].execute("SELECT at.pvp AS preciodivisa, t.coddivisamercado AS coddivisa, dm.ordenidl AS ordenidl FROM idl_divisasmercado dm INNER JOIN tarifas t ON dm.coddivisa = t.coddivisamercado INNER JOIN articulostarifas at ON at.codtarifa = t.codtarifa INNER JOIN atributosarticulos aa ON aa.referencia = at.referencia WHERE aa.barcode = '" + barCode + "' AND t.coddivisamercado IS NOT NULL ORDER BY dm.ordenidl")

    rub119 = ET.SubElement(rub110, "rub119")
    ET.SubElement(rub119, "activity_code").text = "GNS"
    ET.SubElement(rub119, "item_code").text = barCode
    ET.SubElement(rub119, "comment_line_no").text = "1"
    ET.SubElement(rub119, "comment_group").text = "REF"
    ET.SubElement(rub119, "comment").text = referencia
    filas = cx["cur"].fetchall()
    if len(filas) > 0:
            for t in filas:
                rub119 = ET.SubElement(rub110, "rub119")
                ET.SubElement(rub119, "activity_code").text = "GNS"
                ET.SubElement(rub119, "item_code").text = barCode
                ET.SubElement(rub119, "comment_line_no").text = t["ordenidl"]
                ET.SubElement(rub119, "comment_group").text = t["coddivisa"]
                ET.SubElement(rub119, "comment").text = damePrecioDivisa(str(t["preciodivisa"]))

    rub120 = ET.SubElement(rub110, "rub120")
    ET.SubElement(rub120, "activity_code").text = "GNS"
    ET.SubElement(rub120, "item_code").text = barCode
    ET.SubElement(rub120, "logistical_variant_code").text = "10"
    ET.SubElement(rub120, "keyword").text = barCode
    ET.SubElement(rub120, "lv_type_code").text = "10"
    ET.SubElement(rub120, "flag_base_lv").text = "1"
    ET.SubElement(rub120, "flag_packaging_lv").text = "0"
    ET.SubElement(rub120, "sub_packaging_lv_code").text = " "
    ET.SubElement(rub120, "quantity_in_sub_packaging_lvs").text = " "

    peso = str(r["peso"])
    alto = str(r["alto"])
    ancho = str(r["ancho"])
    largo = str(r["largo"])
    volumen = str(r["alto"])

    if peso:
        ET.SubElement(rub120, "net_weight").text = peso

    if peso:
        ET.SubElement(rub120, "gross_weight").text = peso

    if alto:
        ET.SubElement(rub120, "height").text = alto

    if ancho:
        ET.SubElement(rub120, "width").text = ancho

    if largo:
        ET.SubElement(rub120, "depth").text = largo

    if volumen:
        ET.SubElement(rub120, "volume").text = volumen

    ET.SubElement(rub120, "hd_type_code").text = " "
    ET.SubElement(rub120, "location_size_code").text = " "
    # ET.SubElement(rub120, "standard_storage_number_of_packages").text = " "
    ET.SubElement(rub120, "storage_group_code").text = formatollegada
    ET.SubElement(rub120, "preparation_group_code").text = formatollegada
    ET.SubElement(rub120, "flag_management_lv").text = "0"

    # rub129 = ET.SubElement(rub120, "rub129")
    # ET.SubElement(rub129, "activity_code").text = "GNS"
    # ET.SubElement(rub129, "item_code").text = barCode

    rub120 = ET.SubElement(rub110, "rub120")
    ET.SubElement(rub120, "activity_code").text = "GNS"
    ET.SubElement(rub120, "item_code").text = barCode
    ET.SubElement(rub120, "logistical_variant_code").text = "11"
    ET.SubElement(rub120, "keyword").text = barCode
    ET.SubElement(rub120, "lv_type_code").text = "20"
    ET.SubElement(rub120, "flag_base_lv").text = "0"
    ET.SubElement(rub120, "flag_packaging_lv").text = "1"
    ET.SubElement(rub120, "sub_packaging_lv_code").text = "10"
    ET.SubElement(rub120, "quantity_in_sub_packaging_lvs").text = "1"

    """if peso:
        ET.SubElement(rub120, "net_weight").text = peso

    if peso:
        ET.SubElement(rub120, "gross_weight").text = peso

    if alto:
        ET.SubElement(rub120, "height").text = alto

    if ancho:
        ET.SubElement(rub120, "width").text = ancho

    if largo:
        ET.SubElement(rub120, "depth").text = largo

    if volumen:
        ET.SubElement(rub120, "volume").text = volumen"""

    ET.SubElement(rub120, "hd_type_code").text = "STD"
    ET.SubElement(rub120, "location_size_code").text = "STD"
    ET.SubElement(rub120, "standard_storage_number_of_packages").text = "1"
    ET.SubElement(rub120, "storage_group_code").text = formatollegada
    ET.SubElement(rub120, "preparation_group_code").text = formatollegada
    ET.SubElement(rub120, "flag_management_lv").text = "0"

    # rub129 = ET.SubElement(rub120, "rub129")
    # ET.SubElement(rub129, "activity_code").text = "GNS"
    # ET.SubElement(rub129, "item_code").text = barCode

    return rub110


def crearRub120Articulo(r, t, rub110):
    barCode = str(t["barcode"])
    formatollegada = str(r["formatollegada"])

    if formatollegada == "Calzado":
        formatollegada = "CAL"
    elif formatollegada == "Doblada":
        formatollegada = "DOB"
    elif formatollegada == "Colgada":
        formatollegada = "COL"
    else:
        formatollegada = "ACC"

    coddun = t["coddun"]
    rub120 = ET.SubElement(rub110, "rub120")

    """pesoneto = str(t["pesoneto"])
    pesobruto = str(t["pesobruto"])
    altura = str(t["altura"])
    anchura = str(t["anchura"])
    profundidad = str(t["profundidad"])
    volumenlogistica = str(t["volumenlogistica"])"""
    cantidad = str(t["cantidad"])

    ET.SubElement(rub120, "activity_code").text = "GNS"
    ET.SubElement(rub120, "item_code").text = barCode
    ET.SubElement(rub120, "logistical_variant_code").text = "2" + coddun
    ET.SubElement(rub120, "keyword").text = barCode
    ET.SubElement(rub120, "lv_type_code").text = "20"
    ET.SubElement(rub120, "flag_base_lv").text = "0"
    ET.SubElement(rub120, "flag_packaging_lv").text = "0"
    ET.SubElement(rub120, "sub_packaging_lv_code").text = "10"

    if cantidad != "None":
        ET.SubElement(rub120, "quantity_in_sub_packaging_lvs").text = cantidad

    """if pesoneto != "None":
        ET.SubElement(rub120, "net_weight").text = pesoneto

    if pesobruto != "None":
        ET.SubElement(rub120, "gross_weight").text = pesobruto

    if altura != "None":
        ET.SubElement(rub120, "height").text = altura

    if anchura != "None":
        ET.SubElement(rub120, "width").text = anchura

    if profundidad != "None":
        ET.SubElement(rub120, "depth").text = profundidad

    if volumenlogistica != "None":
        ET.SubElement(rub120, "volume").text = volumenlogistica"""

    ET.SubElement(rub120, "hd_type_code").text = "STD"
    ET.SubElement(rub120, "location_size_code").text = "STD"
    # ET.SubElement(rub120, "standard_storage_number_of_packages").text = " "
    ET.SubElement(rub120, "storage_group_code").text = formatollegada
    ET.SubElement(rub120, "preparation_group_code").text = formatollegada
    ET.SubElement(rub120, "flag_management_lv").text = "1"

    # rub129 = ET.SubElement(rub120, "rub129")
    # ET.SubElement(rub129, "activity_code").text = "GNS"
    # ET.SubElement(rub129, "item_code").text = barCode


def crearRub110Articuloint22(r, t, int22):
    barcode = r["barcode"]
    coddun = t["coddun"]
    rub110 = ET.SubElement(int22, "rub110")

    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "item_code").text = barcode
    ET.SubElement(rub110, "logistical_variant_code").text = "2" + coddun
    ET.SubElement(rub110, "logistical_variant_id_type_code").text = "EAN14"
    ean14 = coddun + barcode[0:12]
    ET.SubElement(rub110, "logistical_variant_id_code").text = ean14 + digitoControlEAN(coddun + barcode[0:12])


def crearInt22Rub110(r, int22, articles, cx):
    rub110 = ET.SubElement(int22, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "item_code").text = r["barcode"]
    ET.SubElement(rub110, "logistical_variant_code").text = "11"
    ET.SubElement(rub110, "logistical_variant_id_type_code").text = "EAN13"
    ET.SubElement(rub110, "logistical_variant_id_code").text = r["barcode"]

    cx["cur"].execute("SELECT dn.coddun14 AS coddun FROM atributosarticulos at INNER JOIN articulosprov ap ON at.referencia = ap.referencia LEFT OUTER JOIN eg_dun14prov dn ON ap.codproveedor = dn.codproveedor WHERE at.barcode = '" + str(r["barcode"]) + "' GROUP BY dn.coddun14 ORDER BY dn.coddun14")

    filas = cx["cur"].fetchall()
    if len(filas) > 0:
            for t in filas:
                if t["coddun"]:
                    crearRub110Articuloint22(r, t, int22)


def crearInt20Rub110(r, int20, articles, cx):
    rub110 = ET.SubElement(int20, "rub110")
    ET.SubElement(rub110, "activity_code").text = "GNS"
    ET.SubElement(rub110, "item_code").text = r["barcode"]
    ET.SubElement(rub110, "lv_code").text = "11"
    codZona = "EUROPA"
    if r["codzona"] == "08":
        codZona = "ASIA"
    ET.SubElement(rub110, "item_group_code").text = codZona


def dameNombreFichero():
    hoy = datetime.datetime.now().strftime("%d%m%Y%H%M")
    print(hoy)
    nombreFichero = "idl_articles" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero


def digitoControlEAN(valorSinDC):
    if not valorSinDC or valorSinDC == "":
        return False

    longValorSinDC = len(str(valorSinDC))
    if longValorSinDC != 13:
        return False

    pesos = [3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3]
    suma = 0
    for i in range(len(str(valorSinDC))):
        suma += int(valorSinDC[i]) * int(pesos[i])
    decenaSuperior = (math.floor(suma / 10) + 1) * 10
    valor = decenaSuperior - suma
    if valor == 10:
        valor = 0

    return str(valor)


def damePrecioDivisa(pvpDivisa):
    if not pvpDivisa or pvpDivisa == "":
        return False

    oDivisa = pvpDivisa.split(".")
    parteEntera = str(oDivisa[0])
    parteDecimal = str(oDivisa[1])

    while len(parteEntera) < 6:
        parteEntera = "0" + parteEntera

    while len(parteDecimal) < 2:
        parteDecimal = parteDecimal + "0"

    return str(parteEntera + "." + parteDecimal)
