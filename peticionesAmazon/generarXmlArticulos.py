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

    tipo = "AZ_ARTICULOS";

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()
    refArticulo = ""
    try:
        cx["cur"].execute("SELECT a.referencia AS referencia, az.fechaini as fechaini, a.descripcion as descripcion, a.mgdescripcion as mgdescripcion, a.egcomposicion AS composicion, a.egsignoslavado AS lavado, a.origenproduccion AS origenproduccion FROM az_articulospublicados az INNER JOIN articulos a ON az.referencia = a.referencia WHERE az.sincronizado = false and az.activo = true GROUP BY a.referencia, az.fechaini, a.descripcion, a.mgdescripcion, a.egcomposicion, a.egsignoslavado, a.origenproduccion")

        rows = cx["cur"].fetchall()
        envelope = ET.Element("AmazonEnvelope")
        header = ET.SubElement(envelope, "Header") 
        ET.SubElement(header, "MessageType").text = "Product"
        ET.SubElement(header, "PurgeAndReplace").text = "true"

        referencia = ""
        referencias = ""
        messageId = 1;
        if len(rows) > 0:
            for r in rows:
                referencia = str(r["referencia"])
                print(referencia)
                message = ET.SubElement(envelope, "Message")
                print(messageId)
                ET.SubElement(message, "MessageID").text = str(messageId)
                ET.SubElement(message, "OperationType").text = "Update"
                product = ET.SubElement(message, "Product")
                crearNodoProduct(r, product, cx)
                messageId = messageId + 1
                if referencias != "":
                    referencias += ","
                referencias += "'" + referencia + "'"
        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        print("fin messageId")
        print(messageId)
        tree = ET.ElementTree(envelope)
        nombreFichero = dameNombreFichero()
        tree.write("./articulos/" + nombreFichero)
        #xmlstring = tostring(envelope, 'utf-8', method="xml").decode()
        #print(xmlstring)
        #datosCX = dameDatosConexion("WSAZ_PRODUCTS", cx)
        # datosCX = dameDatosConexion("WSAZ_PRODUCTS_TEST", cx)
        #header = datosCX["header"]
        #url = datosCX["url"]

        #result = post_request(url, header, xmlstring.encode("utf-8"))
        #print(result)

        # if not result:
        #     res[0] = False
        #     res[1] = result
        #     print(result)
        #     print("Error enviando articulos")
        #     cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
        #     cx["conn"].commit()
        # else:

        # res[0] = True
        # res[1] = result
        print("ejecutando update")
        cx["cur"].execute("UPDATE az_articulospublicados  SET sincronizado = true, fechasincro = CURRENT_DATE, horasincro = CURRENT_TIME WHERE referencia IN (" + referencias + ")")
        cx["conn"].commit()
            
    except Exception as e:
        res[0] = False
        res[1] = e
        print(e)
        # registraError(tipo, "Exception", e, cx)

    # idLog = registraLog("ARTICULOS", xmlstring, res, cx)
    # if aArticles != "":
    #     cx["cur"].execute("UPDATE idl_articulos SET idlog = " + str(idLog) + " WHERE referencia IN (" + aArticles + ")")
    #     cx["conn"].commit()

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    cx["conn"].commit()
    cierraConexion(cx)
    # generarXmlArticulos()
    return True


def crearNodoProduct(r, product, cx):
    referencia = str(r["referencia"])
    ET.SubElement(product, "SKU").text = referencia
    ET.SubElement(product, "ProductTaxCode").text = "A_GEN_TAX"
    ET.SubElement(product, "LaunchDate").text = str(r["fechaini"])

    descriptionData = ET.SubElement(product, "DescriptionData") 
    ET.SubElement(descriptionData, "Title").text = str(r["descripcion"])
    ET.SubElement(descriptionData, "Brand").text = "El Ganso (ELGBO)"
    ET.SubElement(descriptionData, "Description").text = str(r["mgdescripcion"])

    if r["origenproduccion"] and str(r["origenproduccion"]) != "":
        ET.SubElement(descriptionData, "BulletPoint").text = "Hecho en " + str(r["origenproduccion"])
    ET.SubElement(descriptionData, "BulletPoint").text = str(r["composicion"])
    ET.SubElement(descriptionData, "BulletPoint").text = str(r["lavado"])
    ET.SubElement(descriptionData, "Manufacturer").text = "El Ganso (ELGBO)"

    ET.SubElement(descriptionData, "SearchTerms").text = ""
    
    ET.SubElement(descriptionData, "ItemType").text = "flat-sheets"
    ET.SubElement(descriptionData, "IsGiftWrapAvailable").text = "false"
    ET.SubElement(descriptionData, "IsGiftMessageAvailable").text = "false"
    ET.SubElement(descriptionData, "RecommendedBrowseNode").text = ""

    productData = ET.SubElement(product, "ProductData") 
    home = ET.SubElement(productData, "Home") 
    ET.SubElement(home, "Parentage").text = "variation-parent"
    variationData = ET.SubElement(home, "VariationData") 
    ET.SubElement(variationData, "VariationTheme").text = "Size-Color"
    ET.SubElement(home, "Material").text = ""
    ET.SubElement(home, "ThreadCount").text = ""


def dameNombreFichero():
    hoy = datetime.datetime.now().strftime("%d%m%Y%H%M")
    print(hoy)
    nombreFichero = "az_products" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero