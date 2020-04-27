import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime
import math


def generarXmlBarcodes():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "AZ_BARCODES";

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()
    refArticulo = ""
    try:
        cx["cur"].execute("SELECT referencia, barcode FROM az_barcodespublicados WHERE sincronizado = false")

        rows = cx["cur"].fetchall()
        envelope = ET.Element("AmazonEnvelope")
        header = ET.SubElement(envelope, "Header") 
        ET.SubElement(header, "DocumentVersion").text = "1.0"
        ET.SubElement(header, "MerchantIdentifier").text = "ELQWN"
        ET.SubElement(envelope, "MessageType").text = "Relationship"

        referencia = ""
        messageId = 1;
        barcodes = "";
        if len(rows) > 0:
            for r in rows:
                referencia = str(r["referencia"])
                message = ET.SubElement(envelope, "Message")
                ET.SubElement(message, "MessageID").text = str(messageId)
                ET.SubElement(message, "OperationType").text = "Update"
                relationship = ET.SubElement(message, "Relationship")
                ET.SubElement(relationship, "ParentSKU").text = referencia
                relation = ET.SubElement(relationship, "Relation")
                ET.SubElement(relation, "SKU").text = str(r["barcode"])
                ET.SubElement(relation, "Type").text = "Variation"
                messageId = messageId + 1
                if barcodes != "":
                	barcodes += ","
                barcodes += "'" + str(r["barcode"]) + "'"

        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        print("barcodefs sincronizados")
        print(barcodes)
        tree = ET.ElementTree(envelope)
        nombreFichero = dameNombreFichero()
        tree.write("./barcodes/" + nombreFichero)
        #xmlstring = tostring(envelope, 'utf-8', method="xml").decode()
        #print(xmlstring)
        #datosCX = dameDatosConexion("WSAZ_IMAGES", cx)
        # datosCX = dameDatosConexion("WSAZ_IMAGES_TEST", cx)
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

        print("lllega")
        # res[0] = True
        # res[1] = result
        cx["cur"].execute("UPDATE az_barcodespublicados  SET sincronizado = true, fechasincro = CURRENT_DATE, horasincro = CURRENT_TIME WHERE barcode IN (" + barcodes + ")")
        cx["conn"].commit()
        print("commit ejecutado");
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

def dameNombreFichero():
    hoy = datetime.datetime.now().strftime("%d%m%Y%H%M")
    print(hoy)
    nombreFichero = "az_barcodes" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero