import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime
import math


def generarXmlImagenes():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "AZ_IMAGENES";

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()
    refArticulo = ""
    try:
        cx["cur"].execute("SELECT referencia, urls FROM az_imagenespublicadas WHERE sincronizado = false")

        rows = cx["cur"].fetchall()
        envelope = ET.Element("AmazonEnvelope")
        header = ET.SubElement(envelope, "Header") 
        ET.SubElement(header, "DocumentVersion").text = "1.0"
        ET.SubElement(header, "MerchantIdentifier").text = "ELQWN"
        ET.SubElement(envelope, "MessageType").text = "ProductImage"

        referencia = ""
        referencias = ""
        aUrls = []
        messageId = 1;
        if len(rows) > 0:
            for r in rows:
                referencia = str(r["referencia"])
                aUrls = r["urls"].split(",")
                for url in aUrls:
                    message = ET.SubElement(envelope, "Message")
                    ET.SubElement(message, "MessageID").text = str(messageId)
                    ET.SubElement(message, "OperationType").text = "Update"
                    productImage = ET.SubElement(message, "ProductImage")
                    ET.SubElement(productImage, "SKU").text = referencia
                    ET.SubElement(productImage, "ImageType").text = "Main"
                    ET.SubElement(productImage, "ImageLocation").text = url
                    messageId = messageId + 1
                if referencias != "":
                    referencias += ","
                referencias += "'" + referencia + "'"

        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        tree = ET.ElementTree(envelope)
        nombreFichero = dameNombreFichero()
        tree.write("./imagenes/" + nombreFichero)
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

        # res[0] = True
        # res[1] = result
        print("ejecutando update")
        cx["cur"].execute("UPDATE az_imagenespublicadas  SET sincronizado = true, fechasincro = CURRENT_DATE, horasincro = CURRENT_TIME WHERE referencia IN (" + referencias + ")")
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

def dameNombreFichero():
    hoy = datetime.datetime.now().strftime("%d%m%Y%H%M")
    print(hoy)
    nombreFichero = "az_images" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero