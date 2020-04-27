import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime
import math


def generarXmlStock():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "AZ_STOCK";

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()
    try:
        cx["cur"].execute("SELECT sw.idssw as idssw, s.barcode AS barcode, s.disponible - CAST(pp.valor as INTEGER) AS disponible FROM param_parametros pp, eg_sincrostockweb sw INNER JOIN stocks s ON sw.idstock = s.idstock INNER JOIN az_barcodespublicados azb on s.barcode = azb.barcode WHERE pp.nombre = 'RSTOCK_AMAZ' AND sw.sincronizadoamazon = false")

        rows = cx["cur"].fetchall()
        envelope = ET.Element("AmazonEnvelope")
        header = ET.SubElement(envelope, "Header") 
        ET.SubElement(header, "DocumentVersion").text = "1.0"
        ET.SubElement(header, "MerchantIdentifier").text = "ELQWN"
        ET.SubElement(envelope, "MessageType").text = "Inventory"
        print(len(rows))
        messageId = 1;
        ids = "";
        if len(rows) > 0:
            for r in rows:
                print("entra");
                message = ET.SubElement(envelope, "Message")
                ET.SubElement(message, "MessageID").text = str(messageId)
                ET.SubElement(message, "OperationType").text = "Update"
                inventory = ET.SubElement(message, "Inventory")
                ET.SubElement(inventory, "SKU").text = str(r["barcode"])
                ET.SubElement(inventory, "Quantity").text = str(r["disponible"])
                ET.SubElement(inventory, "FulfillmentLatency").text = "1"
                messageId = messageId + 1
                if ids != "":
                    ids += ","
                ids += str(r["idssw"])

        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        print("sigue")
        tree = ET.ElementTree(envelope)
        nombreFichero = dameNombreFichero()
        tree.write("./stock/" + nombreFichero)
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
        cx["cur"].execute("UPDATE eg_sincrostockweb  SET sincronizadoamazon = true WHERE idssw IN (" + ids + ")")
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
    nombreFichero = "az_stock" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero