import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *
import datetime
import math


def generarXmlPrecios():
    xmlstring = ""
    res = {}
    res[0] = False
    res[1] = ""

    tipo = "AZ_PRECIOS";

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'" + tipo + "','" + tipo + "',CURRENT_DATE)")
    cx["conn"].commit()
    try:

        cx["cur"].execute("SELECT e.coddivisa as coddivisa FROM param_parametros p INNER JOIN tpv_tiendas t ON p.valor = t.codtienda INNER JOIN empresa e ON t.idempresa = e.id WHERE p.nombre = 'TIENDA_AMAZON'")
        row = cx["cur"].fetchall()
        codDivisa = row[0]["coddivisa"]

        cx["cur"].execute("SELECT azp.referencia AS referencia,azp.pvp as pvp, azp.fechaini AS fechaini, azp.fechafin AS fechafin, azp.pvpoferta AS pvpoferta FROM az_preciospublicados azp WHERE azp.sincronizado = false")

        rows = cx["cur"].fetchall()
        envelope = ET.Element("AmazonEnvelope")
        header = ET.SubElement(envelope, "Header") 
        ET.SubElement(header, "DocumentVersion").text = "1.0"
        ET.SubElement(header, "MerchantIdentifier").text = "ELQWN"
        ET.SubElement(envelope, "MessageType").text = "Price"
        print(len(rows))
        messageId = 1;
        referencias = "";
        if len(rows) > 0:
            for r in rows:
                print("entra");
                message = ET.SubElement(envelope, "Message")
                ET.SubElement(message, "MessageID").text = str(messageId)
                price = ET.SubElement(message, "Price")

                ET.SubElement(price, "SKU").text = str(r["referencia"])
                ET.SubElement(price,"StandardPrice",currency = codDivisa).text = str(r["pvp"])

                if r["pvpoferta"] and r["pvpoferta"] != 0:
                    sale = ET.SubElement(price, "Sale")
                    ET.SubElement(sale, "StartDate").text = str(r["fechaini"])
                    ET.SubElement(sale, "EndDate").text = str(r["fechafin"])
                    ET.SubElement(sale, "SalePrice",currency = codDivisa).text = str(r["pvpoferta"])
                messageId = messageId + 1
                if referencias != "":
                    referencias += ","
                referencias += "'" + str(r["referencia"]) + "'"

        else:
            print("No hay datos para exportar.")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = '" + tipo + "'")
            cx["conn"].commit()
            return True

        print("sigue")
        tree = ET.ElementTree(envelope)
        nombreFichero = dameNombreFichero()
        tree.write("./precios/" + nombreFichero)
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
        cx["cur"].execute("UPDATE az_preciospublicados SET sincronizado = true WHERE referencia IN (" + referencias + ")")
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
    nombreFichero = "az_precios" + hoy + ".xml"
    print(nombreFichero)
    return nombreFichero