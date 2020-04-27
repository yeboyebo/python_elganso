from util import *


def generarEnvioMagento():

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_ENVIO_MAGENTO'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_ENVIO_MAGENTO','IDL_ENVIO_MAGENTO',CURRENT_DATE)")
    cx["conn"].commit()

    try:

        datosCX = dameDatosConexion("WSIDL_ENVIOMAGENTO", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {"Authorization": auth}
        print("URL: ", url)
        #cx["cur"].execute("SELECT codcomanda as incrementid, numseguimiento as trackingid, confirmacionenvio as confirmacionenvio FROM idl_ecommerce WHERE (numseguimientoinformado IS NULL OR numseguimientoinformado = FALSE) AND (confirmacionenvio IN ('Si','Parcial') OR idtpv_comanda IN (select idtpv_comanda FROM idl_ecommercefaltante WHERE enviada = false)) AND (tipo = 'VENTA' OR tipo = 'CAMBIO') AND (eseciweb = false OR eseciweb IS NULL) AND (codcomanda LIKE 'WEB%' OR codcomanda LIKE 'WDV%')")
        cx["cur"].execute("SELECT id as idseguimiento, coddocumento as incrementid, numseguimiento as trackingid, items as items FROM eg_seguimientoenvios WHERE (numseguimientoinformado IS NULL OR numseguimientoinformado = FALSE) AND (tipo = 'ECOMMERCE' OR tipo = 'VIAJE') AND (coddocumento LIKE 'WEB%' OR coddocumento LIKE 'WDV%') AND numseguimiento IS NOT NULL AND numseguimiento <> '' AND numseguimiento <> 'ERROR'")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                numSeguimiento = str(p["trackingid"])
                if numSeguimiento == "None":
                    numSeguimiento = ""

                estado = "en_camino"

                dataJson = '[{"increment_id": "' + str(p["incrementid"])[3:len(str(p["incrementid"]))] + '", "status": "' + estado + '", "trackingId": "' + numSeguimiento + '", "items": [' + str(p["items"]) + ']}]'
                print(dataJson)
                result = post_request(url, header, dataJson)
                result = True
                if not result:
                    return False

                cx["cur"].execute("UPDATE idl_ecommerce SET numseguimientoinformado = true, fechamagento = CURRENT_DATE, horamagento = CURRENT_TIME WHERE codcomanda = '" + str(p["incrementid"]) + "'")
                cx["conn"].commit()
                cx["cur"].execute("UPDATE eg_seguimientoenvios SET numseguimientoinformado = true, fechamagento = CURRENT_DATE, horamagento = CURRENT_TIME WHERE id = " + str(p["idseguimiento"]))
                cx["conn"].commit()

        else:
            print("No hay datos que enviar a Magento")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ENVIO_MAGENTO'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_ENVIO_MAGENTO'")
    cx["conn"].commit()
    cierraConexion(cx)
    generarEnvioMagento()

    return True
