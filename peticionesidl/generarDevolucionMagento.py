from util import *


def generarDevolucionMagento():

    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'IDL_DEVOL_MAGENTO'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'IDL_DEVOL_MAGENTO','IDL_DEVOL_MAGENTO',CURRENT_DATE)")
    cx["conn"].commit()

    try:

        datosCX = dameDatosConexion("WSIDL_DEVOLMAGENTO", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {"Authorization": auth}

        cx["cur"].execute("SELECT d.codcomanda as rmaid, v.tipo AS tipo FROM idl_ecommercedevoluciones d LEFT OUTER JOIN idl_ecommerce v ON d.codcomanda = v.codcomanda WHERE d.confirmacionrecepcion = 'Si' AND (d.informadomagento IS NULL OR d.informadomagento = false) AND (v.codcomanda IS NULL OR (v.codcomanda IS NOT NULL AND v.confirmacionenvio = 'Si')) AND d.codcomanda like 'WDV%' AND (d.eseciweb = false OR d.eseciweb IS NULL) ORDER BY d.codcomanda")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            for p in rows:
                rmaId = str(int(str(p["rmaid"])[3:len(str(p["rmaid"]))]))

                estado = "received"

                if str(p["tipo"]) == "CAMBIO":
                    estado = "complete"

                dataJson = '[{"rma_id": "' + rmaId + '", "status": "' + estado + '", "trackingId": ""}]'

                result = post_request(url, header, dataJson)

                if not result:
                    return False

                cx["cur"].execute("UPDATE idl_ecommercedevoluciones SET informadomagento = true, fechamagento = CURRENT_DATE, horamagento = CURRENT_TIME WHERE codcomanda = '" + str(p["rmaid"]) + "'")
                cx["conn"].commit()

        else:
            print("No hay datos que enviar a Magento")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEVOL_MAGENTO'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'IDL_DEVOL_MAGENTO'")
    cx["conn"].commit()
    cierraConexion(cx)
    generarDevolucionMagento()

    return True
