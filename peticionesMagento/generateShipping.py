from util import *


def generateShipping():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'MGGENERATESHIPPING'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'MGGENERATESHIPPING','MGGENERATESHIPPING',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        datosCX = dameDatosConexion("MGGENERATESHIPPING", cx)
        url = datosCX["url"]
        auth = datosCX["auth"]
        header = {'Authorization': auth, 'Content-Type': "application/json", 'Accept': "application/json"}
        cx["cur"].execute("SELECT s.coddocumento AS coddocumento FROM eg_seguimientoenvios s WHERE s.codalmacen = 'LFWB' AND (s.numseguimiento IS NULL OR s.numseguimiento = '')")

        rows = cx["cur"].fetchall()
        if len(rows) > 0:
            trackingNumber = ""
            for p in rows:
                dataJson = '{"increment_id": "' + str(p["coddocumento"])[3:len(str(p["coddocumento"]))] + '", "warehouse": "LFWB"}'
                print(dataJson)
                result = post_request(url, header, dataJson)

                print(result)

                if result:
                    resultado = json.loads(result)
                    if resultado["result"] == True:
                        trackingNumber = resultado["tracknumber"]

                cx["cur"].execute("UPDATE eg_seguimientoenvios SET numseguimiento = '" + trackingNumber + "' WHERE codalmacen = 'LFWB' AND coddocumento = '" + str(p["coddocumento"]) + "'")
                cx["conn"].commit()

        else:
            print("No hay datos que enviar a Magento")
            cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MGGENERATESHIPPING'")
            cx["conn"].commit()
            return True

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MGGENERATESHIPPING'")
    cx["conn"].commit()
    cierraConexion(cx)
    generateShipping()

    return True
