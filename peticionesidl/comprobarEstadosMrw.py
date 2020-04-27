from zeep import Client
import base64
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def compruebaEstadoMrw():
    cx = creaConexion()

    cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'MRW_ESTADOS'")
    rows = cx["cur"].fetchall()
    if len(rows) > 0:
        return True

    cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'MRW_ESTADOS','MRW_ESTADOS',CURRENT_DATE)")
    cx["conn"].commit()

    try:
        #cx["cur"].execute("SELECT eco.id AS idecommerce, eco.numseguimiento AS numseguimiento FROM idl_ecommerce eco WHERE eco.transportista = 'MRW' AND eco.entregadomrw = FALSE AND (eco.descestadoentregamrw <> 'Entregado' OR eco.descestadoentregamrw IS NULL) AND eco.numseguimiento LIKE 'T%' AND eco.idpreparacion IS NOT NULL AND eco.confirmacionenvio = 'Si'")
        cx["cur"].execute("SELECT s.id AS idseguimiento, s.numseguimiento AS numseguimiento FROM eg_seguimientoenvios s WHERE s.transportista = 'MRW' AND (s.entregadomrw = FALSE OR s.entregadomrw IS NULL) AND (s.descestadoentregamrw <> 'Entregado' OR s.descestadoentregamrw IS NULL) AND s.numseguimiento IS NOT NULL AND s.numseguimientoinformado = TRUE")

        rows = cx["cur"].fetchall()
        idSeguimiento = False
        numSeguimiento = False
        wsdl = 'https://clientes.mrw.es:4433/TrackingService.svc?wsdl'
        client = Client(wsdl)

        if len(rows) > 0:
            for q in rows:
                idSeguimiento = str(q["idseguimiento"])
                numSeguimiento = str(q["numseguimiento"])
                print(numSeguimiento)
                requestData = {
                    'login':'02666ELGANSO',
                    'pass':'02666ELGANSO',
                    'codigoIdioma':'3080',
                    'tipoFiltro':'1',
                    'valorFiltroDesde':numSeguimiento,
                    'valorFiltroHasta':numSeguimiento,
                    'fechaDesde':'',
                    'fechaHasta':'',
                    'tipoInformacion':'0'
                }

                response = client.service.GetEnvios(**requestData)
                #print(response)

                if len(response.Seguimiento.Abonado) <= 0:
                	requestData = {
                    'login':'02666ELGANSO',
                    'pass':'02666ELGANSO',
                    'codigoIdioma':'3080',
                    'tipoFiltro':'0',
                    'valorFiltroDesde':numSeguimiento,
                    'valorFiltroHasta':numSeguimiento,
                    'fechaDesde':'',
                    'fechaHasta':'',
                    'tipoInformacion':'0'
                }

                response = client.service.GetEnvios(**requestData)

                if len(response.Seguimiento.Abonado) > 0:
                    if len(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento) > 0:
                        estadoEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].Estado)
                        descEstadoentregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].EstadoDescripcion)
                        fechaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].FechaEntrega)
                        horaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].HoraEntrega)
                        intentosentregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].Intentos)
                        personaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].PersonaEntrega)
                        numAlbaranMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].NumAlbaran)

                        if fechaEntregaMrw == "None":
                            cx["cur"].execute("UPDATE eg_seguimientoenvios SET entregadomrw = false, estadoentregamrw = '" + estadoEntregaMrw + "', descestadoentregamrw = '" + descEstadoentregaMrw + "', intentosentregamrw = '" + intentosentregaMrw + "', personaentregamrw = '" + personaEntregaMrw + "', numalbaranmrw = '" + numAlbaranMrw + "' WHERE id = " + str(idSeguimiento))
                        else:
                            entregadoMrw = "FALSE"
                            fechaEntregaMrw = fechaEntregaMrw[4:8] + "-" + fechaEntregaMrw[2:4] + "-" + fechaEntregaMrw[0:2]
                            horaEntregaMrw = horaEntregaMrw[0:2] + ":" + horaEntregaMrw[2:4]
                            if estadoEntregaMrw == "00":
                                entregadoMrw = "TRUE"

                            cx["cur"].execute("UPDATE eg_seguimientoenvios SET entregadomrw = " + entregadoMrw + ", estadoentregamrw = '" + estadoEntregaMrw + "', descestadoentregamrw = '" + descEstadoentregaMrw + "', fechaentregamrw = '" + fechaEntregaMrw + "', horaentregamrw = '" + horaEntregaMrw + "', intentosentregamrw = '" + intentosentregaMrw + "', personaentregamrw = '" + personaEntregaMrw + "', numalbaranmrw = '" + numAlbaranMrw + "' WHERE id = " + str(idSeguimiento))

                        cx["conn"].commit()
                    else:
                        cx["cur"].execute("UPDATE eg_seguimientoenvios SET descestadoentregamrw = 'No hay seguimiento' WHERE id = " + str(idSeguimiento))
                        cx["conn"].commit()
                else:
                    cx["cur"].execute("UPDATE eg_seguimientoenvios SET descestadoentregamrw = 'No encontrado' WHERE id = " + str(idSeguimiento))
                    cx["conn"].commit()

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MRW_ESTADOS'")
    cx["conn"].commit()
    cierraConexion(cx)

    return True
