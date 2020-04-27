from zeep import Client
import base64
import xml.etree.cElementTree as ET
from xml.etree.ElementTree import tostring
from util import *


def envioArticulosAmazon():
    print("envioArticulosAmazon")
    cx = creaConexion()
    print("conectado")
    # cx["cur"].execute("SELECT nombre FROM eg_fichprocesados WHERE tipo = 'AZ_ENVIOARTICULOS'")
    # rows = cx["cur"].fetchall()
    # if len(rows) > 0:
    #     return True

    print("sigue")
    # cx["cur"].execute("INSERT INTO eg_fichprocesados (estado,hora,tipo,nombre,fecha) VALUES ('En proceso',CURRENT_TIME,'AZ_ENVIOARTICULOS','AZ_ENVIOARTICULOS',CURRENT_DATE)")
    # cx["conn"].commit()

    try:
        cx["cur"].execute("SELECT az.barcode AS barcode FROM az_articulospublicados az WHERE az.sincronizado = false and az.activo = true limit 1")

        rows = cx["cur"].fetchall()
        print(len(rows))
        barcode = False
        wsdl = 'https://clientes.mrw.es:4433/TrackingService.svc?wsdl'
        client = Client(wsdl)

        if len(rows) > 0:
            for q in rows:
                barcode = str(q["barcode"])
                print(barcode)
                requestData = {
                    'Product': {
                        'SKU': barcode,
                        'StandardProductID': barcode,
                    }
                }

                response = client.service.GetEnvios(**requestData)
                print(response)

                # if len(response.Seguimiento.Abonado) > 0:
                #     if len(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento) > 0:
                #         estadoEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].Estado)
                #         descEstadoentregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].EstadoDescripcion)
                #         fechaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].FechaEntrega)
                #         horaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].HoraEntrega)
                #         intentosentregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].Intentos)
                #         personaEntregaMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].PersonaEntrega)
                #         numAlbaranMrw = str(response.Seguimiento.Abonado[0].SeguimientoAbonado.Seguimiento[0].NumAlbaran)

                #         if fechaEntregaMrw == "None":
                #             cx["cur"].execute("UPDATE idl_ecommerce SET entregadomrw = false, estadoentregamrw = '" + estadoEntregaMrw + "', descestadoentregamrw = '" + descEstadoentregaMrw + "', intentosentregamrw = '" + intentosentregaMrw + "', personaentregamrw = '" + personaEntregaMrw + "', numalbaranmrw = '" + numAlbaranMrw + "' WHERE id = " + str(idEcommerce))
                #         else:
                #             entregadoMrw = "FALSE"
                #             fechaEntregaMrw = fechaEntregaMrw[4:8] + "-" + fechaEntregaMrw[2:4] + "-" + fechaEntregaMrw[0:2]
                #             horaEntregaMrw = horaEntregaMrw[0:2] + ":" + horaEntregaMrw[2:4]
                #             if estadoEntregaMrw == "00":
                #                 entregadoMrw = "TRUE"

                #             cx["cur"].execute("UPDATE idl_ecommerce SET entregadomrw = " + entregadoMrw + ", estadoentregamrw = '" + estadoEntregaMrw + "', descestadoentregamrw = '" + descEstadoentregaMrw + "', fechaentregamrw = '" + fechaEntregaMrw + "', horaentregamrw = '" + horaEntregaMrw + "', intentosentregamrw = '" + intentosentregaMrw + "', personaentregamrw = '" + personaEntregaMrw + "', numalbaranmrw = '" + numAlbaranMrw + "' WHERE id = " + str(idEcommerce))

                #         cx["conn"].commit()
                #     else:
                #         cx["cur"].execute("UPDATE idl_ecommerce SET descestadoentregamrw = 'No hay seguimiento' WHERE id = " + str(idEcommerce))
                #         cx["conn"].commit()
                # else:
                #     cx["cur"].execute("UPDATE idl_ecommerce SET descestadoentregamrw = 'No encontrado' WHERE id = " + str(idEcommerce))
                #     cx["conn"].commit()

    except Exception as e:
        print(e)

    cx["cur"].execute("DELETE FROM eg_fichprocesados WHERE tipo = 'MRW_ESTADOS'")
    cx["conn"].commit()
    cierraConexion(cx)

    return True
