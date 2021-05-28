# requirements
# google-cloud-pubsub==1.7.0 
import json
import time
from datetime import datetime
from random import random
from google.auth import jwt
from google.cloud import pubsub_v1

# --- Base variables and auth path
CREDENTIALS_PATH = "credentials/xenon-chain-308319-c511b4245b0d.json"
PROJECT_ID = "xenon-chain-308319"
TOPIC_ID = "inbound-topic"
MAX_MESSAGES = 1

# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, credentials_path, project_id, topic_id):
        credentials = jwt.Credentials.from_service_account_info(
            json.load(open(credentials_path)),
            audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(
            self.topic_path, 
            data.encode("utf-8"),
            eventId="f920b391-b42d-11eb-aa2c-a7bf513d27f9",
            timestamp="1620939511671",
            datetime="2021-05-13 16:58:31",
            country="CL"
            )
        #result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


# --- Main publishing script
def main():
    i = 0
    publisher = PubSubPublisher(CREDENTIALS_PATH, PROJECT_ID, TOPIC_ID)
    while i < MAX_MESSAGES:
        data = {
           "storeId": 1737,
            "terminalNumber": 5541,
            "transactionDate": "20210513",
            "transactionCode": 1,
            "transactionStatus": 1,
            "sequenceNumber": 5930,
            "countryFlag": "CL",
            "buyDate": "20210513",
            "transactionHour": "00001657",
            "cashierNumber": "00799874",
            "billsDetails": [
                {
                    "siiTicketNumber": "000693101571",
                    "identityNumberDocument": "118724860"
                }
            ],
             "productsDetails": [{
                "shortSkuDescrip": "JEANS MODA FF4I",
                "salesAmount": [
                    {
                        "value": "0000000000019990",
                        "description": "saleAmount"
                    },
                    {
                        "value": "0001",
                        "description": "itemBaseCostWithoutTaxes"
                    },
                    {
                        "value": "00016798",
                        "description": "netAmount"
                    },
                    {
                        "value": "0000",
                        "description": "netAmountCreditNote"
                    }
                ],
                "quantity": 1,
                "transactionChangeType": "0001",
                "upcNumber": "0002998820303587",
                "productDescription": "JEANS MODA FF4I$",
                "posDescription": "JEANS MODA FF4I$",
                "productType": "0000",
                "salesTax": [
                    {
                        "value": "00003192",
                        "description": "productIVA1"
                    },
                    {
                        "value": "0000",
                        "description": "amountIVA1"
                    },
                    {
                        "value": "0001",
                        "description": "ivaPercent"
                    }
                ],
                "productSkuMark": "0000",
                "sku": "882030358",
                "internalId": "17016808",
                "classifications": [
                    {
                        "internalId": "386165",
                        "classificationId": "J05010102",
                        "type": "CommercialLink"
                    }
                ],
                "attributes": [
                    {
                        "id": "SRXCTEMPORADA",
                        "value": "I2021"
                    },
                    {
                        "value": "JEANS MODA SYBILFF4IJFS451 MED GRAV",
                        "id": "SRXCODDESCADI"
                    },
                    {
                        "id": "SRXSEGUNDOMSG",
                        "value": "Falso"
                    },
                    {
                        "value": "false",
                        "id": "IsMarketPlace"
                    },
                    {
                        "id": "MARCA_ATT",
                        "value": "SYBILLA"
                    },
                    {
                        "value": "SYBILLA",
                        "id": "MARCA_ISA"
                    },
                    {
                        "value": "JEANS MODA FF4IJFS451 MED GRAVILLADO 38/28/8",
                        "id": "DisplayName"
                    },
                    {
                        "id": "externalReferenceId",
                        "value": "17016808"
                    },
                    {
                        "value": "FF4IJFS451",
                        "id": "CMODELO"
                    },
                    {
                        "id": "MODELO_ATT",
                        "value": "FF4IJFS451"
                    },
                    {
                        "id": "SRXTIPPRD",
                        "value": "Producto",
                        "lovid": "P"
                    },
                    {
                        "value": "P",
                        "id": "SRXCTPPRD"
                    },
                    {
                        "value": "ACTIVO",
                        "id": "ActiveStatus"
                    },
                    {
                        "id": "STATUSCH",
                        "value": "0"
                    },
                    {
                        "value": "2020-10-08 00:00:00",
                        "id": "SRXDCREACION"
                    },
                    {
                        "value": "JVAREL",
                        "id": "SRXUSRCREA"
                    },
                    {
                        "value": "1",
                        "id": "SRXUNEMPAQUE"
                    },
                    {
                        "value": "1",
                        "id": "SRXUTRANSFERENCIA"
                    },
                    {
                        "value": "50306",
                        "id": "SRXRUTPROV"
                    },
                    {
                        "value": "50306",
                        "id": "BizagiRutProveedor"
                    },
                    {
                        "value": "1",
                        "id": "BizagiDV"
                    },
                    {
                        "id": "providerName",
                        "value": "GLOBAL WORLD TRADING LTD."
                    },
                    {
                        "value": "GLOBAL WORLD TRADING LTD.",
                        "id": "SellerName"
                    },
                    {
                        "value": "50306-1",
                        "id": "providerId"
                    },
                    {
                        "id": "providerCode",
                        "value": "50306"
                    },
                    {
                        "id": "SKU_PADRE_CL",
                        "value": "882030355"
                    },
                    {
                        "value": "Venta",
                        "id": "SRXCRECAUDA"
                    },
                    {
                        "value": "AUTOADHESIVO",
                        "id": "SRXCETIQUETA"
                    },
                    {
                        "id": "SRXMRCPROPIA",
                        "value": "Propia"
                    },
                    {
                        "value": "JEANS MODA FF4IJ",
                        "id": "SRXCODDESCPOS"
                    },
                    {
                        "value": "413",
                        "id": "SRXCODCMPCL"
                    },
                    {
                        "value": "882030355",
                        "id": "CBROKER"
                    },
                    {
                        "value": "Importado",
                        "id": "SRXIMPORTADO"
                    },
                    {
                        "value": "17016808",
                        "id": "BACKEND_ID_CL"
                    },
                    {
                        "value": "XS",
                        "id": "SRXOMSTAMANO"
                    },
                    {
                        "value": "G1",
                        "id": "SRXOMSAGRUPAMIENTO"
                    },
                    {
                        "value": "Mini",
                        "id": "SRXOMSGLOSATAMANO"
                    },
                    {
                        "id": "ReferenceField8",
                        "value": "XS"
                    },
                    {
                        "id": "ReferenceField10",
                        "value": "G1"
                    },
                    {
                        "value": "Mini",
                        "id": "CommerceAttribute2"
                    },
                    {
                        "value": "AZUL 401",
                        "id": "COLOR_NRF"
                    },
                    {
                        "value": "38/28/8",
                        "id": "SRXTALLA"
                    },
                    {
                        "value": "38/28/8",
                        "id": "TALLA_ATT"
                    },
                    {
                        "value": "1",
                        "id": "SRXUNCAPAS"
                    },
                    {
                        "value": "MTR",
                        "id": "SRXUNMED"
                    },
                    {
                        "value": "0,36",
                        "id": "SRXLARGO"
                    },
                    {
                        "id": "SRXPESOARTI",
                        "value": "0,47"
                    },
                    {
                        "id": "SRXALTURA",
                        "value": "0,58"
                    },
                    {
                        "id": "SRXANCHO",
                        "value": "0,02"
                    },
                    {
                        "id": "SRXUNPES",
                        "value": "KGM"
                    },
                    {
                        "id": "SRXUNCAPAS_Pallets",
                        "value": "1000"
                    },
                    {
                        "id": "SRXUNMED_Pallets",
                        "value": "MTR"
                    },
                    {
                        "id": "SRXLARGO_Pallets",
                        "value": "1,80"
                    },
                    {
                        "id": "SRXPESOARTI_Pallets",
                        "value": "148,00"
                    },
                    {
                        "value": "0,50",
                        "id": "SRXALTURA_Pallets"
                    },
                    {
                        "value": "0,60",
                        "id": "SRXANCHO_Pallets"
                    },
                    {
                        "value": "KGM",
                        "id": "SRXUNPES_Pallets"
                    },
                    {
                        "id": "SRXSKUCH",
                        "value": "882030358"
                    },
                    {
                        "id": "SRXORIGEN",
                        "value": "XPC"
                    },
                    {
                        "id": "PAIS_ATT",
                        "value": "CHINA"
                    },
                    {
                        "id": "SRXSUBCLASE",
                        "value": "J05010102",
                        "internalId": "386165"
                    },
                    {
                        "value": "J05010102 JEANS MODA",
                        "id": "SRXDESCSCLASE"
                    },
                    {
                        "internalId": "488",
                        "value": "J050101",
                        "id": "SRXCLASE"
                    },
                    {
                        "value": "J050101 JEANS DENIM",
                        "id": "SRXDESCCLASE"
                    },
                    {
                        "internalId": "487",
                        "value": "J0501",
                        "id": "SRXSUBLINEA"
                    },
                    {
                        "value": "J0501 MEZCLILLA ",
                        "id": "SRXDESCSLIN"
                    },
                    {
                        "id": "SRXLINEA",
                        "internalId": "486",
                        "value": "J05"
                    },
                    {
                        "id": "SRXDESCLIN",
                        "value": "J05 RINCON JUVENIL MUJER"
                    },
                    {
                        "value": "19990",
                        "id": "SRXPVENTACL"
                    },
                    {
                        "value": "IVA",
                        "id": "CTIP_IMPTO_CL"
                    },
                    {
                        "id": "SRXCOSTOCL",
                        "value": "5807,955"
                    },
                    {
                        "id": "DEFAULTUPCEAN_CL",
                        "value": "2998820303587"
                    },
                    {
                        "value": "2998820303587",
                        "id": "SRXUPCEAN"
                    },
                    {
                        "id": "publishedAtg",
                        "value": "false"
                    },
                    {
                        "id": "SoldOnline",
                        "value": "false"
                    },
                    {
                        "id": "lastUpdate",
                        "value": "2021-02-11 07:57:18"
                    },
                    {
                        "id": "publishedConnect",
                        "value": "false"
                    },
                    {
                        "value": "false",
                        "id": "rtvDevMasiva"
                    },
                    {
                        "value": "false",
                        "id": "rtvDevUnitaria"
                    },
                    {
                        "id": "lastReceiptDate",
                        "value": "2021-02-16 00:00:00"
                    },
                    {
                        "id": "ID_ESTILO",
                        "internalId": "17016805",
                        "value": "882030355"
                    },
                    {
                        "id": "DESC_ESTILO",
                        "value": "JEANS MODA FF4IJFS451"
                    },
                    {
                        "value": "MED GRAVILLADO",
                        "id": "COLOR_FANTASIA"
                    },
                    {
                        "id": "ID_SKU_ORIGINAL",
                        "value": "882030358"
                    },
                    {
                        "id": "SoldInStores",
                        "value": "true"
                    },
                    {
                        "value": "true",
                        "id": "availableForPickupInStore"
                    },
                    {
                        "id": "availableForShipToStore",
                        "value": "true"
                    },
                    {
                        "value": "Retail",
                        "id": "channelType"
                    },
                    {
                        "value": "5807,955",
                        "id": "unitCost"
                    },
                    {
                        "value": "JEANS MODA FF4IJFS451 MED GRAVILLADO 38/28/8SYBILLAFF4IJFS45138/28/8",
                        "id": "longDescription"
                    },
                    {
                        "value": "401 ",
                        "id": "itemColor"
                    },
                    {
                        "value": "882030355401373",
                        "id": "styleSecuence"
                    },
                    {
                        "value": "CreateOrUpdate",
                        "id": "markForDeletion"
                    },
                    {
                        "value": "CLP",
                        "id": "currencyCode"
                    },
                    {
                        "value": "MTR",
                        "id": "dimensionUOM"
                    },
                    {
                        "value": "MTR3",
                        "id": "volumeUOM"
                    },
                    {
                        "value": "F",
                        "id": "supplierItemBarcode"
                    },
                    {
                        "id": "statusCode",
                        "value": "0"
                    },
                    {
                        "id": "goodsQuantity",
                        "value": "1"
                    },
                    {
                        "id": "itemSeason",
                        "value": "OI"
                    },
                    {
                        "value": "TotalStandardCost",
                        "id": "costType"
                    },
                    {
                        "id": "uom",
                        "value": "unit"
                    },
                    {
                        "id": "scanQuantity",
                        "value": "1"
                    },
                    {
                        "value": "000",
                        "id": "SlotMiscellaneous1"
                    },
                    {
                        "value": "A",
                        "id": "SlotMiscellaneous2"
                    },
                    {
                        "value": "A",
                        "id": "VelocityCode"
                    }
                ],
                "image": [
                    {
                        "viewNumber": 1,
                        "url": "https://falabella.scene7.com/is/image/Falabella/882030358"
                    },
                    {
                        "url": "http://www.falabella.com/falabella-cl/product/",
                        "viewNumber": 2
                    }
                ]
            },
            {
                "salesAmount": [
                    {
                        "value": "0000000000029990",
                        "description": "saleAmount"
                    },
                    {
                        "value": "0002",
                        "description": "itemBaseCostWithoutTaxes"
                    },
                    {
                        "value": "00025202",
                        "description": "netAmount"
                    },
                    {
                        "value": "0000",
                        "description": "netAmountCreditNote"
                    }
                ],
                "quantity": 1,
                "transactionChangeType": "0001",
                "upcNumber": "0002998820047832",
                "productDescription": "BOTINES MUJ CAS$",
                "posDescription": "BOTINES MUJ CAS$",
                "productType": "0000",
                "salesTax": [
                    {
                        "value": "00004788",
                        "description": "productIVA1"
                    },
                    {
                        "value": "0000",
                        "description": "amountIVA1"
                    },
                    {
                        "value": "0001",
                        "description": "ivaPercent"
                    }
                ],
                "productSkuMark": "0000",
                "sku": "882004783",
                "internalId": "16893810",
                "classifications": [
                    {
                        "internalId": "7143378",
                        "type": "CommercialLink",
                        "classificationId": "J10020202"
                    }
                ],
                "attributes": [
                    {
                        "id": "SRXCTEMPORADA",
                        "value": "I2021"
                    },
                    {
                        "id": "SRXCODDESCADI",
                        "value": "BOTINES MU SYBILHAKANI NE BLACK"
                    },
                    {
                        "id": "SRXSEGUNDOMSG",
                        "value": "Falso"
                    },
                    {
                        "value": "false",
                        "id": "IsMarketPlace"
                    },
                    {
                        "id": "MARCA_ATT",
                        "value": "SYBILLA"
                    },
                    {
                        "id": "MARCA_ISA",
                        "value": "SYBILLA"
                    },
                    {
                        "id": "DisplayName",
                        "value": "BOTINES MUJ CAS HAKANI NE BLACK 39"
                    },
                    {
                        "value": "16893810",
                        "id": "externalReferenceId"
                    },
                    {
                        "value": "HAKANI NE",
                        "id": "CMODELO"
                    },
                    {
                        "value": "HAKANI NE",
                        "id": "MODELO_ATT"
                    },
                    {
                        "id": "SRXTIPPRD",
                        "value": "Producto",
                        "lovid": "P"
                    },
                    {
                        "value": "P",
                        "id": "SRXCTPPRD"
                    },
                    {
                        "value": "ACTIVO",
                        "id": "ActiveStatus"
                    },
                    {
                        "value": "0",
                        "id": "STATUSCH"
                    },
                    {
                        "id": "SRXDCREACION",
                        "value": "2020-09-04 00:00:00"
                    },
                    {
                        "id": "SRXUSRCREA",
                        "value": "CMEZASA"
                    },
                    {
                        "value": "1",
                        "id": "SRXUNEMPAQUE"
                    },
                    {
                        "value": "1",
                        "id": "SRXUTRANSFERENCIA"
                    },
                    {
                        "value": "65175",
                        "id": "SRXRUTPROV"
                    },
                    {
                        "value": "65175",
                        "id": "BizagiRutProveedor"
                    },
                    {
                        "id": "BizagiDV",
                        "value": "3"
                    },
                    {
                        "value": "WENZHOU BOBO SHOES INTERNATIONAL CO",
                        "id": "providerName"
                    },
                    {
                        "value": "WENZHOU BOBO SHOES INTERNATIONAL CO",
                        "id": "SellerName"
                    },
                    {
                        "value": "65175-3",
                        "id": "providerId"
                    },
                    {
                        "value": "820785",
                        "id": "providerCode"
                    },
                    {
                        "id": "SKU_PADRE_CL",
                        "value": "882004780"
                    },
                    {
                        "value": "Venta",
                        "id": "SRXCRECAUDA"
                    },
                    {
                        "id": "SRXCETIQUETA",
                        "value": "AUTOADHESIVO"
                    },
                    {
                        "value": "Propia",
                        "id": "SRXMRCPROPIA"
                    },
                    {
                        "value": "BOTINES MUJ CAS ",
                        "id": "SRXCODDESCPOS"
                    },
                    {
                        "value": "24",
                        "id": "SRXCODCMPCL"
                    },
                    {
                        "value": "882004780",
                        "id": "CBROKER"
                    },
                    {
                        "id": "SRXIMPORTADO",
                        "value": "Importado"
                    },
                    {
                        "value": "16893810",
                        "id": "BACKEND_ID_CL"
                    },
                    {
                        "id": "SRXOMSTAMANO",
                        "value": "XS"
                    },
                    {
                        "id": "SRXOMSAGRUPAMIENTO",
                        "value": "G1"
                    },
                    {
                        "value": "Mini",
                        "id": "SRXOMSGLOSATAMANO"
                    },
                    {
                        "id": "ReferenceField8",
                        "value": "XS"
                    },
                    {
                        "value": "G1",
                        "id": "ReferenceField10"
                    },
                    {
                        "id": "CommerceAttribute2",
                        "value": "Mini"
                    },
                    {
                        "id": "COLOR_NRF",
                        "value": "NEGRO 001"
                    },
                    {
                        "value": "39",
                        "id": "SRXTALLA"
                    },
                    {
                        "value": "39",
                        "id": "TALLA_ATT"
                    },
                    {
                        "id": "SRXUNCAPAS",
                        "value": "1"
                    },
                    {
                        "id": "SRXUNMED",
                        "value": "MTR"
                    },
                    {
                        "value": "0,32",
                        "id": "SRXLARGO"
                    },
                    {
                        "value": "0,50",
                        "id": "SRXPESOARTI"
                    },
                    {
                        "id": "SRXALTURA",
                        "value": "0,13"
                    },
                    {
                        "value": "0,23",
                        "id": "SRXANCHO"
                    },
                    {
                        "value": "KGM",
                        "id": "SRXUNPES"
                    },
                    {
                        "value": "1000",
                        "id": "SRXUNCAPAS_Pallets"
                    },
                    {
                        "id": "SRXUNMED_Pallets",
                        "value": "MTR"
                    },
                    {
                        "id": "SRXLARGO_Pallets",
                        "value": "0,40"
                    },
                    {
                        "value": "0,50",
                        "id": "SRXPESOARTI_Pallets"
                    },
                    {
                        "id": "SRXALTURA_Pallets",
                        "value": "0,20"
                    },
                    {
                        "id": "SRXANCHO_Pallets",
                        "value": "0,30"
                    },
                    {
                        "value": "KGM",
                        "id": "SRXUNPES_Pallets"
                    },
                    {
                        "value": "882004783",
                        "id": "SRXSKUCH"
                    },
                    {
                        "id": "SRXORIGEN",
                        "value": "XPC"
                    },
                    {
                        "id": "PAIS_ATT",
                        "value": "CHINA"
                    },
                    {
                        "value": "J10020202",
                        "id": "SRXSUBCLASE",
                        "internalId": "7143378"
                    },
                    {
                        "id": "SRXDESCSCLASE",
                        "value": "J10020202 BOTINES MUJ CASUAL"
                    },
                    {
                        "internalId": "1291",
                        "value": "J100202",
                        "id": "SRXCLASE"
                    },
                    {
                        "id": "SRXDESCCLASE",
                        "value": "J100202 BOTINES MUJER"
                    },
                    {
                        "value": "J1002",
                        "id": "SRXSUBLINEA",
                        "internalId": "1287"
                    },
                    {
                        "value": "J1002 ZAPATOS MUJER",
                        "id": "SRXDESCSLIN"
                    },
                    {
                        "value": "J10",
                        "id": "SRXLINEA",
                        "internalId": "1265"
                    },
                    {
                        "id": "SRXDESCLIN",
                        "value": "J10 CALZADO"
                    },
                    {
                        "id": "SRXPVENTACL",
                        "value": "34990"
                    },
                    {
                        "id": "CTIP_IMPTO_CL",
                        "value": "IVA"
                    },
                    {
                        "id": "SRXCOSTOCL",
                        "value": "7668"
                    },
                    {
                        "value": "2998820047832",
                        "id": "DEFAULTUPCEAN_CL"
                    },
                    {
                        "value": "2998820047832",
                        "id": "SRXUPCEAN"
                    },
                    {
                        "value": "true",
                        "id": "publishedAtg"
                    },
                    {
                        "id": "SoldOnline",
                        "value": "true"
                    },
                    {
                        "value": "",
                        "id": "lastUpdate"
                    },
                    {
                        "value": "false",
                        "id": "publishedConnect"
                    },
                    {
                        "id": "rtvDevMasiva",
                        "value": "false"
                    },
                    {
                        "value": "false",
                        "id": "rtvDevUnitaria"
                    },
                    {
                        "id": "ID_ESTILO",
                        "value": "882004780",
                        "internalId": "16893807"
                    },
                    {
                        "value": "BOTINES MUJ CAS HAKANI NE",
                        "id": "DESC_ESTILO"
                    },
                    {
                        "id": "COLOR_FANTASIA",
                        "value": "BLACK"
                    },
                    {
                        "id": "ID_SKU_ORIGINAL",
                        "value": "882004783"
                    },
                    {
                        "id": "SoldInStores",
                        "value": "true"
                    },
                    {
                        "value": "true",
                        "id": "availableForPickupInStore"
                    },
                    {
                        "id": "availableForShipToStore",
                        "value": "true"
                    },
                    {
                        "value": "Retail",
                        "id": "channelType"
                    },
                    {
                        "id": "unitCost",
                        "value": "7668"
                    },
                    {
                        "value": "BOTINES MUJ CAS HAKANI NE BLACK 39SYBILLAHAKANI NE39",
                        "id": "longDescription"
                    },
                    {
                        "value": "001 ",
                        "id": "itemColor"
                    },
                    {
                        "value": "882004780137",
                        "id": "styleSecuence"
                    },
                    {
                        "value": "CreateOrUpdate",
                        "id": "markForDeletion"
                    },
                    {
                        "value": "CLP",
                        "id": "currencyCode"
                    },
                    {
                        "id": "dimensionUOM",
                        "value": "MTR"
                    },
                    {
                        "value": "MTR3",
                        "id": "volumeUOM"
                    },
                    {
                        "value": "F",
                        "id": "supplierItemBarcode"
                    },
                    {
                        "value": "0",
                        "id": "statusCode"
                    },
                    {
                        "value": "1",
                        "id": "goodsQuantity"
                    },
                    {
                        "value": "OI",
                        "id": "itemSeason"
                    },
                    {
                        "value": "TotalStandardCost",
                        "id": "costType"
                    },
                    {
                        "value": "unit",
                        "id": "uom"
                    },
                    {
                        "value": "1",
                        "id": "scanQuantity"
                    },
                    {
                        "id": "SlotMiscellaneous1",
                        "value": "000"
                    },
                    {
                        "id": "SlotMiscellaneous2",
                        "value": "A"
                    },
                    {
                        "value": "A",
                        "id": "VelocityCode"
                    }
                ],
                "image": [
                    {
                        "url": "https://falabella.scene7.com/is/image/Falabella/882004783",
                        "viewNumber": 1
                    },
                    {
                        "viewNumber": 2,
                        "url": "http://www.falabella.com/falabella-cl/product/"
                    }
                ]
            },
            {
                "salesAmount": [
                    {
                        "value": "0000000000059990",
                        "description": "saleAmount"
                    },
                    {
                        "value": "0003",
                        "description": "itemBaseCostWithoutTaxes"
                    },
                    {
                        "value": "00050412",
                        "description": "netAmount"
                    },
                    {
                        "value": "0000",
                        "description": "netAmountCreditNote"
                    }
                ],
                "quantity": 1,
                "transactionChangeType": "0001",
                "upcNumber": "0002014595372006",
                "productDescription": "BOTIN PJ WD002 $",
                "posDescription": "BOTIN PJ WD002 $",
                "productType": "0000",
                "salesTax": [
                    {
                        "value": "00009578",
                        "description": "productIVA1"
                    },
                    {
                        "value": "0000",
                        "description": "amountIVA1"
                    },
                    {
                        "value": "0001",
                        "description": "ivaPercent"
                    }
                ],
                "productSkuMark": "0000",
                "sku": "14595372",
                "internalId": "16992200",
                "classifications": [
                    {
                        "classificationId": "J10020201",
                        "type": "CommercialLink",
                        "internalId": "1292"
                    }
                ],
                "attributes": [
                    {
                        "value": "I2021",
                        "id": "SRXCTEMPORADA"
                    },
                    {
                        "id": "SRXCODDESCADI",
                        "value": "BOTIN PJ WD002 CHOCOLATE 37"
                    },
                    {
                        "value": "Falso",
                        "id": "SRXSEGUNDOMSG"
                    },
                    {
                        "value": "false",
                        "id": "IsMarketPlace"
                    },
                    {
                        "value": "PANAMA JACK",
                        "id": "MARCA_ATT"
                    },
                    {
                        "id": "MARCA_ISA",
                        "value": "PANAMAJACK"
                    },
                    {
                        "value": " BOTIN PJ WD002 CHOCOLATE 37",
                        "id": "DisplayName"
                    },
                    {
                        "value": "16992200",
                        "id": "externalReferenceId"
                    },
                    {
                        "value": "I21_WD002CA",
                        "id": "CMODELO"
                    },
                    {
                        "value": "I21_WD002CA",
                        "id": "MODELO_ATT"
                    },
                    {
                        "value": "Producto",
                        "id": "SRXTIPPRD",
                        "lovid": "P"
                    },
                    {
                        "value": "P",
                        "id": "SRXCTPPRD"
                    },
                    {
                        "value": "ACTIVO",
                        "id": "ActiveStatus"
                    },
                    {
                        "value": "0",
                        "id": "STATUSCH"
                    },
                    {
                        "id": "SRXDCREACION",
                        "value": "2020-10-01 00:00:00"
                    },
                    {
                        "value": "FGORICHON",
                        "id": "SRXUSRCREA"
                    },
                    {
                        "value": "1",
                        "id": "SRXUNEMPAQUE"
                    },
                    {
                        "id": "SRXUTRANSFERENCIA",
                        "value": "1"
                    },
                    {
                        "id": "SRXRUTPROV",
                        "value": "78919170"
                    },
                    {
                        "id": "BizagiRutProveedor",
                        "value": "78919170"
                    },
                    {
                        "id": "BizagiDV",
                        "value": "0"
                    },
                    {
                        "value": "PE Y PE S.A.",
                        "id": "providerName"
                    },
                    {
                        "value": "PE Y PE S.A.",
                        "id": "SellerName"
                    },
                    {
                        "value": "78919170-0",
                        "id": "providerId"
                    },
                    {
                        "id": "providerCode",
                        "value": "28023"
                    },
                    {
                        "id": "SKU_PADRE_CL",
                        "value": "14595369"
                    },
                    {
                        "value": "Venta",
                        "id": "SRXCRECAUDA"
                    },
                    {
                        "id": "SRXCETIQUETA",
                        "value": "AUTOADHESIVO"
                    },
                    {
                        "id": "SRXMRCPROPIA",
                        "value": "Otras Marcas"
                    },
                    {
                        "value": "BOTIN PJ WD002",
                        "id": "SRXCODDESCPOS"
                    },
                    {
                        "value": "49",
                        "id": "SRXCODCMPCL"
                    },
                    {
                        "value": "I21_WD002CA",
                        "id": "CBROKER"
                    },
                    {
                        "id": "SRXIMPORTADO",
                        "value": "Nacional"
                    },
                    {
                        "value": "16992200",
                        "id": "BACKEND_ID_CL"
                    },
                    {
                        "id": "SRXOMSTAMANO",
                        "value": "XS"
                    },
                    {
                        "value": "G1",
                        "id": "SRXOMSAGRUPAMIENTO"
                    },
                    {
                        "id": "SRXOMSGLOSATAMANO",
                        "value": "Mini"
                    },
                    {
                        "value": "XS",
                        "id": "ReferenceField8"
                    },
                    {
                        "id": "ReferenceField10",
                        "value": "G1"
                    },
                    {
                        "id": "CommerceAttribute2",
                        "value": "Mini"
                    },
                    {
                        "value": "CAFE 200",
                        "id": "COLOR_NRF"
                    },
                    {
                        "value": "37",
                        "id": "SRXTALLA"
                    },
                    {
                        "value": "37",
                        "id": "TALLA_ATT"
                    },
                    {
                        "value": "1",
                        "id": "SRXUNCAPAS"
                    },
                    {
                        "value": "MTR",
                        "id": "SRXUNMED"
                    },
                    {
                        "value": "0,32",
                        "id": "SRXLARGO"
                    },
                    {
                        "id": "SRXPESOARTI",
                        "value": "0,53"
                    },
                    {
                        "value": "0,11",
                        "id": "SRXALTURA"
                    },
                    {
                        "id": "SRXANCHO",
                        "value": "0,16"
                    },
                    {
                        "id": "SRXUNPES",
                        "value": "KGM"
                    },
                    {
                        "id": "SRXUNCAPAS_Pallets",
                        "value": "1000"
                    },
                    {
                        "id": "SRXUNMED_Pallets",
                        "value": "MTR"
                    },
                    {
                        "id": "SRXLARGO_Pallets",
                        "value": "1,20"
                    },
                    {
                        "value": "1200,00",
                        "id": "SRXPESOARTI_Pallets"
                    },
                    {
                        "value": "1,30",
                        "id": "SRXALTURA_Pallets"
                    },
                    {
                        "id": "SRXANCHO_Pallets",
                        "value": "1,20"
                    },
                    {
                        "id": "SRXUNPES_Pallets",
                        "value": "KGM"
                    },
                    {
                        "id": "SRXSKUCH",
                        "value": "14595372"
                    },
                    {
                        "value": "SRX",
                        "id": "SRXORIGEN"
                    },
                    {
                        "value": "CHILE",
                        "id": "PAIS_ATT"
                    },
                    {
                        "internalId": "1292",
                        "value": "J10020201",
                        "id": "SRXSUBCLASE"
                    },
                    {
                        "id": "SRXDESCSCLASE",
                        "value": "J10020201 BOTINES MUJ OUTDOOR"
                    },
                    {
                        "id": "SRXCLASE",
                        "value": "J100202",
                        "internalId": "1291"
                    },
                    {
                        "id": "SRXDESCCLASE",
                        "value": "J100202 BOTINES MUJER"
                    },
                    {
                        "id": "SRXSUBLINEA",
                        "internalId": "1287",
                        "value": "J1002"
                    },
                    {
                        "id": "SRXDESCSLIN",
                        "value": "J1002 ZAPATOS MUJER"
                    },
                    {
                        "internalId": "1265",
                        "id": "SRXLINEA",
                        "value": "J10"
                    },
                    {
                        "value": "J10 CALZADO",
                        "id": "SRXDESCLIN"
                    },
                    {
                        "value": "59990",
                        "id": "SRXPVENTACL"
                    },
                    {
                        "value": "IVA",
                        "id": "CTIP_IMPTO_CL"
                    },
                    {
                        "value": "30663,6",
                        "id": "SRXCOSTOCL"
                    },
                    {
                        "id": "DEFAULTUPCEAN_CL",
                        "value": "2014595372006"
                    },
                    {
                        "value": "2014595372006",
                        "id": "SRXUPCEAN"
                    },
                    {
                        "value": "false",
                        "id": "publishedAtg"
                    },
                    {
                        "value": "false",
                        "id": "SoldOnline"
                    },
                    {
                        "id": "lastUpdate",
                        "value": "2021-02-26 15:54:12"
                    },
                    {
                        "value": "false",
                        "id": "publishedConnect"
                    },
                    {
                        "value": "true",
                        "id": "rtvDevMasiva"
                    },
                    {
                        "id": "rtvDevUnitaria",
                        "value": "true"
                    },
                    {
                        "id": "lastReceiptDate",
                        "value": "2021-03-16 00:00:00"
                    },
                    {
                        "value": "2021-03-22 00:00:00",
                        "id": "lastSaleDate"
                    },
                    {
                        "value": "14595369",
                        "internalId": "16992197",
                        "id": "ID_ESTILO"
                    },
                    {
                        "value": " BOTIN PJ WD002 CHOCOLATE",
                        "id": "DESC_ESTILO"
                    },
                    {
                        "value": "14595372",
                        "id": "ID_SKU_ORIGINAL"
                    },
                    {
                        "value": "true",
                        "id": "SoldInStores"
                    },
                    {
                        "value": "true",
                        "id": "availableForPickupInStore"
                    },
                    {
                        "value": "true",
                        "id": "availableForShipToStore"
                    },
                    {
                        "id": "channelType",
                        "value": "Retail"
                    },
                    {
                        "value": "30663,6",
                        "id": "unitCost"
                    },
                    {
                        "value": " BOTIN PJ WD002 CHOCOLATE 37PANAMAJACKI21_WD002CA37",
                        "id": "longDescription"
                    },
                    {
                        "value": "200 ",
                        "id": "itemColor"
                    },
                    {
                        "value": "1459536920046",
                        "id": "styleSecuence"
                    },
                    {
                        "id": "markForDeletion",
                        "value": "CreateOrUpdate"
                    },
                    {
                        "value": "CLP",
                        "id": "currencyCode"
                    },
                    {
                        "value": "MTR",
                        "id": "dimensionUOM"
                    },
                    {
                        "value": "MTR3",
                        "id": "volumeUOM"
                    },
                    {
                        "value": "F",
                        "id": "supplierItemBarcode"
                    },
                    {
                        "id": "statusCode",
                        "value": "0"
                    },
                    {
                        "id": "goodsQuantity",
                        "value": "1"
                    },
                    {
                        "id": "itemSeason",
                        "value": "OI"
                    },
                    {
                        "value": "TotalStandardCost",
                        "id": "costType"
                    },
                    {
                        "value": "unit",
                        "id": "uom"
                    },
                    {
                        "value": "1",
                        "id": "scanQuantity"
                    },
                    {
                        "value": "000",
                        "id": "SlotMiscellaneous1"
                    },
                    {
                        "value": "A",
                        "id": "SlotMiscellaneous2"
                    },
                    {
                        "value": "A",
                        "id": "VelocityCode"
                    }
                ],
                "image": [
                    {
                        "url": "https://falabella.scene7.com/is/image/Falabella/14595372",
                        "viewNumber": 1
                    },
                    {
                        "url": "http://www.falabella.com/falabella-cl/product/",
                        "viewNumber": 2
                    }
                ]
            }
        ],
        "documentCode": "0039",
        "salesExecutives": [
            {
                "value": "00799874",
                "description": "salesManCode"
            },
            {
                "value": "00799874",
                "description": "salesManCode"
            }
        ],
        "previousDocument": [
            {
                "siiTimestamp": "0000"
            }
        ],
        "paymentDetails": [
            {
                "salesAmount": [
                    {
                        "value": "00109970",
                        "description": "transbankAmount"
                    },
                    {
                        "value": "0000",
                        "description": "chargeCode"
                    },
                    {
                        "value": "0000000000109970",
                        "description": "totalAmountCash"
                    },
                    {
                        "value": "00092412",
                        "description": "totalNetNCND"
                    },
                    {
                        "value": "00017558",
                        "description": "totalIvaNCND"
                    },
                    {
                        "value": "0000",
                        "description": "totalAditionalIvaNCND"
                    },
                    {
                        "value": "0000",
                        "description": "totalNetReverse"
                    },
                    {
                        "value": "0000",
                        "description": "totalIvaReverse"
                    },
                    {
                        "value": "0000",
                        "description": "totalAditionalIvaNCNDReverse"
                    }
                ],
                "docTypeNumberTBK": "001187248600",
                "sendMessageTBK": "00165712",
                "receiveMesgTBK": "00165737",
                "uniqueNumber": "0017375541005930210513165720",
                "cardBrand": "0002"
            }
        ],
        "deletedProductsNumber": "0000",
        "diminishProductsNumber": "0000",
        "transactionSetCode": "B",
        "transactionStatev": "Valid"
        }
        publisher.publish(json.dumps(data))
        time.sleep(random())
        i += 1

if __name__ == "__main__":
    main()