""" 
Implementación Dataflow evento Sales
Pendiente:
    - Capturar atributos y enviar a dataset correspondiente.
    - Nomeclatura correcta tablas y campos.
    - Encriptación rut.
    - Almacenar en Storage.

 """
import argparse
import os
import logging
import json
import datetime
import base64
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.io.gcp.pubsub

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

# Service account key path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv('GCP_CREDENTIALS')
 # projects/your_gcp_project/subscriptions/your_pub_sub_subscription
INPUT_SUBSCRIPTION = os.getenv('GCP_PUBSUB_SUBSCRIPTION')
# your_project:your_dataset.your_table
BIGQUERY_TABLE = os.getenv('GCP_TABLE') 
# field_1:FIELD_1_TYPE,field_2:FIELD_2_TYPE,....
#BIGQUERY_SCHEMA = os.getenv('GCP_BIGQUERY_SCHEMA') 
OUTPUT_STORAGE = "1"

BIGQUERY_SCHEMA = {
    "fields": [
        #{  "name": "event_id", "type": "STRING"            } ,
        #{  "name": "country_cd", "type": "STRING"            } ,
        {  "name": "transaction_date_dt", "type": "DATE" } ,
        {  "name": "store_id", "type": "NUMERIC" } ,
        {  "name": "terminal_number_num", "type": "NUMERIC" } ,  
        {  "name": "sequence_number_num", "type": "NUMERIC" } ,
        {  "name": "transaction_code_cd", "type": "NUMERIC" } ,
        {  "name": "transaction_status_cd", "type": "NUMERIC" } ,
        {  "name": "transaction_set_code_cd", "type": "STRING" } ,
        {  "name": "transaction_state_v_desc", "type": "STRING" } ,
        {  "name": "cashier_number_num", "type": "NUMERIC"} ,
        {  "name": "transaction_hour_tm", "type": "TIME" },
        {  "name": "sii_ticket_number_num", "type": "NUMERIC", "description": "Sii Ticket Number" },
        {  "name": "identity_number_document_id", "type": "STRING"    } ,
        {  "name": "identity_number_document_type_cd", "type": "NUMERIC", "description" : "Type code Document" },
        {
            "name": "product_rec",
            "type": "RECORD",
            "mode": "REPEATED",
            "description": "Products Details",
            "fields" : [
                { "name": "upc_number_num", "type": "NUMERIC", "description":"Bar code UPC/EAN"  },
                { "name": "sku_id", "type": "STRING", "description":"Stock Keeping Unit"  },
                { "name": "quantity_qty", "type": "NUMERIC", "description" : "Quantity of products"  },
                { "name": "sale_amount_amt", "type": "NUMERIC", "description" : "Sale Amount"  },
                { "name": "net_amount_amt", "type": "NUMERIC", "description" : "Net Amount"  },
                { "name": "iva1_amt", "type": "NUMERIC", "description" : "Iva Amount"  },
                { "name": "iva_percent", "type": "NUMERIC", "description" : "Percent Amount"  },
                { "name": "promotion_discount_amt", "type": "NUMERIC", "description" : "Promotion Discount Amount"  },
                { "name": "product_discount_amt", "type": "NUMERIC", "description" : "Product Discuount Amount"  },
                { "name": "base_without_tax_amt", "type": "NUMERIC", "description" : "Base without tax Amount"  },
                { "name": "iva_import_amt", "type": "NUMERIC", "description" : "Iva Import Amount"  },
                { "name": "iva_code_cd", "type": "STRING", "description" : "Iva Code"  },
                { "name": "discount_type_cd", "type": "STRING", "description" : "Discount Type"  },
                { "name": "item_base_cost_without_taxes_cd", "type": "STRING", "description" : "Item Base COst"  },
                { "name": "net_amount_credit_note_amt", "type": "NUMERIC", "description" : "Net amount Credit Note"  },
                { "name": "promotion_value_amt", "type": "NUMERIC", "description" : "Promotion value amount"  },
                
            ]
        },
        {  "name": "document_code_cd", "type": "STRING"   },
        {  "name": "order_number_id", "type" :"STRING", "description": "Order number"},
        {  "name": "bill_details", "type": "STRING", "description":"Payload Original Message"},
        {  "name": "payment_details", "type": "STRING", "description":"Payload Original Message"},
        {  "name": "sales_amount", "type": "STRING", "description":"Payload Original Message"},
        {  "name": "sales_tax", "type": "STRING", "description":"Payload Original Message"},
        {  "name": "partition_date_tmst", "type": "TIMESTAMP"    } ,

    ]
}

additional_bq_parameters = { "timePartitioning": {"type": "DAY", "field": "partition_date_tmst"}}

def parseDate(string_date):
    return datetime.datetime.strptime(string_date, "%Y%m%d").strftime("%Y-%m-%d")

def parseTime(string_time):
    str_init = len(string_time) - 4
    str_end = len(string_time)
    return datetime.datetime.strptime(string_time[str_init:str_end] + '00', "%H%M%S").strftime("%H:%M:%S")
    #return datetime.datetime.strptime(string_time, "%H%M%S").strftime("%H:%M:%S")

def parseAttributes(attributes):
    attributes_parsed = {}
    #attributes_parsed["event_id"] = attributes["eventId"]
    #--- attributes_parsed["event_type_name"] = attributes["eventType"]
    #--- attributes_parsed["entity_type_name"] = attributes["entityType"]
    attributes_parsed["country_cd"] = attributes["country"].lower()
    #--- attributes_parsed["channel_name"] = attributes["channel"]
    #--- attributes_parsed["date_time"] = attributes["datetime"]
    #--- attributes_parsed["version"] = attributes["version"]
    #--- attributes_parsed["commerce"] = attributes["commerce"]
    return attributes_parsed

class TransformBase(beam.DoFn):
    def process(self, el: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        base64_message = el["message"]["data"]
        base64_bytes = str.encode(base64_message)
        message_bytes = base64.urlsafe_b64decode(base64_bytes)
        message_decode = message_bytes.decode('utf-8')
        #message_clean = message_decode.replace("\\", "")
        message_json = json.loads(message_decode)

        yield message_json

class ParsingData(beam.DoFn):
    def process(self, el: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        
        #data = parseAttributes(el)
        data = {}
        #try:
            # header["attributes"]
        data_sale = el["data"]
        data_json = json.loads(data_sale) #  "data" : { "sale": { .... } }
        element_data = data_json["sale"] 
        
        data["transaction_date_dt"] = parseDate(element_data["transactionDate"])
        data["store_id"] = element_data["storeId"]
        data["terminal_number_num"] = element_data["terminalNumber"]
        data["sequence_number_num"]  = element_data["sequenceNumber"]
        
        data["transaction_code_cd"] = element_data["transactionCode"]
        data["transaction_status_cd"] = element_data["transactionStatus"]

        if "transactionSetCode" in element_data:
            data["transaction_set_code_cd"] = element_data["transactionSetCode"]
        if "transactionStatev" in element_data:
            data["transaction_state_v_desc"] = element_data["transactionStatev"]

        if "cashierNumber" in element_data:
            data["cashier_number_num"] = element_data["cashierNumber"]
        
        if "transactionHour" in element_data:
            data["transaction_hour_tm"] = parseTime(element_data["transactionHour"])
        
        # Transacciones a persistir (1,2,3,4,30,31,35,45,46)
        # transaction_code_cd -> 42, apertura de cajas.
        
        products_detail = []
        
        if data["transaction_code_cd"] in [1, 35]:
            if "productsDetails" in element_data:
                for product in element_data["productsDetails"]:
                    product_detail = {} # Dentro del for por la extraña estructura de Perú    
                    if "upcNumber" in product:
                        product_detail["upc_number_num"] = product["upcNumber"]
                    if "upcProductGuideD" in product:
                        product_detail["upc_number_num"] = product["upcProductGuideD"]
                    if "sku" in product:
                        product_detail["sku_id"] = product["sku"]
                    if "quantity" in product:
                        product_detail["quantity_qty"] = product["quantity"]
                    if "salesAmount" in product:
                        #print(product["salesAmount"])
                        data["sales_amount"] = str(product["salesAmount"])

                        for amount in product["salesAmount"]:
                            if amount["description"] == "saleAmount":
                                product_detail["sale_amount_amt"] = amount["value"]
                            if amount["description"] == "netAmount":
                                product_detail["net_amount_amt"] = amount["value"]
                            if amount["description"] == "promotionDiscountAmount":
                                product_detail["promotion_discount_amt"] = amount["value"]
                            if amount["description"] == "productDiscountAmount":
                                product_detail["product_discount_amt"] = amount["value"]
                            if amount["description"] == "discountType":
                                product_detail["discount_type_cd"] = amount["value"]
                            if amount["description"] == "itemBaseCostWithoutTaxes":
                                product_detail["item_base_cost_without_taxes_cd"] = amount["value"]
                            if amount["description"] == "netAmountCreditNote":
                                product_detail["net_amount_credit_note_amt"] = amount["value"]
                            if amount["description"] == "promotionValue":
                                product_detail["promotion_value_amt"] = amount["value"]
                    if "salesTax" in product:
                        #print(product["salesTax"])
                        data["sales_tax"] = str(product["salesTax"])
                        for tax in product["salesTax"]:
                            if tax["description"] == "productIVA1":
                                product_detail["iva1_amt"] = tax["value"]
                            if tax["description"] == "ivaPercent":
                                product_detail["iva_percent"] = tax["value"]
                            if tax["description"] == "baseWithoutTax":
                                product_detail["base_without_tax_amt"] = tax["value"]
                            if tax["description"] == "ivaImport":
                                product_detail["iva_import_amt"] = tax["value"]
                            if tax["description"] == "ivaCode":
                                product_detail["iva_code_cd"] = tax["value"]
                                                        
                    # En Perú funciona de manera extraña.    
                    if (not bool(product_detail) ) == False:
                        products_detail.append(product_detail)

        # Por validar, ya que se generaría un record sobre record.
        data["product_rec"] = products_detail

        if "billsDetails" in element_data:
            for detail in element_data["billsDetails"]:
                if "siiTicketNumber" in detail:
                    data["sii_ticket_number_num"] = detail["siiTicketNumber"]
                if "identityNumberDocument" in detail:
                    try:
                        message = detail["identityNumberDocument"]
                        message_bytes = message.encode('ascii')
                        base64_bytes = base64.b64encode(message_bytes)
                        base64_message = base64_bytes.decode('ascii')
                        data["identity_number_document_id"] = base64_message
                    except:
                        print('error b64encode')
                if "identityNumberDocumentType" in detail:
                    data["identity_number_document_type_cd"] = detail["identityNumberDocumentType"]


        #stamps = []
        #if "stamps" in element_data:
        #    for stamp in element_data["stamps"]:
        #        stamp_detail = {}
        #        stamp_detail["description"] = stamp["description"]
        #        stamp_detail["value"] = stamp["value"]
        #        stamps.append(stamp_detail)

        #data["stamps_rec"] = stamps

        if "documentCode" in element_data:
            data["document_code_cd"] = element_data["documentCode"]

        #if "previousDocument" in element_data:
        #    for previous in element_data["previousDocument"]:
        #        if "siiTimestamp" in previous:
        #            data["sii_timestamp"] = previous["siiTimestamps"]

        #executives = []
        #if "salesExecutives" in element_data:
        #    for executive in element_data["salesExecutives"]:
        #        executiv = {}
        #        executiv["description"] = executive["description"]
        #        executiv["value"] = executive["value"]
        #        executives.append(executiv)

        #data["sales_executives_rec"] = executives

        if "buyOrderNumber" in element_data:
            data["order_number_id"] = element_data["buyOrderNumber"]
            
        #except:
        #    print("Something went wrong")
            
        #finally:
        #data["payload_data"] = str(data_json["sale"])
        
        #if "billsDetails" in element_data:
        #    data["bill_details"] = str(element_data["billsDetails"])
        
        #if "paymentDetails" in element_data:
        #    data["payment_details"] = str(element_data["paymentDetails"])
        

        data["partition_date_tmst"] = timestamp.to_rfc3339()
        yield data
    
class DataPersist(beam.DoFn):
    def process(self, el: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        yield el

class DataTransformFiltering(beam.DoFn):

    def __init__(self, country):
        self.country = country

    def process(self, el: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        
        data = parseAttributes(el)

        data_sale = el["data"]
        data_json = json.loads(data_sale) #  "data" : { "sale": { .... } }
        element_data = data_json["sale"] 

        if data["country_cd"] == self.country:
            if "transactionCode" in element_data:
                if element_data["transactionCode"] in [1,2,3,4,30,31,35,45,46]:
                    yield el

        #if "transactionCode" in element_data:
        #    if element_data["transactionCode"] in [1,2,3,4,30,31,35,45,46]:
        #        el_valid = data
        #    else:
        #        el_valid = None

        #if "countryFlag" in element_data:
        #    if self.country == element_data["countryFlag"].lower():
        #        print(self.country)
                
        #yield data

def run():
    # Parsing arguments
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subscription like "projects/your_gcp_project/subscriptions/your_pub_sub_subscription" ',
        default=INPUT_SUBSCRIPTION
    )

    parser.add_argument(
        "--output_table", help="Output BigQuery Table", default=BIGQUERY_TABLE
    )
    parser.add_argument(
        "--output_schema",
        help="Output BigQuery Schema in text format",
        default=BIGQUERY_SCHEMA,
    )
    # All the arg that not in the definition are considerated like arg to pipelina
    known_args, pipeline_args = parser.parse_known_args()

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    print(known_args.output_table)

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:

        streaming_data = ( 
            p | 'ReadFromPubSub' >> beam.io.gcp.pubsub.ReadFromPubSub( 
                subscription=known_args.input_subscription, 
                timestamp_attribute=None,
                with_attributes=True)
        )

        # Cada mensaje en el Topic llega como [ {} ], esto indica que puede venir más de un elemento.

        transform_data = (
            streaming_data 
            | 'Map result PubsubMessage' >> beam.Map(lambda elem: elem.data) # .data because is a PubSubMessage
            | 'Map result Json loads' >> beam.Map( lambda elem: json.loads(elem.decode('utf-8')) )
            | 'Transform base64' >> beam.ParDo( TransformBase() )
            #| 'Storage Data' >> beam.ParDo(StorageGcpNew('output_directory/') )
        )

        #data_filtering = (
        #    transform_data
        #    | 'Filtering Data Sales' >> beam.ParDo( DataTransformFiltering() )
        #)

        # Debería ser al revés, luego se modificará.
        data_filtering_cl = (
            transform_data
            | 'Filtering data CL' >> beam.ParDo( DataTransformFiltering('cl') )
        )

        data_filtering_pe = (
            transform_data
            | 'Filtering data PE' >> beam.ParDo( DataTransformFiltering('pe') )
        )

        data_filtering_co = (
            transform_data
            | 'Filtering data CO' >> beam.ParDo( DataTransformFiltering('co') )
        )
        
        data_filtering_ar = (
            transform_data
            | 'Filtering data AR' >> beam.ParDo( DataTransformFiltering('ar') )
        )

        # Storage in Bucket
        data_persist_cl = (
            data_filtering_cl
            | 'Select Data to Persist CL' >> beam.ParDo( DataPersist() )
        )

        data_persist_ar = (
            data_filtering_ar
            | 'Select Data to Persist AR' >> beam.ParDo( DataPersist() )
        )

        data_persist_co = (
            data_filtering_co
            | 'Select Data to Persist CO' >> beam.ParDo( DataPersist() )
        )

        data_persist_pe = (
            data_filtering_pe
            | 'Select Data to Persist PE' >> beam.ParDo( DataPersist() )
        )
        # Storage in Bucket

        parsing_data_cl = (
            #transform_data
            data_filtering_cl
            | 'Parsing All Data CL' >> beam.ParDo( ParsingData() )
        )

        parsing_data_ar = (
            #transform_data
            data_filtering_ar
            | 'Parsing All Data AR' >> beam.ParDo( ParsingData() )
        )

        parsing_data_co = (
            #transform_data
            data_filtering_co
            | 'Parsing All Data CO' >> beam.ParDo( ParsingData() )
        )

        parsing_data_pe = (
            #transform_data
            data_filtering_pe
            | 'Parsing All Data PE' >> beam.ParDo( ParsingData() )
        )

        write_bq_transaction_cl = (
            parsing_data_cl | 'Write BigQuery Transaction CL' >> beam.io.WriteToBigQuery(
                #known_args.output_table,
                'xenon-chain-308319:trf_fal_corp_falabella_stro_uat.btd_cl_transaction', 
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )

        write_bq_transaction_ar = (
            parsing_data_ar | 'Write BigQuery Transaction AR' >> beam.io.WriteToBigQuery(
                #known_args.output_table,
                'xenon-chain-308319:trf_fal_corp_falabella_stro_uat.btd_ar_transaction', 
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )

        write_bq_transaction_co = (
            parsing_data_co | 'Write BigQuery Transaction CO' >> beam.io.WriteToBigQuery(
                #known_args.output_table,
                'xenon-chain-308319:trf_fal_corp_falabella_stro_uat.btd_co_transaction', 
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )

        write_bq_transaction_pe = (
            parsing_data_pe | 'Write BigQuery Transaction PE' >> beam.io.WriteToBigQuery(
                #known_args.output_table,
                'xenon-chain-308319:trf_fal_corp_falabella_stro_uat.btd_pe_transaction', 
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )
    
if __name__ == "__main__":
    run()