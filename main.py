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
        {  "name": "event_id", "type": "STRING"            } ,
        #{  "name": "event_type_name", "type": "STRING"            } ,
        #{  "name": "entity_type_name", "type": "STRING"            } ,
        {  "name": "country_cd", "type": "STRING"            } ,
        #{  "name": "channel_name", "type": "STRING"            } ,
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
        #{  "name": "transaction_date", "type": "DATE"     } ,
        #{  "name": "transaction_code", "type": "NUMERIC"    } , 
        #{  "name": "transaction_status", "type": "NUMERIC"  } , 
        #{  "name": "sequence_number", "type": "NUMERIC"     } , 
        #{  "name": "country_flag", "type": "STRING"         } , 
        #{  "name": "buy_date", "type": "DATE"             } , 
        #{  "name": "transaction_hour", "type": "STRING"     } , 
        #{  "name": "cashier_number", "type": "STRING"       } , 
        {
            "name": "products_details_rec",
            "type": "RECORD",
            "mode": "REPEATED",
            "description": "Products Details",
            "fields" : [
        #        { "name": "quantity", "type": "NUMERIC"                 },
        #        { "name": "transaction_change_type", "type": "STRING"   },
                { "name": "upc_number_num", "type": "NUMERIC"                }
        #        { "name": "product_description", "type": "STRING"       },
        #        { "name": "pos_description", "type": "STRING"           },
        #        { "name": "productType", "type": "STRING"               },
        #        { "name": "product_sku_mark", "type": "STRING"          },
        #        { "name": "sku", "type": "STRING"                       },
        #        { "name": "internal_id", "type": "STRING"               },
        #        { "name": "sale_amount", "type": "NUMERIC"               },
        #        { "name": "item_base_cost_without_taxes", "type": "NUMERIC" },
        #        { "name": "net_amount", "type": "NUMERIC"               },
        #        { "name": "net_amount_credit_note", "type": "NUMERIC"   },
        #        { "name": "product_iva_1", "type": "NUMERIC" },
        #        { "name": "amount_iva_1", "type": "NUMERIC"               },
        #        { "name": "iva_percent", "type": "NUMERIC"   }
            ]
        },
          
        # { "name": "document_code", "type": "STRING"   },
        #
        #{
        #    "name": "payment_details",
        #    "type": "RECORD",
        #    "mode": 'REPEATED',
        #    "fields" : [
        #    
        #    ]
        #}
        
        #{ "name": "deleted_products_number", "type": "STRING"   },
        #{ "name": "diminish_products_number", "type": "STRING"   },
        #{ "name": "transaction_set_code", "type": "STRING"   },
        #{ "name": "transaction_statev", "type": "STRING"   },

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
    attributes_parsed["event_id"] = attributes["eventId"]
    #attributes_parsed["event_type_name"] = attributes["eventType"]
    #attributes_parsed["entity_type_name"] = attributes["entityType"]
    attributes_parsed["country_cd"] = attributes["country"].lower()
    #attributes_parsed["channel_name"] = attributes["channel"]
    #attributes_parsed["date_time"] = attributes["datetime"]
    #attributes_parsed["version"] = attributes["version"]
    #attributes_parsed["commerce"] = attributes["commerce"]
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
        
        data = parseAttributes(el)

        try:
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

            data["cashier_number_num"] = element_data["cashierNumber"]
            
            if "transactionHour" in element_data:
                data["transaction_hour_tm"] = parseTime(element_data["transactionHour"])
            
            # ProductsDetails in 1, 35.
            products_detail = []
            
            if data["transaction_code_cd"] in [1, 35]:
                if "productsDetails" in element_data:
                    product_detail = {}
                    for product in element_data["productsDetails"]:
                        if "upcNumber" in product:
                            product_detail["upc_number_num"] = product["upcNumber"]
                        products_detail.append(product_detail)

            
            if "billsDetails" in element_data:
                for detail in element_data["billsDetails"]:
                    if "siiTicketNumber" in detail:
                        data["sii_ticket_number_num"] = detail["siiTicketNumber"]
                    if "identityNumberDocument" in detail:
                        data["identity_number_document_id"] = detail["identityNumberDocument"]
                    if "identityNumberDocumentType" in detail:
                        data["identity_number_document_type_cd"] = detail["identityNumberDocumentType"]
            #for bill_detail in parsed["billsDetails"]:
            #new_bill_detail = {}
            #new_bill_detail["sii_ticket_number"] = bill_detail["siiTicketNumber"]
            #new_bill_detail["identity_number_document"] = bill_detail["identityNumberDocument"]
            #new_parsed_bill_details.append(new_bill_detail)

            data["products_details_rec"] = products_detail
            
        except:
            print("Something went wrong")
            
        finally:
            data["partition_date_tmst"] = timestamp.to_rfc3339()
            yield data

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

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:

        streaming_data = ( 
            p | 'ReadFromPubSub' >> beam.io.gcp.pubsub.ReadFromPubSub( 
                subscription=known_args.input_subscription, 
                timestamp_attribute=None,
                with_attributes=True)
        )

        transform_data = (
            streaming_data 
            | 'Map result PubsubMessage' >> beam.Map(lambda elem: elem.data) # .data because is a PubSubMessage
            | 'Map result Json loads' >> beam.Map( lambda elem: json.loads(elem.decode('utf-8')) )
            | 'Transform base64' >> beam.ParDo( TransformBase() )
            #| 'Storage Data' >> beam.ParDo(StorageGcpNew('output_directory/') )
        )

        parsing_data = (
            transform_data
            | '' >> beam.ParDo( ParsingData() )
        )

        write_bq_transaction = (
            parsing_data | 'Write BigQuery Transaction' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )
    
if __name__ == "__main__":
    run()