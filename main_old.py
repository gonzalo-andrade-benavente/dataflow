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
        
        #{  "name": "transaction_date", "type": "DATE"     } ,
        #{  "name": "transaction_code", "type": "NUMERIC"    } , 
        #{  "name": "transaction_status", "type": "NUMERIC"  } , 
        #{  "name": "sequence_number", "type": "NUMERIC"     } , 
        #{  "name": "country_flag", "type": "STRING"         } , 
        #{  "name": "buy_date", "type": "DATE"             } , 
        #{  "name": "transaction_hour", "type": "STRING"     } , 
        #{  "name": "cashier_number", "type": "STRING"       } , 
        
        #{
        #    "name": "bills_details",
        #    "type": "RECORD",
        #    "mode": "REPEATED",
        #    "fields": [
        #        {   "name": "sii_ticket_number", "type": "STRING"           },
        #        {   "name": "identity_number_document", "type": "STRING"    }
        #    ]
        #},

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

additional_bq_parameters_pay = { "timePartitioning": {"type": "DAY", "field": "partition_date_tmst"}}

output_table_pay = "%s:%s.%s" % (os.getenv('GCP_PROJECT'), os.getenv('GCP_BIGQUERY_DATASET'), 'btd_payment') 

output_schema_pay = {
    "fields": [
        {  "name": "event_id", "type": "STRING" } ,
        {  "name": "event_type_name", "type": "STRING" } ,
        {  "name": "entity_type_name", "type": "STRING" } ,
        {  "name": "country_cd", "type": "STRING" } ,
        {  "name": "channel_name", "type": "STRING" },
        {  "name": "transaction_date_dt", "type": "DATE" } ,
        {  "name": "store_id", "type": "NUMERIC" } ,
        {  "name": "terminal_number_num", "type": "NUMERIC" } ,  
        {  "name": "sequence_number_num", "type": "NUMERIC" } ,
        {  "name": "transaction_code_cd", "type": "NUMERIC" } ,
        {  "name": "transaction_status_cd", "type": "NUMERIC" } ,
        {  "name": "transaction_set_code_cd", "type": "STRING" } ,
        {  "name": "transaction_state_v_desc", "type": "STRING" } ,
        {
            "name": "payment_details_rec",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields" : [
                { "name": "doc_type_numbertbk_num", "type": "NUMERIC" },
                { "name": "send_messagetbk_tm", "type": "STRING" },
                { "name": "received_messagetbk_tm", "type": "STRING" },
                { "name": "unique_number_num", "type": "NUMERIC" },
                { "name": "card_brand_cd", "type": "STRING" },
                { "name": "transbank_amount_amt", "type": "NUMERIC" },
                { "name": "charge_code_cd", "type": "STRING" },
                { "name": "total_amount_cash_amt", "type": "NUMERIC" },
                { "name": "total_net_ncnd_amt", "type": "NUMERIC" },
                { "name": "total_iva_ncnd_amt", "type": "NUMERIC" },
                { "name": "total_aditional_iva_ncnd_amt", "type": "NUMERIC" },
                { "name": "total_net_reverse_amt", "type": "NUMERIC"  },
                { "name": "total_iva_reverse_amt", "type": "NUMERIC" },
                { "name": "total_aditional_iva_ncnd_reverse_amt", "type": "NUMERIC"  },
            ]
        },
        {  "name": "partition_date_tmst", "type": "TIMESTAMP" } , 
    ]
}

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

            
            data["products_details_rec"] = products_detail
            


        except:
            print("Something went wrong")
            
        finally:
            data["partition_date_tmst"] = timestamp.to_rfc3339()
            yield data


       

class StorageGcpNew(beam.DoFn):
    def process(self, el: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        base64_message = el["message"]["data"]
        base64_bytes = str.encode(base64_message)
        message_bytes = base64.urlsafe_b64decode(base64_bytes)
        message_decode = message_bytes.decode('utf-8')
        #message_clean = message_decode.replace("\\", "")
        message_json = json.loads(message_decode)

        # header["attributes"]
        attributes = parseAttributes(message_json)

        print(attributes)

class StorageGcp(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
    
    #def process(self, batch, window=beam.DoFn.WindowParam):
    #    print('Falta persistir en Cloud Storage')

    def process(self, element: apache_beam.io.gcp.pubsub.PubsubMessage, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        data_bytes = element.data
        json_data = json.loads(data_bytes.decode("utf-8"))

        #data:bytes = element.data
        #json_data = json.loads(data)
        
        #json_data = json.loads(data.decode("utf-8"))
        
        base64_message = json_data["message"]["data"]
        #base64_message = 'R29uemFsbyBxdWUgcGFzYQ==' # Gonzalo que pasa
        #base64_message = "eyJldmVudElkIjoiNWQxNjI1MTAtYzQ4Yy0xMWViLWFhMmMtYTdiZjUxM2QyN2Y5IiwiZXZlbnRUeXBlIjoic2FsZXMiLCJlbnRpdHlJZCI6IjVkMTYyNTEwLWM0OGMtMTFlYi1hYTJjLWE3YmY1MTNkMjdmOSIsImVudGl0eVR5cGUiOiJzYWxlc0NyZWF0aW9uIiwidGltZXN0YW1wIjoiMTYyMjczOTI3MDM2OCIsImRhdGV0aW1lIjoiMjAyMS0wNi0wMyAxMjo1NDozMCIsInZlcnNpb24iOiIxLjAiLCJjb3VudHJ5IjoiQ08iLCJjb21tZXJjZSI6IkZhbGFiZWxsYSIsImNoYW5uZWwiOiJwdWJzdWIiLCJkb21haW4iOiJjb3JwIiwiY2FwYWJpbGl0eSI6ImNvcnAiLCJtaW1lVHlwZSI6ImFwcGxpY2F0aW9uL2pzb24iLCJkYXRhIjoie1wic2FsZVwiOntcInN0b3JlSWRcIjozNixcInRlcm1pbmFsTnVtYmVyXCI6OCxcInRyYW5zYWN0aW9uRGF0ZVwiOlwiMjAyMTA2MDNcIixcInRyYW5zYWN0aW9uQ29kZVwiOjEsXCJ0cmFuc2FjdGlvblN0YXR1c1wiOjIsXCJzZXF1ZW5jZU51bWJlclwiOjkyNzksXCJjb3VudHJ5RmxhZ1wiOlwiQ09cIixcImFtb3VudHNcIjpbe1wiYnV5RGF0ZVwiOlwiMjAyMTA2MDNcIixcInRyYW5zYWN0aW9uSG9wdXJYRlwiOlwiMDAwMDExNTNcIn1dLFwiYmlsbHNEZXRhaWxzXCI6W3tcInNpaVRpY2tldE51bWJlclwiOlwiMTIwMDAwMTY3NjM5XCIsXCJpZGVudGl0eU51bWJlckRvY3VtZW50VHlwZVwiOlwiMDAwMVwiLFwiaWRlbnRpdHlOdW1iZXJEb2N1bWVudFwiOlwiMDAwMVwifV0sXCJjYXNoaWVyTnVtYmVyXCI6XCIwMDA3NDUwMFwiLFwicHJvZHVjdHNEZXRhaWxzXCI6W3tcInVwY051bWJlclwiOlwiMDAwMDYyODE3NTQ2NDUzNVwiLFwibm9ybWFsUHJpY2VcIjpcIjAwMTU5OTkwXCIsXCJjdXJyZW50UHJpY2VcIjpcIjAwMTU5OTkwXCIsXCJhbW91bnRXaXRob3V0VGF4XCI6XCIwMDEzNDQ0NVwiLFwiaXRlbUlWQVwiOlwiMDAwMjU1NDVcIixcImFncmVlbWVudENvZGVcIjpcIjAwMDFcIixcIml2YUNvZGVcIjpcIjAwMDFcIixcInByb21vdGlvbkNvZGVHZW5lcmF0ZWRCeVNlcmlhbENvdXBvblwiOlwiMDAwNTUyOTZcIixcInNhbGVzQW1vdW50XCI6W3tcInZhbHVlXCI6XCIwMDE1OTk5MFwiLFwiZGVzY3JpcHRpb25cIjpcInByb2R1Y3REaXNjb3VudEFtb3VudFwifV0sXCJzYWxlc1RheFwiOlt7XCJ2YWx1ZVwiOlwiMDAxMzQ0NDVcIixcImRlc2NyaXB0aW9uXCI6XCJiYXNlV2l0aG91dFRheFwifSx7XCJ2YWx1ZVwiOlwiMDAwMjU1NDVcIixcImRlc2NyaXB0aW9uXCI6XCJpdmFJbXBvcnRcIn0se1widmFsdWVcIjpcIjAwMDFcIixcImRlc2NyaXB0aW9uXCI6XCJpdmFDb2RlXCJ9XSxcInNrdVwiOlwiMTE4ODM3MjlcIixcImludGVybmFsSWRcIjpcIjEzOTQxMDcyXCIsXCJjbGFzc2lmaWNhdGlvbnNcIjpbe1widHlwZVwiOlwiQ29tbWVyY2lhbExpbmtcIixcImNsYXNzaWZpY2F0aW9uSWRcIjpcIkoxMDAxMDEwMVwiLFwiaW50ZXJuYWxJZFwiOlwiNzgyXCJ9XSxcImF0dHJpYnV0ZXNcIjpbe1widmFsdWVcIjpcIlYyMDIyXCIsXCJpZFwiOlwiU1JYQ1RFTVBPUkFEQVwifSx7XCJpZFwiOlwiU1JYQ09EREVTQ0FESVwiLFwidmFsdWVcIjpcIkNTUFJJTkctMTMyMDY2MzYtMzAwNC00MDIwLTEtNDEwLTQxXCJ9LHtcImlkXCI6XCJTUlhTRUdVTkRPTVNHXCIsXCJ2YWx1ZVwiOlwiRmFsc29cIn0se1widmFsdWVcIjpcImZhbHNlXCIsXCJpZFwiOlwiSXNNYXJrZXRQbGFjZVwifSx7XCJpZFwiOlwiTUFSQ0FfQVRUXCIsXCJ2YWx1ZVwiOlwiQ0FMTCBJVCBTUFJJTkdcIn0se1widmFsdWVcIjpcIkNBTEwgSVQgU1BcIixcImlkXCI6XCJNQVJDQV9JU0FcIn0se1widmFsdWVcIjpcIkNTUFJJTkctMTMyMDY2MzYtMzAwNC00MDIwLTEtNDEwLTQxXCIsXCJpZFwiOlwiRGlzcGxheU5hbWVcIn0se1widmFsdWVcIjpcIjEzOTQxMDcyXCIsXCJpZFwiOlwiZXh0ZXJuYWxSZWZlcmVuY2VJZFwifSx7XCJpZFwiOlwiQ01PREVMT1wiLFwidmFsdWVcIjpcIkNPTk5FUjQxMFwifSx7XCJpZFwiOlwiTU9ERUxPX0FUVFwiLFwidmFsdWVcIjpcIkNPTk5FUjQxMFwifSx7XCJpZFwiOlwiU1JYVElQUFJEXCIsXCJsb3ZpZFwiOlwiUFwiLFwidmFsdWVcIjpcIlByb2R1Y3RvXCJ9LHtcInZhbHVlXCI6XCJQXCIsXCJpZFwiOlwiU1JYQ1RQUFJEXCJ9LHtcImlkXCI6XCJBY3RpdmVTdGF0dXNcIixcInZhbHVlXCI6XCJBQ1RJVk9cIn0se1wiaWRcIjpcIlNUQVRVU0NIXCIsXCJ2YWx1ZVwiOlwiMFwifSx7XCJpZFwiOlwiU1JYRENSRUFDSU9OXCIsXCJ2YWx1ZVwiOlwiMjAyMS0wMi0wMiAwMDowMDowMFwifSx7XCJ2YWx1ZVwiOlwiTE1JR09NRVpcIixcImlkXCI6XCJTUlhVU1JDUkVBXCJ9LHtcInZhbHVlXCI6XCI2XCIsXCJpZFwiOlwiU1JYVU5FTVBBUVVFXCJ9LHtcImlkXCI6XCJTUlhVVFJBTlNGRVJFTkNJQVwiLFwidmFsdWVcIjpcIjFcIn0se1wiaWRcIjpcIlNSWFJVVFBST1ZcIixcInZhbHVlXCI6XCI0NDQ0NDQzNzhcIn0se1wiaWRcIjpcIkJpemFnaVJ1dFByb3ZlZWRvclwiLFwidmFsdWVcIjpcIjQ0NDQ0NDM3OFwifSx7XCJpZFwiOlwiQml6YWdpRFZcIixcInZhbHVlXCI6XCI1XCJ9LHtcInZhbHVlXCI6XCJBTERPIEdST1VQIElOVEVSTkFUSU9OQUwgQUdcIixcImlkXCI6XCJwcm92aWRlck5hbWVcIn0se1wiaWRcIjpcIlNlbGxlck5hbWVcIixcInZhbHVlXCI6XCJBTERPIEdST1VQIElOVEVSTkFUSU9OQUwgQUdcIn0se1wiaWRcIjpcInByb3ZpZGVySWRcIixcInZhbHVlXCI6XCI0NDQ0NDQzNzgtNVwifSx7XCJ2YWx1ZVwiOlwiMzM0MzdcIixcImlkXCI6XCJwcm92aWRlckNvZGVcIn0se1widmFsdWVcIjpcIjExODgzNzIyXCIsXCJpZFwiOlwiU0tVX1BBRFJFX0NMXCJ9LHtcImlkXCI6XCJTUlhDUkVDQVVEQVwiLFwidmFsdWVcIjpcIlZlbnRhXCJ9LHtcInZhbHVlXCI6XCJBVVRPQURIRVNJVk9cIixcImlkXCI6XCJTUlhDRVRJUVVFVEFcIn0se1wiaWRcIjpcIlNSWE1SQ1BST1BJQVwiLFwidmFsdWVcIjpcIlByb3BpYVwifSx7XCJpZFwiOlwiU1JYQ09EREVTQ1BPU1wiLFwidmFsdWVcIjpcIkNTUFJJTkctMTMyMDY2M1wifSx7XCJ2YWx1ZVwiOlwiMTU1XCIsXCJpZFwiOlwiU1JYQ09EQ01QQ0xcIn0se1wiaWRcIjpcIkNCUk9LRVJcIixcInZhbHVlXCI6XCIxNTk2MzczNlwifSx7XCJ2YWx1ZVwiOlwiSW1wb3J0YWRvXCIsXCJpZFwiOlwiU1JYSU1QT1JUQURPXCJ9LHtcInZhbHVlXCI6XCIxMzk0MTA3MlwiLFwiaWRcIjpcIkJBQ0tFTkRfSURfQ0xcIn0se1wiaWRcIjpcIlNSWE9NU1RBTUFOT1wiLFwidmFsdWVcIjpcIlhTXCJ9LHtcImlkXCI6XCJTUlhPTVNBR1JVUEFNSUVOVE9cIixcInZhbHVlXCI6XCJHMVwifSx7XCJ2YWx1ZVwiOlwiTWluaVwiLFwiaWRcIjpcIlNSWE9NU0dMT1NBVEFNQU5PXCJ9LHtcInZhbHVlXCI6XCJYU1wiLFwiaWRcIjpcIlJlZmVyZW5jZUZpZWxkOFwifSx7XCJpZFwiOlwiUmVmZXJlbmNlRmllbGQxMFwiLFwidmFsdWVcIjpcIkcxXCJ9LHtcInZhbHVlXCI6XCJNaW5pXCIsXCJpZFwiOlwiQ29tbWVyY2VBdHRyaWJ1dGUyXCJ9LHtcInZhbHVlXCI6XCJBWlVMIDQxMFwiLFwiaWRcIjpcIkNPTE9SX05SRlwifSx7XCJpZFwiOlwiU1JYVEFMTEFcIixcInZhbHVlXCI6XCI0MVwifSx7XCJpZFwiOlwiVEFMTEFfQVRUXCIsXCJ2YWx1ZVwiOlwiNDFcIn0se1wiaWRcIjpcIlNSWFVOQ0FQQVNcIixcInZhbHVlXCI6XCIxXCJ9LHtcInZhbHVlXCI6XCJNVFJcIixcImlkXCI6XCJTUlhVTk1FRFwifSx7XCJ2YWx1ZVwiOlwiMCwzN1wiLFwiaWRcIjpcIlNSWExBUkdPXCJ9LHtcImlkXCI6XCJTUlhQRVNPQVJUSVwiLFwidmFsdWVcIjpcIjEsMjdcIn0se1wiaWRcIjpcIlNSWEFMVFVSQVwiLFwidmFsdWVcIjpcIjAsMTJcIn0se1widmFsdWVcIjpcIjAsMjJcIixcImlkXCI6XCJTUlhBTkNIT1wifSx7XCJpZFwiOlwiU1JYVU5QRVNcIixcInZhbHVlXCI6XCJLR01cIn0se1wiaWRcIjpcIlNSWFVOQ0FQQVNfQ2FzZXNcIixcInZhbHVlXCI6XCI2XCJ9LHtcInZhbHVlXCI6XCJNVFJcIixcImlkXCI6XCJTUlhVTk1FRF9DYXNlc1wifSx7XCJ2YWx1ZVwiOlwiMCw0NFwiLFwiaWRcIjpcIlNSWExBUkdPX0Nhc2VzXCJ9LHtcInZhbHVlXCI6XCI3LDYwXCIsXCJpZFwiOlwiU1JYUEVTT0FSVElfQ2FzZXNcIn0se1widmFsdWVcIjpcIjAsMzdcIixcImlkXCI6XCJTUlhBTFRVUkFfQ2FzZXNcIn0se1widmFsdWVcIjpcIjAsMzlcIixcImlkXCI6XCJTUlhBTkNIT19DYXNlc1wifSx7XCJpZFwiOlwiU1JYVU5QRVNfQ2FzZXNcIixcInZhbHVlXCI6XCJLR01cIn0se1widmFsdWVcIjpcIjZcIixcImlkXCI6XCJTUlhVTkNBUEFTX1BhbGxldHNcIn0se1widmFsdWVcIjpcIk1UUlwiLFwiaWRcIjpcIlNSWFVOTUVEX1BhbGxldHNcIn0se1wiaWRcIjpcIlNSWExBUkdPX1BhbGxldHNcIixcInZhbHVlXCI6XCIxLDIwXCJ9LHtcImlkXCI6XCJTUlhQRVNPQVJUSV9QYWxsZXRzXCIsXCJ2YWx1ZVwiOlwiMTM2LDgwXCJ9LHtcInZhbHVlXCI6XCIxLDExXCIsXCJpZFwiOlwiU1JYQUxUVVJBX1BhbGxldHNcIn0se1widmFsdWVcIjpcIjEsMDBcIixcImlkXCI6XCJTUlhBTkNIT19QYWxsZXRzXCJ9LHtcInZhbHVlXCI6XCJLR01cIixcImlkXCI6XCJTUlhVTlBFU19QYWxsZXRzXCJ9LHtcInZhbHVlXCI6XCIxMTg4MzcyOVwiLFwiaWRcIjpcIlNSWFNLVUNIXCJ9LHtcImlkXCI6XCJTUlhPUklHRU5cIixcInZhbHVlXCI6XCJTUlhcIn0se1wiaWRcIjpcIlBBSVNfQVRUXCIsXCJ2YWx1ZVwiOlwiQ0FOQURBXCJ9LHtcImludGVybmFsSWRcIjpcIjc4MlwiLFwiaWRcIjpcIlNSWFNVQkNMQVNFXCIsXCJ2YWx1ZVwiOlwiSjEwMDEwMTAxXCJ9LHtcImlkXCI6XCJTUlhERVNDU0NMQVNFXCIsXCJ2YWx1ZVwiOlwiSjEwMDEwMTAxIFpBUCBIT00gVVJCQU5PXCJ9LHtcInZhbHVlXCI6XCJKMTAwMTAxXCIsXCJpbnRlcm5hbElkXCI6XCI3ODFcIixcImlkXCI6XCJTUlhDTEFTRVwifSx7XCJ2YWx1ZVwiOlwiSjEwMDEwMSBaQVBBVE9TIEhPTUJSRVwiLFwiaWRcIjpcIlNSWERFU0NDTEFTRVwifSx7XCJ2YWx1ZVwiOlwiSjEwMDFcIixcImludGVybmFsSWRcIjpcIjc4MFwiLFwiaWRcIjpcIlNSWFNVQkxJTkVBXCJ9LHtcImlkXCI6XCJTUlhERVNDU0xJTlwiLFwidmFsdWVcIjpcIkoxMDAxIFpBUEFUT1MgSE9NQlJFXCJ9LHtcImludGVybmFsSWRcIjpcIjc2OFwiLFwiaWRcIjpcIlNSWExJTkVBXCIsXCJ2YWx1ZVwiOlwiSjEwXCJ9LHtcImlkXCI6XCJTUlhERVNDTElOXCIsXCJ2YWx1ZVwiOlwiSjEwIENBTFpBRE9cIn0se1wiaWRcIjpcIlNSWFBWRU5UQUNMXCIsXCJ2YWx1ZVwiOlwiMTU5OTkwXCJ9LHtcImlkXCI6XCJDVElQX0lNUFRPX0NMXCIsXCJ2YWx1ZVwiOlwiSVZBIDE5JVwifSx7XCJ2YWx1ZVwiOlwiNjM0NTYsOTU3XCIsXCJpZFwiOlwiU1JYQ09TVE9DTFwifSx7XCJpZFwiOlwiREVGQVVMVFVQQ0VBTl9DTFwiLFwidmFsdWVcIjpcIjYyODE3NTQ2NDUzNVwifSx7XCJ2YWx1ZVwiOlwiNjI4MTc1NDY0NTM1XCIsXCJpZFwiOlwiU1JYVVBDRUFOXCJ9LHtcImlkXCI6XCJwdWJsaXNoZWRBdGdcIixcInZhbHVlXCI6XCJ0cnVlXCJ9LHtcInZhbHVlXCI6XCJ0cnVlXCIsXCJpZFwiOlwiU29sZE9ubGluZVwifSx7XCJ2YWx1ZVwiOlwiMjAyMS0wNC0yNSAwOTozMTozOVwiLFwiaWRcIjpcImxhc3RVcGRhdGVcIn0se1widmFsdWVcIjpcImZhbHNlXCIsXCJpZFwiOlwicHVibGlzaGVkQ29ubmVjdFwifSx7XCJpZFwiOlwicnR2RGV2TWFzaXZhXCIsXCJ2YWx1ZVwiOlwiZmFsc2VcIn0se1widmFsdWVcIjpcImZhbHNlXCIsXCJpZFwiOlwicnR2RGV2VW5pdGFyaWFcIn0se1widmFsdWVcIjpcIjIwMjEtMDQtMTkgMDA6MDA6MDBcIixcImlkXCI6XCJsYXN0UmVjZWlwdERhdGVcIn0se1wiaW50ZXJuYWxJZFwiOlwiMTM5NDEwNjVcIixcImlkXCI6XCJJRF9FU1RJTE9cIixcInZhbHVlXCI6XCIxMTg4MzcyMlwifSx7XCJpZFwiOlwiREVTQ19FU1RJTE9cIixcInZhbHVlXCI6XCJDU1BSSU5HLTEzMjA2NjM2LTMwMDQtNDAyMC0xLTQxMFwifSx7XCJpZFwiOlwiSURfU0tVX09SSUdJTkFMXCIsXCJ2YWx1ZVwiOlwiMTE4ODM3MjlcIn0se1widmFsdWVcIjpcInRydWVcIixcImlkXCI6XCJTb2xkSW5TdG9yZXNcIn0se1widmFsdWVcIjpcInRydWVcIixcImlkXCI6XCJhdmFpbGFibGVGb3JQaWNrdXBJblN0b3JlXCJ9LHtcInZhbHVlXCI6XCJ0cnVlXCIsXCJpZFwiOlwiYXZhaWxhYmxlRm9yU2hpcFRvU3RvcmVcIn0se1wiaWRcIjpcImNoYW5uZWxUeXBlXCIsXCJ2YWx1ZVwiOlwiXCJ9LHtcImlkXCI6XCJ1bml0Q29zdFwiLFwidmFsdWVcIjpcIjYzNDU2LDk1N1wifSx7XCJpZFwiOlwibG9uZ0Rlc2NyaXB0aW9uXCIsXCJ2YWx1ZVwiOlwiQ1NQUklORy0xMzIwNjYzNi0zMDA0LTQwMjAtMS00MTAtNDFDQUxMIElUIFNQQ09OTkVSNDEwNDFcIn0se1wiaWRcIjpcIml0ZW1Db2xvclwiLFwidmFsdWVcIjpcIjQxMCBcIn0se1widmFsdWVcIjpcIjExODgzNzIyNDEwNTBcIixcImlkXCI6XCJzdHlsZVNlY3VlbmNlXCJ9LHtcInZhbHVlXCI6XCJDcmVhdGVPclVwZGF0ZVwiLFwiaWRcIjpcIm1hcmtGb3JEZWxldGlvblwifSx7XCJ2YWx1ZVwiOlwiQ09QXCIsXCJpZFwiOlwiY3VycmVuY3lDb2RlXCJ9LHtcImlkXCI6XCJkaW1lbnNpb25VT01cIixcInZhbHVlXCI6XCJNVFJcIn0se1widmFsdWVcIjpcIk1UUjNcIixcImlkXCI6XCJ2b2x1bWVVT01cIn0se1wiaWRcIjpcInN1cHBsaWVySXRlbUJhcmNvZGVcIixcInZhbHVlXCI6XCJGXCJ9LHtcImlkXCI6XCJzdGF0dXNDb2RlXCIsXCJ2YWx1ZVwiOlwiMFwifSx7XCJ2YWx1ZVwiOlwiMVwiLFwiaWRcIjpcImdvb2RzUXVhbnRpdHlcIn0se1wiaWRcIjpcIml0ZW1TZWFzb25cIixcInZhbHVlXCI6XCJQVlwifSx7XCJpZFwiOlwiY29zdFR5cGVcIixcInZhbHVlXCI6XCIxXCJ9LHtcInZhbHVlXCI6XCJ1bml0XCIsXCJpZFwiOlwidW9tXCJ9LHtcInZhbHVlXCI6XCIxXCIsXCJpZFwiOlwic2NhblF1YW50aXR5XCJ9LHtcImlkXCI6XCJTbG90TWlzY2VsbGFuZW91czFcIixcInZhbHVlXCI6XCIwMDBcIn0se1wiaWRcIjpcIlNsb3RNaXNjZWxsYW5lb3VzMlwiLFwidmFsdWVcIjpcIkFcIn0se1wiaWRcIjpcIlZlbG9jaXR5Q29kZVwiLFwidmFsdWVcIjpcIkFcIn1dLFwiaW1hZ2VcIjpbe1widmlld051bWJlclwiOjEsXCJ1cmxcIjpcImh0dHBzOi8vZmFsYWJlbGxhLnNjZW5lNy5jb20vaXMvaW1hZ2UvRmFsYWJlbGxhQ08vMTE4ODM3MjlcIn1dfV0sXCJtZXRhZGF0YVwiOntcImlkXCI6MzYsXCJuYW1lXCI6XCJTQU4gRElFR09cIixcImZhbnRhc3lOYW1lXCI6XCJTYW4gRGllZ28gLSBNZWRlbGzDrW5cIixcImNvdW50cnlDb2RlXCI6XCJDT1wiLFwiYWRkcmVzczFcIjpcIlRJRU5EQSBTQU4gRElFR09cIixcImFkZHJlc3MyXCI6XCJDYXJyZXJhIDQzIE7Dum1lcm8gMzYgLSA0ICBDQyBTQU5ESUVHT1wiLFwiY2l0eVwiOlwiTUVERUxMSU5cIixcInN0YXRlXCI6XCJBTlRJT1FVSUFcIixcImxhdGl0dWRlXCI6XCI2LDE0MDVcIixcImxvbmdpdHVkZVwiOlwiNiwxNDA1XCIsXCJwb3N0YWxjb2RlXCI6XCJcIixcInR5cGVGYWNpbGl0eVwiOlwiQ3VzdG9tZXIgU3RvcmVcIixcInBob25lTnVtYmVyXCI6XCIxMTFcIixcInNlcnZpY2VzXCI6W3tcInNlcnZpY2VOYW1lXCI6XCJCYW5jbyBGYWxhYmVsbGFcIixcInZhbHVlXCI6dHJ1ZX0se1wic2VydmljZU5hbWVcIjpcIkZhbGFiZWxsYSBDb25uZWN0XCIsXCJ2YWx1ZVwiOnRydWV9LHtcInNlcnZpY2VOYW1lXCI6XCJTZXJ2aWNpbyBhbCBDbGllbnRlXCIsXCJ2YWx1ZVwiOnRydWV9LHtcInNlcnZpY2VOYW1lXCI6XCJDTVJcIixcInZhbHVlXCI6dHJ1ZX0se1wic2VydmljZU5hbWVcIjpcIkNsdWIgRGVjb1wiLFwidmFsdWVcIjpmYWxzZX0se1wic2VydmljZU5hbWVcIjpcIlZpYWplcyBGYWxhYmVsbGFcIixcInZhbHVlXCI6dHJ1ZX0se1wic2VydmljZU5hbWVcIjpcIk5vdmlvc1wiLFwidmFsdWVcIjp0cnVlfSx7XCJzZXJ2aWNlTmFtZVwiOlwiQ2x1YiBCZWLDqVwiLFwidmFsdWVcIjp0cnVlfSx7XCJzZXJ2aWNlTmFtZVwiOlwiQ2FuamUgUHVudG9zIENNUlwiLFwidmFsdWVcIjp0cnVlfSx7XCJzZXJ2aWNlTmFtZVwiOlwiU2VndXJvcyBGYWxhYmVsbGFcIixcInZhbHVlXCI6dHJ1ZX0se1wic2VydmljZU5hbWVcIjpcIkp1YW4gVmFsZGV6XCIsXCJ2YWx1ZVwiOnRydWV9LHtcInNlcnZpY2VOYW1lXCI6XCJXaSBGaVwiLFwidmFsdWVcIjp0cnVlfSx7XCJzZXJ2aWNlTmFtZVwiOlwiQ29tcHJhIE9ubGluZSBSZXRpcmEgZW4gVGllbmRhXCIsXCJ2YWx1ZVwiOnRydWV9LHtcInNlcnZpY2VOYW1lXCI6XCJTb3BvcnRlIEV4cHJlc3NcIixcInZhbHVlXCI6dHJ1ZX1dLFwiaW1hZ2VzTGlua1wiOlt7XCJpbWFnZU5hbWVcIjpcImxpbmtHZW9cIixcInZhbHVlXCI6XCJodHRwOi8vczdkNS5zY2VuZTcuY29tL2lzL2ltYWdlL0ZhbGFiZWxsYUNPL1JFVF9Db2xvbWJpYV9TYW5EaWVnb19NZWRlbGxpbkI_JEFwcF9NYXBhUmV0JFwifSx7XCJpbWFnZU5hbWVcIjpcImxpbmtQaG90b1wiLFwidmFsdWVcIjpcImh0dHA6Ly93d3cuZmFsYWJlbGxhLmNvbS5jby9zdGF0aWMvc2l0ZS9jb21tb24vSEVUL3RpZW5kYS1TYW5EaWVnby5qcGdcIn1dfX0sXCJxdWVyeVwiOntcInNhbGVzRXhlY3V0aXZlQ29kZUlkXCI6W10sXCJjdXN0b21lcm9yZGVySWRcIjpcIlwiLFwiZXZlbnRJZFwiOlwiXCIsXCJzdG9yZUlkXCI6MzYsXCJ0ZXJtaW5hbElkXCI6OCxcInNlcXVlbmNlSWRcIjo5Mjc5LFwicGF5bWVudElkXCI6W10sXCJzdGF0dXNJZFwiOjIsXCJ0cmFuc2FjdGlvbklkXCI6MSxcImRvY3VtZW50SWRcIjpbMTIwMDAwMTY3NjM5XSxcInB1cmNoYXNlRGF0ZVwiOlwiXCIsXCJwcm9jZXNzRGF0ZVwiOjIwMjEwNjAzLFwiaWRlbnRpdHlEb2N1bWVudE51bWJlclwiOlwiMDAwMVwiLFwiZW1haWxcIjpcIlwiLFwib3JwaGFuTGFiZWxcIjpbXSxcInNhbGVEb2N1bWVudFwiOlwiXCIsXCJrZXlcIjpcIjM2LTgtOTI3OVwiLFwia2V5RGF0ZVwiOlwiMzYtOC05Mjc5LVwifX0ifQ=="
        
        base64_bytes = str.encode(base64_message)
        #base64_bytes = base64_message.encode('ascii')
        message_bytes = base64.urlsafe_b64decode(base64_bytes)
        #message = message_bytes.decode('ascii')
        #message = base64_bytes.decode('ascii')

        #enc = base64.b64decode(base64_message)
        
        print(message_bytes.decode('utf-8'))


class CustomParsingPayment(beam.DoFn):

    def process(self, element: apache_beam.io.gcp.pubsub.PubsubMessage, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        data:bytes = element.data
        data = json.loads(data.decode("utf-8"))
        #print(element.attributes)
        attributes_parsed = parseAttributes(element)
        
        new_parsed = attributes_parsed
        new_parsed["transaction_date_dt"] = parseDate(data["transactionDate"])
        new_parsed["store_id"] = data["storeId"]
        new_parsed["terminal_number_num"] = data["terminalNumber"]
        new_parsed["sequence_number_num"]  = data["sequenceNumber"]
        new_parsed["transaction_code_cd"] = data["transactionCode"]
        new_parsed["transaction_status_cd"] = data["transactionStatus"]
        new_parsed["transaction_set_code_cd"] = data["transactionSetCode"]
        new_parsed["transaction_state_v_desc"] = data["transactionStatev"]

        # Payment Details Record
        new_payment_details = []
        
        for payment_detail in data["paymentDetails"]:
            
            new_payment_detail = {}
            new_payment_detail["doc_type_numbertbk_num"] = payment_detail["docTypeNumberTBK"]
            new_payment_detail["send_messagetbk_tm"] = parseTime(payment_detail["sendMessageTBK"].removeprefix('00'))
            new_payment_detail["received_messagetbk_tm"] = parseTime(payment_detail["receiveMesgTBK"].removeprefix('00'))
            new_payment_detail["unique_number_num"] = payment_detail["uniqueNumber"]
            new_payment_detail["card_brand_cd"] = payment_detail["cardBrand"]
            #print(payment_detail["sendMessageTBK"][2:8])
            #print(payment_detail["sendMessageTBK"].removeprefix('00'))
            #print(parseTime(payment_detail["sendMessageTBK"].removeprefix('00')))
            
            for sale_amount in payment_detail["salesAmount"]:
                if sale_amount["description"] == 'transbankAmount':
                    new_payment_detail["transbank_amount_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'chargeCode':
                    new_payment_detail["charge_code_cd"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalAmountCash':
                    new_payment_detail["total_amount_cash_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalNetNCND':
                    new_payment_detail["total_net_ncnd_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalIvaNCND':
                    new_payment_detail["total_iva_ncnd_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalAditionalIvaNCND':
                    new_payment_detail["total_aditional_iva_ncnd_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalNetReverse':
                    new_payment_detail["total_net_reverse_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalIvaReverse':
                    new_payment_detail["total_iva_reverse_amt"] = sale_amount["value"]
                elif sale_amount["description"] == 'totalAditionalIvaNCNDReverse':
                    new_payment_detail["total_aditional_iva_ncnd_reverse_amt"] = sale_amount["value"]
            
            new_payment_details.append(new_payment_detail)

        new_parsed["payment_details_rec"] = new_payment_details
        new_parsed["partition_date_tmst"] = timestamp.to_rfc3339()
        
        yield new_parsed


class CustomParsingTransaction(beam.DoFn):

    def process(self, element: apache_beam.io.gcp.pubsub.PubsubMessage, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        data:bytes = element.data
        data = json.loads(data.decode("utf-8"))
        #print(element.attributes)
        attributes_parsed = parseAttributes(element)
        
        new_parsed = attributes_parsed
        new_parsed["store_id"] = data["storeId"]

        yield new_parsed


class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """

    def to_runner_api_parameter(self, unused_context):
        # Not very relevant, returns a URN (uniform resource name) and the payload
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        Simple processing function to parse the data and add a timestamp
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        parsed = json.loads(element.decode("utf-8"))
        
        new_parsed = {}
        #∫new_parsed["timestamp"] = timestamp.to_rfc3339()

        new_parsed["store_id"] = parsed["storeId"]
        new_parsed["terminal_number"] = parsed["terminalNumber"]
        new_parsed["transaction_date"] = parseDate(parsed["transactionDate"])
        new_parsed["transaction_code"] = parsed["transactionCode"]
        new_parsed["transaction_status"] = parsed["transactionStatus"]
        new_parsed["sequence_number"]  = parsed["sequenceNumber"]
        new_parsed["country_flag"] = parsed["countryFlag"]
        new_parsed["buy_date"] = parseDate(parsed["buyDate"]) 
        new_parsed["transaction_hour"] = parsed["transactionHour"]
        new_parsed["cashier_number"] = parsed["cashierNumber"]
        new_parsed["partition_date"] = timestamp.to_rfc3339()

        new_parsed_bill_details = []
        
        for bill_detail in parsed["billsDetails"]:
            new_bill_detail = {}
            new_bill_detail["sii_ticket_number"] = bill_detail["siiTicketNumber"]
            new_bill_detail["identity_number_document"] = bill_detail["identityNumberDocument"]
            new_parsed_bill_details.append(new_bill_detail)

    
        new_parsed["bills_details"] = new_parsed_bill_details

        new_parsed_products_details = []

        for product_detail in parsed["productsDetails"]:

            single_amount = {}
            
            for sale_amount in product_detail["salesAmount"]:
                if sale_amount["description"] == 'saleAmount':
                    single_amount["sale_amount"] = sale_amount["value"]
                elif sale_amount["description"] == 'itemBaseCostWithoutTaxes':
                    single_amount["item_base_cost_without_taxes"] = sale_amount["value"]
                elif sale_amount["description"] == 'netAmount':
                    single_amount["net_amount"] = sale_amount["value"]
                elif sale_amount["description"] == 'netAmountCreditNote':
                    single_amount["net_amount_credit_note"] = sale_amount["value"]


            new_product_detail = {}
            #new_product_detail["short_sku_descrip"] = "No se lo que le pasa" #product_detail["shortSkuDescrip"]
            new_product_detail["quantity"] = product_detail["quantity"]
            new_product_detail["transaction_change_type"] = product_detail["transactionChangeType"]
            new_product_detail["upc_number"] = product_detail["upcNumber"]
            new_product_detail["product_description"] = product_detail["productDescription"]
            new_product_detail["pos_description"] = product_detail["posDescription"]
            new_product_detail["productType"] = product_detail["productType"]
            new_product_detail["product_sku_mark"] = product_detail["productSkuMark"]
            new_product_detail["sku"] = product_detail["sku"]
            new_product_detail["internal_id"] = product_detail["internalId"]
            new_product_detail["sale_amount"] = single_amount["sale_amount"]
            new_product_detail["item_base_cost_without_taxes"] = single_amount["item_base_cost_without_taxes"]
            new_product_detail["net_amount"] = single_amount["net_amount"]
            new_product_detail["net_amount_credit_note"] = single_amount["net_amount_credit_note"]

            new_parsed_products_details.append(new_product_detail)
            #print(new_product_detail)

        new_parsed["products_details"] = new_parsed_products_details

        new_parsed["document_code"] = parsed["documentCode"]
        new_parsed["deleted_products_number"] = parsed["deletedProductsNumber"]
        new_parsed["diminish_products_number"] = parsed["diminishProductsNumber"]
        new_parsed["transaction_set_code"] = parsed["transactionSetCode"]
        new_parsed["transaction_statev"] = parsed["transactionStatev"]

        yield new_parsed

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

        #storage_data = (
        #    streaming_data 
        #    | 'Map result PubsubMessage' >> beam.Map(lambda elem: elem.data) # .data because is a PubSubMessage
        #    | 'Map result Json loads' >> beam.Map( lambda elem: json.loads(elem.decode('utf-8')) )
        #    | 'Transform base64' >> beam.ParDo( TransformBase() )
        #    #| 'Storage Data' >> beam.ParDo(StorageGcpNew('output_directory/') )
        #)

        #parsing_message_transaction = ( 
        #    streaming_data | 'Parsing Transaction' >> beam.ParDo(CustomParsingTransaction())
        #)

        #parsing_message_payment = ( 
        #    streaming_data | 'Parsing Payment' >> beam.ParDo(CustomParsingPayment())
        #)

        write_bq_transaction = (
            parsing_data | 'Write BigQuery Transaction' >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )

        #write_bq_payment = (
        #    parsing_message_payment | 'Write BigQuery Paymenth' >> beam.io.WriteToBigQuery(
        #        output_table_pay,
        #        schema=output_schema_pay,
        #        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #        additional_bq_parameters=additional_bq_parameters_pay
        #    )
        #)

        #write_bq_payment = (
        #    
        #)

        #
        #(
        #    p
        #    | "Step 1  - ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
        #        subscription=known_args.input_subscription, timestamp_attribute=None #withAtributtes
        #    )
        #    #| "Step 2 - Storage in bucket" >> beam.ParDo(StorageGcp("{}/ALL/sales/".format(OUTPUT_STORAGE)))
        #    | "Step 3 - CustomParse" >> beam.ParDo(CustomParsing())
        #    | "Step 4 - WriteToBigQuery" >> beam.io.WriteToBigQuery(
        #        known_args.output_table,
        #        schema=known_args.output_schema,
        #        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #        additional_bq_parameters=additional_bq_parameters
        #    )
        #)

    
if __name__ == "__main__":
    run()