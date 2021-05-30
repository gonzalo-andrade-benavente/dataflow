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
        {  "name": "event_type_name", "type": "STRING"            } ,
        {  "name": "entity_type_name", "type": "STRING"            } ,
        {  "name": "country_cd", "type": "STRING"            } ,
        {  "name": "channel_name", "type": "STRING"            } ,

        {  "name": "store_id", "type": "NUMERIC"            } ,
        {  "name": "terminal_number", "type": "NUMERIC"     } ,  
        {  "name": "transaction_date", "type": "DATE"     } ,
        {  "name": "transaction_code", "type": "NUMERIC"    } , 
        {  "name": "transaction_status", "type": "NUMERIC"  } , 
        {  "name": "sequence_number", "type": "NUMERIC"     } , 
        {  "name": "country_flag", "type": "STRING"         } , 
        {  "name": "buy_date", "type": "DATE"             } , 
        {  "name": "transaction_hour", "type": "STRING"     } , 
        {  "name": "cashier_number", "type": "STRING"       } , 
        {  "name": "partition_date", "type": "TIMESTAMP"    } ,
        {
            "name": "bills_details",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {   "name": "sii_ticket_number", "type": "STRING"           },
                {   "name": "identity_number_document", "type": "STRING"    }
            ]
        },
        {
            "name": "products_details",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields" : [
               # { "name": "short_sku_descrip", "type": "STRING"         }, #No se repite, provoca error.
                { "name": "quantity", "type": "NUMERIC"                 },
                { "name": "transaction_change_type", "type": "STRING"   },
                { "name": "upc_number", "type": "STRING"                },
                { "name": "product_description", "type": "STRING"       },
                { "name": "pos_description", "type": "STRING"           },
                { "name": "productType", "type": "STRING"               },
                { "name": "product_sku_mark", "type": "STRING"          },
                { "name": "sku", "type": "STRING"                       },
                { "name": "internal_id", "type": "STRING"               },
                { "name": "sale_amount", "type": "NUMERIC"               },
                { "name": "item_base_cost_without_taxes", "type": "NUMERIC" },
                { "name": "net_amount", "type": "NUMERIC"               },
                { "name": "net_amount_credit_note", "type": "NUMERIC"   },
                { "name": "product_iva_1", "type": "NUMERIC" },
                { "name": "amount_iva_1", "type": "NUMERIC"               },
                { "name": "iva_percent", "type": "NUMERIC"   }
            ]
        },  
        { "name": "document_code", "type": "STRING"   },
        #
        #{
        #    "name": "payment_details",
        #    "type": "RECORD",
        #    "mode": 'REPEATED',
        #    "fields" : [
        #    
        #    ]
        #}
        { "name": "deleted_products_number", "type": "STRING"   },
        { "name": "diminish_products_number", "type": "STRING"   },
        { "name": "transaction_set_code", "type": "STRING"   },
        { "name": "transaction_statev", "type": "STRING"   }

    ]
}

additional_bq_parameters = { "timePartitioning": {"type": "DAY", "field": "partition_date"}}

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
        #        { "name": "transbank_amount_amt", "type": "NUMERIC" },
        #        { "name": "charge_code_cd", "type": "STRING" },
        #        { "name": "total_amount_cash_amt", "type": "NUMERIC" },
        #        { "name": "total_net_ncnd_amt", "type": "NUMERIC" },
        #        { "name": "total_iva_ncnd_amt", "type": "NUMERIC" },
        #        { "name": "total_aditional_iva_ncnd_amt", "type": "NUMERIC" },
        #        { "name": "total_net_reverse_amt", "type": "NUMERIC"  },
        #        { "name": "total_iva_reverse_amt", "type": "NUMERIC" },
        #        { "name": "total_aditional_iva_ncnd_reverse_amt", "type": "NUMERIC"  },
            ]
        },
        {  "name": "partition_date_tmst", "type": "TIMESTAMP" } , 
    ]
}

def parseDate(string_date):
    return datetime.datetime.strptime(string_date, "%Y%m%d").strftime("%Y-%m-%d")

def parseTime(string_time):
    return datetime.datetime.strptime(string_time, "%H%M%S").strftime("%H:%M:%S")

def parseAttributes(attributes):
    attributes_parsed = {}
    attributes_parsed["event_id"] = attributes.attributes["eventId"]
    attributes_parsed["event_type_name"] = attributes.attributes["eventType"]
    attributes_parsed["entity_type_name"] = attributes.attributes["entityType"]
    attributes_parsed["country_cd"] = attributes.attributes["country"].lower()
    attributes_parsed["channel_name"] = attributes.attributes["channel"]
    return attributes_parsed

class StorageGcp(beam.DoFn):
    def __init__(self, output_path):
        self.output_path = output_path
    
    def process(self, batch, window=beam.DoFn.WindowParam):
        print('hola mundo')

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

        #parsing_message_transaction = ( 
        #    streaming_data | 'Parsing Transaction' >> beam.ParDo(CustomParsingTransaction())
        #)

        parsing_message_payment = ( 
            streaming_data | 'Parsing Payment' >> beam.ParDo(CustomParsingPayment())
        )

        #write_bq_transaction = (
        #    parsing_message_transaction | 'Write BigQuery Transaction' >> beam.io.WriteToBigQuery(
        #        known_args.output_table,
        #        schema=known_args.output_schema,
        #        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #        additional_bq_parameters=additional_bq_parameters
        #    )
        #)

        write_bq_payment = (
            parsing_message_payment | 'Write BigQuery Paymenth' >> beam.io.WriteToBigQuery(
                output_table_pay,
                schema=output_schema_pay,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters_pay
            )
        )

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