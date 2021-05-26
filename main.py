import argparse
import os
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

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

BIGQUERY_SCHEMA = {
    "fields": [
        {  "name": "store_id", "type": "NUMERIC"            } ,
        {  "name": "terminal_number", "type": "NUMERIC"     } ,  
        {  "name": "transaction_date", "type": "STRING"     } ,
        {  "name": "transaction_code", "type": "NUMERIC"    } , 
        {  "name": "transaction_status", "type": "NUMERIC"  } , 
        {  "name": "sequence_number", "type": "NUMERIC"     } , 
        {  "name": "country_flag", "type": "STRING"         } , 
        {  "name": "buy_date", "type": "STRING"             } , 
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
                { "name": "internal_id", "type": "STRING"               }
            ]
        }
    ]
}

additional_bq_parameters = { "timePartitioning": {"type": "DAY", "field": "partition_date"}}


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
        new_parsed["transaction_date"] = parsed["transactionDate"]
        new_parsed["transaction_code"] = parsed["transactionCode"]
        new_parsed["transaction_status"] = parsed["transactionStatus"]
        new_parsed["sequence_number"]  = parsed["sequenceNumber"]
        new_parsed["country_flag"] = parsed["countryFlag"]
        new_parsed["buy_date"] = parsed["buyDate"]
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
            new_parsed_products_details.append(new_product_detail)
            #print(new_product_detail)

        new_parsed["products_details"] = new_parsed_products_details

        #print(new_parsed)
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
        (
            p
            | "Step 1  - ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=known_args.input_subscription, timestamp_attribute=None
            )
            #| "Step 2 - Storage in bucket"
            | "Step 3 - CustomParse" >> beam.ParDo(CustomParsing())
            | "Step 4 - WriteToBigQuery" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=known_args.output_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=additional_bq_parameters
            )
        )

    
if __name__ == "__main__":
    run()