contador=1
while [ $contador -le 1000 ]
do
     gcloud pubsub subscriptions pull sub-fal-corp-stro-pose --project=txd-sales-prd --auto-ack --format=json > data-example2/sales1.json
     python3 test_publish_msg.py
     echo "Este es el mensaje número" $contador
     ((contador++))
done



