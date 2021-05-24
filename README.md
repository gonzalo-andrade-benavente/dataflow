# Resume

Dataflow made in apache-beam with deploy in GCP. 

Features Dataflow
* Public message in GCP-Topic.
* Dataflow pull the message and store in GCP-Storage.
* Process and parse the message to persist en GCP-BigQuery.  


# Test in local

Create a directory to virtualenv
`````
$ user@equip directory % mkdir venv-df
`````

Create and activate virtualenv

`````
$ user@equip directory % virtualenv venv-df/
$ user@equip directory %. venv-df/bin/activate
$ (venv-df) user@equip directory %

`````

Python Libraries

`````
$ (venv-df) user@equip directory % pip3 install "apache-beam[gcp]"
$ (venv-df) user@equip directory % pip3 install python-dotenv
`````

Execution simple

`````
$ (venv-df) user@equip directory % python3 main.py
`````


# Test in gcp
In locally you need python version older tan 3.9.0. I try to change but i can, so copy the code in gcp console and execute from there.
