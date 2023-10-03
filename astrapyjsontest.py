from astrapyjson.config.client import create_astra_client
import astrapyjson

# from astrapyjson import astra_vector_client, astra_serverless_client
from dotenv import load_dotenv
import os, json

import http

http.client.HTTPConnection.debuglevel = 1

load_dotenv()

astra_client = create_astra_client(
    astra_database_id=os.environ["ASTRA_DB_ID"],
    astra_database_region=os.environ["ASTRA_DB_REGION"],
    astra_application_token=os.environ["ASTRA_DB_APPLICATION_TOKEN"],
)

# API Key
# astra_serverless_client.init(token=astra_token, database_id=database_id)
# astra_vector_client.init(token=astra_token, database_id=database_id)
# astra_vector_client = init(dbid="name", dbregion="us-east-1", token="token")

# astra_vector_client.create_vector_collection(name="name", size=5) # Options: function
my_namespace = astra_client.collections.namespace("vector")
# my_collection = my_namespace.create_collection(name="vanillabean")
returnvalue = my_namespace.create_vector_collection(name="new_vector", size=10)
print(returnvalue)
newreturnvalue = my_namespace.delete_collection(name="new_vector")
print(newreturnvalue)
returnvalue = my_namespace.get_collections()
print(returnvalue)
