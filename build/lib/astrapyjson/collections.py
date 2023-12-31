# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from astrapy.base import http_methods
from astrapy.base import AstraClient
import logging
import json

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 100
DEFAULT_BASE_PATH = "/api/json/v1"
print(DEFAULT_BASE_PATH)


class AstraCollection:
    def __init__(self, astra_client=None, namespace_name=None, collection_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.collection_name = collection_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}/{collection_name}"
        self.namespace_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def _post(self, path=None, document=None):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}",
            json_data=document,
        )

    def find(self, filter=None, options=None):
        options = {} if options is None else options
        request_params = {"filter": json.dumps(query), "page-size": DEFAULT_PAGE_SIZE}
        request_params.update(options)
        response = self.astra_client.request(
            method=http_methods.GET, path=self.base_path, url_params=request_params
        )
        if isinstance(response, dict):
            return response
        return None

    def find_one(self, query=None, options=None):
        options = {} if options is None else options
        request_params = {"where": json.dumps(query), "page-size": 1}
        request_params.update(options)
        response = self._get(path=None, options=request_params)
        if response is not None:
            keys = list(response.keys())
            if len(keys) == 0:
                return None
            return response[keys[0]]
        return None

    def create(self, path=None, document=None):
        if path is not None:
            return self._put(path=path, document=document)
        return self.astra_client.request(
            method=http_methods.POST, path=self.base_path, json_data=document
        )

    def update(self, path, document):
        return self.astra_client.request(
            method=http_methods.PATCH,
            path=f"{self.base_path}/{path}",
            json_data=document,
        )

    def replace(self, path, document):
        return self._put(path=path, document=document)

    def delete(self, path):
        return self.astra_client.request(
            method=http_methods.POST, path=f"{self.base_path}/{path}"
        )

    def batch(self, documents=None, id_path=""):
        if id_path == "":
            id_path = "documentId"
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/batch",
            json_data=documents,
            url_params={"id-path": id_path},
        )

    def push(self, path=None, value=None):
        json_data = {"operation": "$push", "value": value}
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}/function",
            json_data=json_data,
        )
        return res.get("data")

    def pop(self, path=None):
        json_data = {"operation": "$pop"}
        res = self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/{path}/function",
            json_data=json_data,
        )
        return res.get("data")


class AstraNamespace:
    def __init__(self, astra_client=None, namespace_name=None):
        self.astra_client = astra_client
        self.namespace_name = namespace_name
        self.base_path = f"{DEFAULT_BASE_PATH}/{namespace_name}"

    def collection(self, collection_name):
        return AstraCollection(
            astra_client=self.astra_client,
            namespace_name=self.namespace_name,
            collection_name=collection_name,
        )

    def get_collections(self):
        response = self.astra_client.request(
            method=http_methods.PATH, path=self.namespace_path
        )
        if isinstance(response, dict):
            return reponse.get("data")

    def create_collection(self, name=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/collections",
            json_data={"name": name},
        )

    def create_vector_collection(self, data={}):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}/collections",
            json_data=data,
        )

    def delete_collection(self, name=""):
        return self.astra_client.request(
            method=http_methods.POST,
            path=f"{self.base_path}",
            json_data={"deleteCollection": {"name": name}},
        )


class AstraDocumentClient:
    def __init__(self, astra_client=None):
        self.astra_client = astra_client

    def namespace(self, namespace_name):
        return AstraNamespace(
            astra_client=self.astra_client, namespace_name=namespace_name
        )


def create_client(
    astra_database_id=None,
    astra_database_region=None,
    astra_application_token=None,
    base_url=None,
    auth_base_url=None,
    username=None,
    password=None,
    debug=False,
):
    astra_client = AstraClient(
        astra_database_id=astra_database_id,
        astra_database_region=astra_database_region,
        astra_application_token=astra_application_token,
        base_url=base_url,
        auth_base_url=auth_base_url,
        username=username,
        password=password,
        debug=debug,
    )
    return AstraDocumentClient(astra_client=astra_client)
