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

from astrapy.serverless import AstraCollection, AstraJsonClient
from astrapy.vector import AstraVectorClient
from astrapy.base import AstraClient
import uuid
import pytest
import logging
import os
from faker import Faker
import http
import json
import http.client as http_client

logger = logging.getLogger(__name__)
fake = Faker()

# http_client.HTTPConnection.debuglevel = 1

from dotenv import load_dotenv

load_dotenv()

ASTRA_DB_ID = os.environ.get("ASTRA_DB_ID")
ASTRA_DB_REGION = os.environ.get("ASTRA_DB_REGION")
ASTRA_DB_APPLICATION_TOKEN = os.environ.get("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_KEYSPACE = os.environ.get("ASTRA_DB_KEYSPACE")

TEST_COLLECTION_NAME = "test"

cliffu = str(uuid.uuid4())


@pytest.fixture
def test_collection():
    astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )
    json_client = AstraJsonClient(astra_client=astra_client)
    test_collection = json_client.namespace(ASTRA_DB_KEYSPACE).collection(
        TEST_COLLECTION_NAME
    )
    return test_collection


@pytest.fixture
def cliff_uuid():
    return cliffu


@pytest.fixture
def test_namespace():
    astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )
    json_client = AstraJsonClient(astra_client=astra_client)

    return json_client.namespace(ASTRA_DB_KEYSPACE)


@pytest.mark.webtest("should initialize an AstraDB Collections Client")
def test_connect(test_collection):
    assert type(test_collection) is AstraCollection


@pytest.mark.webtest("should create a collection")
def test_create_collection(test_namespace):
    res = test_namespace.create_collection(name="pytest_collection")
    assert res is not None
    res2 = test_namespace.create_collection(name="test_schema")
    assert res2 is not None
    res = test_namespace.create_collection(name="test")
    assert res is not None


@pytest.mark.webtest("should get all collections")
def test_get_collections(test_namespace):
    res = test_namespace.get_collections()
    assert res["status"]["collections"] is not None


@pytest.mark.webtest("should delete a collection")
def test_delete_collection(test_namespace):
    res = test_namespace.delete_collection(name="pytest_collection")
    assert res is not None
    res2 = test_namespace.delete_collection(name="test_schema")
    assert res2 is not None


@pytest.mark.webtest("should create a document")
def test_create_document(test_collection, cliff_uuid):
    json_query = {
        "_id": cliff_uuid,
        "first_name": "Cliff",
        "last_name": "Wicklow",
    }
    test_collection.create(document=json_query)

    document = test_collection.find_one(filter={"_id": cliff_uuid})

    assert document is not None


@pytest.mark.webtest("should create multiple documents")
def test_insert_many(test_collection):
    id_1 = fake.bothify(text="????????")
    id_2 = fake.bothify(text="????????")
    documents = [
        {
            "_id": id_1,
            "first_name": "Dang",
            "last_name": "Son",
        },
        {
            "_id": id_2,
            "first_name": "Yep",
            "last_name": "Boss",
        },
    ]
    res = test_collection.insert_many(documents=documents)
    assert res is not None

    document = test_collection.find(filter={"first_name": "Yep"})
    assert document is not None


@pytest.mark.webtest("should create a subdocument")
def test_create_subdocument(test_collection, cliff_uuid):
    document = test_collection.update_one(
        filter={"_id": cliff_uuid},
        update={"$set": {"addresses.city": "New York", "addresses.state": "NY"}},
    )

    document = test_collection.find_one(filter={"_id": cliff_uuid})

    assert document["document"]["addresses"] is not None


#
@pytest.mark.webtest("should create a document without an ID")
def test_create_document_without_id(test_collection):
    response = test_collection.create(
        document={
            "first_name": "New",
            "last_name": "Guy",
        }
    )
    document = test_collection.find_one(filter={"first_name": "New"})
    assert document["document"]["last_name"] == "Guy"


@pytest.mark.webtest("should update a document")
def test_update_document(test_collection, cliff_uuid):
    test_collection.update_one(
        filter={"_id": cliff_uuid},
        update={"$set": {"first_name": "Dang"}},
    )
    document = test_collection.find_one(filter={"_id": cliff_uuid})
    assert document["document"]["_id"] == cliff_uuid


@pytest.mark.webtest("replace a document")
def test_replace_document(test_collection, cliff_uuid):
    test_collection.find_one_and_replace(
        filter={"_id": cliff_uuid},
        replacement={
            "_id": cliff_uuid,
            "addresses": {
                "work": {
                    "city": "New York",
                    "state": "NY",
                }
            },
        },
    )
    document = test_collection.find_one(filter={"_id": cliff_uuid})
    print(document)

    assert document is not None
    document_2 = test_collection.find_one(
        filter={"_id": cliff_uuid}, projection={"addresses.work.city": 1}
    )

    print("HOME", json.dumps(document_2, indent=4))


@pytest.mark.webtest("should delete a subdocument")
def test_delete_subdocument(test_collection, cliff_uuid):
    response = test_collection.delete_subdocument(id=cliff_uuid, subdoc="addresses")
    document = test_collection.find(filter={"_id": cliff_uuid})
    assert response is not None


@pytest.mark.webtest("should delete a document")
def test_delete_document(test_collection, cliff_uuid):
    response = test_collection.delete(id=cliff_uuid)

    assert response is not None


@pytest.mark.webtest("should find documents")
def test_find_documents(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(
        document={
            "_id": user_id,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Wicklow",
        },
    )
    user_id_2 = str(uuid.uuid4())
    test_collection.create(
        document={
            "_id": user_id_2,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Danger",
        },
    )
    document = test_collection.find(filter={"first_name": f"Cliff-{user_id}"})
    assert document is not None


@pytest.mark.webtest("should find a single document")
def test_find_one_document(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(
        document={
            "_id": user_id,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Wicklow",
        },
    )
    user_id_2 = str(uuid.uuid4())
    test_collection.create(
        document={
            "_id": user_id_2,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Danger",
        },
    )
    document = test_collection.find_one(filter={"first_name": f"Cliff-{user_id}"})
    print("DOCUMENT", document)

    assert document["document"] is not None

    document = test_collection.find_one(filter={"first_name": f"Cliff-Not-There"})
    assert document["document"] == None


@pytest.mark.webtest("should use document functions")
def test_functions(test_collection):
    user_id = str(uuid.uuid4())
    test_collection.create(
        document={
            "_id": user_id,
            "first_name": f"Cliff-{user_id}",
            "last_name": "Wicklow",
            "roles": ["admin", "user"],
        },
    )
    update = {"$pop": {"roles": 1}}
    options = {"returnDocument": "after"}

    pop_res = test_collection.pop(
        filter={"_id": user_id}, update=update, options=options
    )

    doc_1 = test_collection.find_one(filter={"_id": user_id})
    assert doc_1["document"]["_id"] == user_id

    update = {"$push": {"roles": "users"}}
    options = {"returnDocument": "after"}

    test_collection.push(filter={"_id": user_id}, update=update, options=options)
    doc_2 = test_collection.find_one(filter={"_id": user_id})
    assert doc_2["document"]["_id"] == user_id
