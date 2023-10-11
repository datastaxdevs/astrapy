## AstraPy

[![Actions Status](https://github.com/datastax/astrapy/workflows/Tests/badge.svg)](https://github.com/datastax/astrapy/actions)

AstraPy is a Pythonic SDK for [DataStax Astra](https://astra.datastax.com) and [Stargate](https://stargate.io/)

### Resources

- [DataStax Astra](https://astra.datastax.com)
- [Stargate](https://stargate.io/)

### Getting Started

Install AstraPy

```shell
pip install astrapy
```

Setup your Astra client

```python
from astrapy.base import AstraClient

astra_client = AstraClient(
        astra_database_id=ASTRA_DB_ID,
        astra_database_region=ASTRA_DB_REGION,
        astra_application_token=ASTRA_DB_APPLICATION_TOKEN,
    )

vector_client = AstraVectorClient(astra_client=astra_client)
test_namespace = vector_client.namespace(ASTRA_DB_KEYSPACE)
test_collection = vector_client.namespace(ASTRA_DB_KEYSPACE).collection(TEST_COLLECTION_NAME)
```

####Getting started
Check out the [notebook](https://colab.research.google.com/github/synedra/astra_vector_examples/blob/main/notebook/vector.ipynb#scrollTo=f04a1806) which has examples for finding and inserting information into the database, including vector commands.

Take a look at the [vector tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_vector.py) and the [collection tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_collections.py) for specific endpoint examples.

#### Using the Ops Client

You can use the Ops client to work the with Astra DevOps API. Check the [devops tests](https://github.com/datastax/astrapy/blob/master/tests/astrapy/test_devops.py)

```python
# astra_client created above
# create a keyspace using the Ops API
astra_client.ops.create_keyspace(database=ASTRA_DB_ID, keyspace=KEYSPACE_NAME)
```

#### Using the REST Client

You can use the REST client to work with the Astra REST API. [API Reference](https://docs.datastax.com/en/astra/docs/_attachments/restv2.html#tag/Data)
