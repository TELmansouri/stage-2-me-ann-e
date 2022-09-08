import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import pandas as pd
import numpy as np
import json
from datetime import datetime

class Generic:
    # Creation of a constructor
    def __init__(self, es, name):
        self.es = es,# Reference elastic search
        self.name = name,# Name of the document that will be processed

    dp = pd.read_csv("ATS_FULL_SYNC_XXXX.csv")
    # Creat a new column that's a concatenation between name and serial_number columns
    dp['_id1'] = dp['name'] + dp['serial_number']

    def get_data_from_elastic():
        # query: The elasticsearch query.
        for e in dp['_id1']:
            query = {
                "query": {
                    "match": {
                        "_id": "e"
                    }
                }
            }
            # Scan function to get all the documents whose id match that of the documents imported
            rel = scan(client=es,
                       query=query,
                       scroll='1m',
                       index='my_index',
                       raise_on_error=True,
                       preserve_order=False,
                       clear_scroll=True)
        # Keep response in a list.
        result = list(rel)
        temp = []
        # We need only '_source' and '_id', which has all the fields required.
        # This elimantes the elasticsearch metdata like _id, _type, _index.
        for hit in result:
            temp.append(hit['_source','_id'])
            # Create a dataframe.
            df = pd.DataFrame(temp)
        return df

    df = get_data_from_elastic()

    # Convert to JSON
    dp1 = dp.to_dict('records')
    df1 = df.to_dict('records')

    # Generators enable large datasets won’t have to be loaded into memory to slow down the process. It’s the best way for Python helpers to import Elasticsearch data.
    def generator(dp1):
        for c, line in enumerate(dp1):
            # Yield is a keyword in Python that is used to return from a function without destroying the states of its local variable and when the function is called, the execution starts from the last yield statement.
            yield {
                '_index': 'my_index',
                '_type': '_doc',
                '_id': line.get('_id1'),
                '_source': {
                    'name': line.get('name'),
                    'serial_number': line.get('serial_number'),
                    'eaa_cm_key': line.get('eaa_cm_key'),
                    'eaa_tt_key': line.get('eaa_tt_key'),
                    'eaa_fm_key': line.get('eaa_fm_key'),
                    'class': line.get('class'),
                    'family': line.get('family'),
                }
            }


    def update(dp1):
        # Creat a loop for to creat a body or a source that will be updated
        for c, line in enumerate(dp1):
            source_update = {
                'doc': {
                    'name': line.get('name'),
                    'serial_number': line.get('serial_number'),
                    'eaa_cm_key': line.get('eaa_cm_key'),
                    'eaa_tt_key': line.get('eaa_tt_key'),
                    'eaa_fm_key': line.get('eaa_fm_key'),
                    'class': line.get('class'),
                    'family': line.get('family'),
                }
            }
            # Update the _source of the document using the update function
            try:
                es.update(
                    index='my_index',
                    doc_type='_doc',
                    id='_id1',
                    body=source_update,
            )
            except Exception as err:
                print(err)


    def generator1(dp1,df1):
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        for c, line in enumerate(df1):
            for d, row in enumerate(dp1):
                 if row.get('d') != line.get('c')  :
                     # Yield is a keyword in Python that is used to return from a function without destroying the states of its local variable and when the function is called, the execution starts from the last yield statement.
                     yield {
                         '_index': 'my_index1',
                         '_type': '_doc',
                         '_id': line.get('_id'),
                         '_source': {
                              'name': row.get('name'),
                              'serial_number': row.get('serial_number'),
                              'name_of_changed_field': line.get('c'),
                              'old_value': line.get('c'),
                              'new_value': row.get('d'),
                              'date_of_update': timestamp,
                }
            }


