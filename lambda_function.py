import json
import boto3
from botocore.config import Config
from Queries import Query, DATABASE_NAME, TABLE_NAME

def lambda_handler(event, context):

    path = event.get('path', '/')

    resource = event.get('resource', '/')

    method = event.get('httpMethod', 'GET')

    query_params = event.get('queryStringParameters', {})

    session = boto3.Session()
    query_client = session.client('timestream-query', config=Config(region_name="us-east-2"))
    q = Query(query_client)

    
 

    # https://github.com/aws-samples/serverless-test-samples/blob/main/python-test-samples/apigw-lambda/events/event.json

    if resource == '/artists' and method == 'GET':
        show = query_params.get('show', None)
        all = "SELECT Artist, COUNT() FROM spinalytics.songs WHERE Show_Name = '"+ show + "' GROUP BY Artist ORDER BY COUNT() DESC LIMIT 10" 
        json_result = q.run_query(all)
        return {

        'statusCode': 200,

        'headers': {
            'Content-Type': 'application/json',

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'

        },
        
        'body': json.dumps(json_result)

    }

    elif resource == '/genres' and method == 'GET':
        show = query_params.get('show', None)
        all = "SELECT Genre, COUNT() FROM spinalytics.songs WHERE Show_Name = '"+ show +"' AND Genre != 'Unknown' GROUP BY Genre ORDER BY COUNT() DESC LIMIT 10"
        json_result = q.run_query(all)
        return {

        'statusCode': 200,

        'headers': {
            'Content-Type': 'application/json',

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'

        },

        'body': json.dumps(json_result)

    }
    elif resource == '/songs' and method == 'GET':
        show = query_params.get('show', None)
        all = "SELECT Song, COUNT() FROM spinalytics.songs WHERE Show_Name = '"+ show +"' GROUP BY Song ORDER BY COUNT() DESC LIMIT 10"
        json_result = q.run_query(all)
        return {

        'statusCode': 200,

        'headers': {
            'Content-Type': 'application/json',

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'

        },

        'body': json.dumps(json_result)

    }
    elif resource == '/shows' and method == 'GET':
        #json_result = '{ ["show": "foo", "show": "bar"]}'
        all = "SELECT DISTINCT(Show_Name) FROM spinalytics.songs"
        json_result = q.run_query(all)
        return {

        'statusCode': 200,

        'headers': {
            'Content-Type': 'application/json',

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'

        },

        'body': json.dumps(json_result)

    }

    else:

        return {

        'statusCode': 200,

        'headers': {
            'Content-Type': 'application/json',

            'Access-Control-Allow-Headers': 'Content-Type',

            'Access-Control-Allow-Origin': '*',

            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'

        },

        'body': json.dumps('Hello from Lambda!')

    }

 

 



