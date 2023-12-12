import json
import os
import boto3
import requests
#from gql import gql
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "GitHub-token"
    region_name = "ap-southeast-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    get_secret_value_reponse = client.get_secret_value(SecretId=secret_name)

    secret_data = json.loads(get_secret_value_reponse['SecretString'])

    # print(secret_data['token'])

    return secret_data['token']
    
    
def upload_s3(bucket_name,key,data):
    s3_client = boto3.client("s3")
    print(bucket_name)
    print(key)
    
    concat_strings = lambda str1, str2: ''.join([str1, str2])
    
    try:
        print("vao day roi ne")
        response = s3_client.head_object(Bucket=bucket_name, Key=key)
        print('exists in S3 bucket')
    
        add = s3_client.get_object(Bucket=bucket_name,Key=key)
        print(add)
        
        exist_data = add['Body'].read().decode('utf-8')
        new_data = concat_strings(exist_data,data)
        s3_client.put_object(Bucket=bucket_name,Key=key,Body=new_data)
        
    except Exception as e:
        print('does not exist in S3 bucket')
    
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=data)
        
    
def query_graphql(query,token):
    
    base_url = 'https://api.github.com/graphql'
    headers = {
        'Authorization': f'{token}'
    }
    
    response = requests.post(base_url,json= {'query':query,},headers =headers)
    return response.json()
    
    
def lambda_handler(event, context):
    
    event={'org': 'microsoft', 'next':'', 'hasNextPage': ''}
    
    query = """
                query {
                    organization(login: "_org_") {
                        repositories(first:10, _next_){
                            pageInfo{
                                startCursor
                                endCursor
                                hasNextPage
                            }
                            nodes{
                                id
                                name
                                url
                            }
                        }
                    }
                }"""
    #lay token
    token = get_secret()

    #kiem tra event "org" da co gia tri truyen vao chua
    if event["org"]!="":
        query = query.replace('_org_',f'{event["org"]}')
 
    # kiem tra event "next" da co gia tri truyen vao chua
    if event["next"] == "":
        x = query_graphql(query.replace('_next_',''),token)
    else :
        x = query_graphql(query.replace('_next_',f'after :"{event["next"]}"'),token)

    # x = query_graphql(query,token)


    #check la hasnextpage, gia tri True or False
    check = x['data']['organization']['repositories']['pageInfo']['hasNextPage']
    
    #end la endCursor
    end = x['data']['organization']['repositories']['pageInfo']['endCursor']
    
    #gan event "hasNextPage" 
    event["hasNextPage"]=f"{check}"
    
    #gia tri cua nodes
    data_nodes = x['data']['organization']['repositories']['nodes']
    
    kq = json.dumps(data_nodes, indent =2)
    #loai bo [ va ] trong ket qua
    data_out = kq[1:-1]
    
    # upload_s3("my-busket-test","done.txt",data_out)
    
    if event["hasNextPage"]=="False":
        upload_s3("my-busket-test","done.txt",data_out)
        #neu khong co nextpage thi gan event "next" lai gia tri 
        event["next"]=""
        return event
    else:
        upload_s3("my-busket-test","done.txt",data_out)
        event["next"]=f'{end}'
    
    return event


