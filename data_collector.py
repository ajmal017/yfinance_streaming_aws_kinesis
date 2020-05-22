import boto3
import os
import subprocess
import sys
import json

subprocess.check_call([sys.executable, "-m", "pip", "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')
import yfinance as yf

import pandas

def lambda_handler(event, context):
    
    ###################################
    ### Date and Company Parameters ###
    ###################################
   
    sdate = "2020-05-14"
    edate = "2020-05-15"
    tickers = yf.Tickers('fb shop bynd nflx pins sq ttd okta snap ddog')
    
    ##########################
    ### Load to Dataframes ###
    ##########################
    
    fb = tickers.tickers.FB.history(start = sdate, end = edate,interval="1m")
    shop = tickers.tickers.SHOP.history(start = sdate, end = edate,interval="1m")
    bynd = tickers.tickers.BYND.history(start = sdate, end = edate,interval="1m")
    nflx = tickers.tickers.NFLX.history(start = sdate, end = edate,interval="1m")
    pins = tickers.tickers.PINS.history(start = sdate, end = edate,interval="1m")
    sq = tickers.tickers.SQ.history(start = sdate, end = edate,interval="1m")
    ttd = tickers.tickers.TTD.history(start = sdate, end = edate,interval="1m")
    okta = tickers.tickers.OKTA.history(start = sdate, end = edate,interval="1m")
    snap = tickers.tickers.SNAP.history(start = sdate, end = edate,interval="1m")
    ddog = tickers.tickers.DDOG.history(start = sdate, end = edate,interval="1m")
    
    #######################
    ### Add Name Column ###
    #######################
    
    fb['name'] = 'FB'
    shop['name'] = 'SHOP'
    bynd['name'] = 'BYND'
    nflx['name'] = 'NFLX'
    pins['name'] = 'PINS'
    sq['name'] = 'SQ'
    ttd['name'] = 'TTD'
    okta['name'] = 'OKTA'
    snap['name'] = 'SNAP'
    ddog['name'] = 'DDOG'
    
    ##########################################
    ### Combine and Format to Desired Spec ###
    ##########################################
    
    df = pandas.concat([fb, shop, bynd, nflx, pins, sq, ttd, okta, snap, ddog])
    df = df.reset_index()
    df = df.reset_index(drop=True)
    df = df[['High','Low','Datetime','name']]
    df = df.rename(columns={"High": "high", "Low": "low", "Datetime": "ts"})
    
    #################################################
    ### Format Date and Drop Pull Timestamp Entry ###
    #################################################
    
    df['ts'] = df['ts'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    df = df.loc[df.ts < edate]
    df['ts']=df['ts'].astype(str)
    
    #######################
    ### Convert to JSON ###
    #######################
    
    as_jsonstr = df.to_json(orient='records', lines=True)
    
    ###############################
    ### initialize boto3 client ###
    ###############################
    
    fh = boto3.client("firehose", "us-east-2")
    
    #######################
    ### Push and Encode ###
    #######################
    
    fh.put_record(
        DeliveryStreamName="project-delivery-stream", 
        Record={"Data": as_jsonstr.encode('utf-8')})
    
    ##############
    ### Return ###
    ##############
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Done! Recorded: {as_jsonstr}')
    }