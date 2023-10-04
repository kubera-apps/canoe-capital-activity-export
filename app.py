import pandas as pd
import os
from dotenv import load_dotenv
import aiohttp
import asyncio
from datetime import datetime as dt
import sys

load_dotenv()

class Auth:
    redirect_uri = 'https://www.xo.team/'
    url = 'https://api.canoesoftware.com/oauth/token'
    CLIENT_ID = os.getenv('CLIENT_ID') # CLIENT_ID should reside in .env file
    CLIENT_SECRET = os.getenv('CLIENT_SECRET') # CLIENT_SECRET should reside in .env file
    ORG_NAME = os.getenv('ORG_NAME') # ORG_NAME should reside in .env file
    DATE_AFTER = os.getenv('DATE_AFTER') # DATE_AFTER should reside in .env file
    headers = {
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/json',
    }
    body = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
    }
    date_after = dt.strptime('2000-01-01', '%Y-%m-%d')

    if DATE_AFTER:
        date_after = dt.strptime(DATE_AFTER, '%Y-%m-%d')

    @staticmethod
    async def authenticate():
        async with aiohttp.ClientSession() as session:
            async with session.post(Auth.url, json=Auth.body, headers=Auth.headers) as resp:
                parsed = await resp.json()
                access_token = parsed['access_token']
                return access_token

# Organizations class that inherits from Auth class
class Organizations(Auth):
    def __init__(self, access_token):
        super().__init__()
        self.headers = {
            'Authorization': f'Bearer {access_token}', # need to adjust
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }

    async def get_organizations(self):
        url = 'https://api.canoesoftware.com/v1/organizations'
        params = {
            # restrict to organization if required using organization id. ids will have all the organization ids
            'type': 'account'
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.headers, params=params) as resp:
                data = await resp.json()
                orgData = [p for p in data if p['name'] == self.ORG_NAME]
                return data, [orgData[0]["id"]], [orgData[0]["name"]]

    async def get_single_org_document_data(self, session, org_id):
        url = f'https://api.canoesoftware.com/v1/organizations/{org_id}/document-data'
        params = {
            'fields': 'name,document_type,validated_data',
            'category': 'Capital Activity'
        }
        async with session.get(url, headers=self.headers, params=params) as resp:
            response = await resp.json()
            call = [p for p in response if p['document_type'] == "Capital Call Notice"]
            distribution = [p for p in response if p['document_type'] == "Capital Distribution Notice"]
            
            return {
                'call': call,
                'distribution': distribution
            }

    async def get_all_org_document_data(self):
        _, org_ids, _ = await self.get_organizations()
        async with aiohttp.ClientSession() as session:
            responses = await asyncio.gather(*(self.get_single_org_document_data(session, org_id) for org_id in org_ids))
            call = []
            distribution = []
            statement = []

            for item in responses:
                call.extend(item['call'])
                distribution.extend(item['distribution'])

            return {
                'call': call,
                'distribution': distribution
            }

    async def get_all(self):
        responses = await self.get_all_org_document_data()
        result_dfs = []

        for response in responses['call']:
            validatedData = response['allocations'][0]['validated_data']
            
            date = ''
            if 'dueDate' in validatedData:
                date = validatedData['dueDate']

            cashIn = 0
            if 'capitalCall' in validatedData:
                cashIn = validatedData['capitalCall']
            
            entity = ''
            if 'entity' in validatedData:
                entity = validatedData['entity']
            
            fundName = ''
            if 'fundName' in validatedData:
                fundName = validatedData['fundName']
            
            call_activity = {
                'clientNameOrEmail': entity,
                'assetName': fundName,
                'date': date,
                'cashIn': cashIn,
                'cashOut': 0
            }
            activityDate = dt.strptime(date, '%Y-%m-%d')
            if (activityDate > self.date_after):
                result_dfs.append(call_activity)

        for response in responses['distribution']:
            validatedData = response['allocations'][0]['validated_data']
            
            date = ''
            if 'distributionDate' in validatedData:
                date = validatedData['distributionDate']

            cashOut = 0
            if 'distribution' in validatedData:
                cashOut = validatedData['distribution']
            
            entity = ''
            if 'entity' in validatedData:
                entity = validatedData['entity']
            
            fundName = ''
            if 'fundName' in validatedData:
                fundName = validatedData['fundName']
            
            distribution_activity = {
                'clientNameOrEmail': entity,
                'assetName': fundName,
                'date': date,
                'cashIn': 0,
                'cashOut': cashOut
            }
            
            activityDate = dt.strptime(date, '%Y-%m-%d')
            if (activityDate > self.date_after):
                result_dfs.append(distribution_activity)
        
        result_dfs.sort(key=lambda x: x['date'], reverse=True)
        fileName = 'capital_activity.csv'
        pd.DataFrame(result_dfs).to_csv(fileName, index=False) # Write to csv
        print(' ')
        print('------------------------------------')
        print('CSV file[' + fileName + '] generated successfully')
        print('------------------------------------')
        return

async def main():
    access_token = await Auth.authenticate()
    org = Organizations(access_token)
    dfs = await org.get_all()

if __name__ == '__main__':
    asyncio.run(main())