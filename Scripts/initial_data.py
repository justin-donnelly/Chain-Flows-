import os
import requests
import json 
import pandas as pd
from pandas.tseries.offsets import DateOffset
from pandas import json_normalize
import time
from defillama import DefiLlama
from datetime import datetime, timedelta
llama = DefiLlama()
import datetime as dt
import pytz
import sqlite3

def get_net_flows(chain_name:str):
    """
    Takes in the name of a block chain, checks 
    all bridges to see if the chain is used on that bridge
    aggregates inflow/outflow of money to chain via each bridge
    """
    ids = []
    aggregated_data = {}

    bridges = "https://bridges.llama.fi/bridges?includeChains=true"
    bridges = requests.get(bridges).json()
    
    for i in range(len(bridges['bridges'])):
        if f"{chain_name}" in bridges['bridges'][i]['chains']:
            ids.append(bridges['bridges'][i]['id'])
    
    for id in ids:
        url = f"https://bridges.llama.fi/bridgevolume/{chain_name}?id={id}"
        vol_data = requests.get(url).json()[-60:]

        for entry in vol_data:
            date = datetime.utcfromtimestamp(int(entry['date']))
            eastern = pytz.timezone("US/Eastern")
            date = date.replace(tzinfo=pytz.utc).astimezone(eastern)
            date = date.strftime('%Y-%m-%d')
            net_inflow = entry['depositUSD'] - entry['withdrawUSD']

            if date in aggregated_data:
                aggregated_data[date] += net_inflow
            else:
                aggregated_data[date] = net_inflow
    sorted_flows = sorted(aggregated_data.items(), key=lambda x: x[0])

    return sorted_flows

def get_stables(chain_name:str):
    """
    Stable ids is a list 1-160 for all stablecoin ids on defillama
    chain name + stablecoin id is passed into the request and the 
    aggregate stable coin value on each chain is calculated 
    """
    stable_ids = list(range(1,160))
    aggregated_stables = {}
       
    for id in stable_ids:
        url = f"https://stablecoins.llama.fi/stablecoincharts/{chain_name}?stablecoin={id}"
        stables_data = requests.get(url).json()[-60:]  # Get the last 60 records

        if stables_data:
            for entry in stables_data:
                date = datetime.utcfromtimestamp(int(entry['date']))
                eastern = pytz.timezone("US/Eastern")
                date = date.replace(tzinfo=pytz.utc).astimezone(eastern).strftime('%Y-%m-%d')

                # Correctly using the safe access method here
                total_circulating_peggedUSD = entry.get('totalCirculating', {}).get('peggedUSD', 0)

                # Aggregate using the safely accessed value
                if date in aggregated_stables:
                    aggregated_stables[date] += total_circulating_peggedUSD
                else:
                    aggregated_stables[date] = total_circulating_peggedUSD
    sorted_agg_stables = sorted(aggregated_stables.items(), key = lambda x:x[0])

    return sorted_agg_stables

def get_tvl(chain_name:str):
    """
    Take in chain name, pass to TVL request, gather TVL across chains
    """
    tvl = {}
    tvl_url = F"https://api.llama.fi/v2/historicalChainTvl/{chain_name}"
    tvl_data = requests.get(tvl_url).json()[-60:]

    for entry in tvl_data:
        date = datetime.utcfromtimestamp(int(entry['date']))
        eastern = pytz.timezone("US/Eastern")
        date = date.replace(tzinfo=pytz.utc).astimezone(eastern)
        date = date.strftime('%Y-%m-%d')

        
        tvl[date] = entry['tvl']
    
    sorted_tvl = sorted(tvl.items(), key = lambda x:x[0])

    return sorted_tvl

def get_dex_vol(chain_name:str):
    """
    Get aggregate dex volume done on each chain aggregated across all dexes
    """
    dex_volume = {'24H': 0, '7D': 0, '30D': 0}
    dex_url = f"https://api.llama.fi/overview/dexs/{chain_name}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume"

    try:
        dex_data = requests.get(dex_url).json()
        for entry in dex_data['protocols']:
            dex_volume['24H'] += round(entry.get('total24h', 0),2) if entry.get('total24h') is  not None else 0 
            dex_volume['7D'] += round(entry.get('total7d', 0),2) if entry.get('total7d') is  not None else 0 
            dex_volume['30D'] += round(entry.get('total30d', 0),3) if entry.get('total30d') is  not None else 0 
    except Exception as e:
        print(f"An error occurred: {e}")

    return dex_volume

def create_tables():
    conn = sqlite3.connect('chain_data.db')
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS net_flows
                      (chain_name TEXT, date TEXT, net_inflow REAL, PRIMARY KEY (chain_name, date))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Stablecoins
                      (chain_name TEXT, date TEXT, total_stables REAL, PRIMARY KEY (chain_name, date))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS TVL
                      (chain_name TEXT, date TEXT, TVL REAL, PRIMARY KEY (chain_name, date))''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Dex_Volume
                      (chain_name TEXT, volume_24h REAL, volume_7d REAL, volume_30d REAL, PRIMARY KEY (chain_name))''')
    
    
    conn.commit()
    conn.close()

def insert_data(table_name, data_dict):
    """
    Insert data into the specified table.
    
    Parameters:
    - table_name: The name of the table to insert data into.
    - data_dict: A dictionary where keys are column names and values are the data to insert.
    """
    conn = sqlite3.connect('chain_data.db')
    cursor = conn.cursor()
    
    # Prepare column names and placeholders for values
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['?' for _ in data_dict])
    
    query = f'''INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})'''
    cursor.execute(query, list(data_dict.values()))
    
    conn.commit()
    conn.close()

def fetch_and_insert_flows(chain_name):
    flows_data = get_net_flows(chain_name)  # Make sure this function returns the correct data format
    for date, net_inflow in flows_data:
        insert_data('net_flows', {  # Use the correct table name as defined in create_tables
            'chain_name': chain_name,
            'date': date,
            'net_inflow': net_inflow
        })


def fetch_and_insert_stablecoins(chain_name):
    stables_data = get_stables(chain_name)  # Assuming this returns a list of tuples (date, total_stables)
    for date, total_stables in stables_data:
        insert_data('Stablecoins', {
            'chain_name': chain_name,
            'date': date,
            'total_stables': total_stables
        })

# Example for fetching and inserting TVL and DEX Volume (simplified)
def fetch_and_insert_tvl(chain_name):
    tvl_data = get_tvl(chain_name)  # Assuming it returns sorted tuples (date, TVL)
    for date, tvl in tvl_data:
        insert_data('TVL', {
            'chain_name': chain_name,
            'date': date,
            'TVL': tvl
        })

def fetch_and_insert_dex_volume(chain_name):
    dex_volume_data = get_dex_vol(chain_name)  
    formatted_dex_volume_data = {
        'chain_name': chain_name,
        'volume_24h': dex_volume_data.get('24H', 0),  # Assume get_dex_vol returns dict with '24H', '7D', '30D'
        'volume_7d': dex_volume_data.get('7D', 0),
        'volume_30d': dex_volume_data.get('30D', 0),
    }
    insert_data('Dex_Volume', formatted_dex_volume_data)

# Iterate over all chains and fetch/insert data for each
chains = ['Ethereum', 'Solana', 'Base', 'Sei', 'Sui', 'Injective', 'Avalanche', 'Optimism']

create_tables()

for chain in chains:
    fetch_and_insert_flows(chain)
    fetch_and_insert_stablecoins(chain)
    fetch_and_insert_tvl(chain)
    fetch_and_insert_dex_volume(chain)


