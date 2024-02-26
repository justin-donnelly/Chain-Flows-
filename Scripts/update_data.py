import os
import requests
import json 
import pandas as pd
from pandas.tseries.offsets import DateOffset
from pandas import json_normalize
import time
from defillama import DefiLlama
from datetime import datetime
llama = DefiLlama()
import datetime as dt
import pytz
import sqlite3
from datetime import datetime, timezone

def get_most_recent_net_flows(chain_name: str):
    ids = []
    aggregated_data = {}
    tot = 0 
    # Fetch bridge data
    bridges_url = "https://bridges.llama.fi/bridges?includeChains=true"
    bridges_response = requests.get(bridges_url).json()
    
    # Collect IDs for bridges that involve the specified chain
    for bridge in bridges_response['bridges']:
        if chain_name in bridge['chains']:
            ids.append(bridge['id'])
    
    # Initialize variables to track the most recent date and its net flows
    
    for id in ids:
        url = f"https://bridges.llama.fi/bridgevolume/{chain_name}?id={id}"  # Use f-string for dynamic URL
        response = requests.get(url)
        if response.status_code == 200:  # Check if the request was successful
            d = response.json()
            if d:  # Check if the list is not empty
                d = d[-1]  # Get the most recent data
                # if the date of the most recent entry is = todays date or something like that
                tot += d['depositUSD'] - d['withdrawUSD']
            else:
                pass
        else:
            pass
    
    # Return the most recent date and its aggregated net flows
    aggregated_data[datetime.now().strftime('%Y-%m-%d')] = round(tot,2) 
    return aggregated_data

def get_most_recent_stables(chain_name: str):
    stable_ids = list(range(1, 161))
    total = 0
    date = None  # Initialize date variable outside the loop

    for id in stable_ids:
        url = f"https://stablecoins.llama.fi/stablecoincharts/{chain_name}?stablecoin={id}"
        response = requests.get(url).json()
        if response:  # Ensure the response is not empty
            last_entry = response[-1]
            total += last_entry.get('totalCirculating', {}).get('peggedUSD', 0)
            date = datetime.utcfromtimestamp(int(last_entry['date'])).replace(tzinfo=pytz.utc).astimezone(pytz.timezone("US/Eastern"))

    if date:  # Check if date was updated (implies at least one successful data fetch)
        return {date.strftime('%Y-%m-%d'): total}
    else:
        return {}
    
def get_most_recent_tvl(chain_name: str):
    """
    Fetches the most recent Total Value Locked (TVL) for the specified chain.
    """
    tvl_url = f"https://api.llama.fi/v2/historicalChainTvl/{chain_name}"
    response = requests.get(tvl_url)
    if response.status_code == 200:
        tvl_data = response.json()
        if tvl_data:
            # Assuming the last entry is the most recent
            most_recent_entry = tvl_data[-1]
            date = datetime.utcfromtimestamp(int(most_recent_entry['date'])).replace(tzinfo=pytz.utc).astimezone(pytz.timezone("US/Eastern"))
            most_recent_date = date.strftime('%Y-%m-%d')
            most_recent_tvl = most_recent_entry['tvl']
            
            # Return the most recent date and its TVL
            return {most_recent_date: most_recent_tvl}
    return {}

def get_dex_vol(chain_name:str):
    """
    Get aggregate dex volume done on each chain aggregated across all dexes
    """
    dex_volume = {'24H': 0, '7D': 0, '30D': 0}
    dex_url = f"https://api.llama.fi/overview/dexs/{chain_name}?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume"

    try:
        dex_data = requests.get(dex_url).json()
        for entry in dex_data['protocols']:
            dex_volume['24H'] += entry.get('total24h', 0) if entry.get('total24h') is  not None else 0 
            dex_volume['7D'] += entry.get('total7d', 0) if entry.get('total7d') is  not None else 0 
            dex_volume['30D'] += entry.get('total30d', 0) if entry.get('total30d') is  not None else 0 
    except Exception as e:
        print(f"An error occurred: {e}")

    dex_volume['24H'] = round(dex_volume['24H'],2)
    dex_volume['7D'] = round(dex_volume['7D'],2)
    dex_volume['30D'] = round(dex_volume['30D'],2)

    return dex_volume

def entry_exists(chain_name, date, table_name):
    conn = sqlite3.connect('../chain_data.db')
    cursor = conn.cursor()
    
    query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE chain_name=? AND date=? LIMIT 1)"
    cursor.execute(query, (chain_name, date))
    exists = cursor.fetchone()[0]
    
    conn.close()
    return exists == 1

def insert_data(table_name, data_dict):
    """
    Insert data into the specified table.
    
    Parameters:
    - table_name: The name of the table to insert data into.
    - data_dict: A dictionary where keys are column names and values are the data to insert.
    """
    conn = sqlite3.connect('../chain_data.db')
    cursor = conn.cursor()
    
    # Prepare column names and placeholders for values
    columns = ', '.join(data_dict.keys())
    placeholders = ', '.join(['?' for _ in data_dict])
    
    query = f'''INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})'''
    cursor.execute(query, list(data_dict.values()))
    
    conn.commit()
    conn.close()

def fetch_and_insert_flows(chain_name):
    # Use the modified get_net_flows function that focuses on the most recent data
    flows_data = get_most_recent_net_flows(chain_name)
    for date, net_inflow in flows_data.items():
        if not entry_exists(chain_name, date, 'net_flows'):
            insert_data('net_flows', {
                'chain_name': chain_name,
                'date': date,
                'net_inflow': net_inflow
            })

def fetch_and_insert_stablecoins(chain_name):
    # Use the modified get_stables function
    stables_data = get_most_recent_stables(chain_name)
    for date, total_stables in stables_data.items():
        if not entry_exists(chain_name, date, 'Stablecoins'):
            insert_data('Stablecoins', {
                'chain_name': chain_name,
                'date': date,
                'total_stables': total_stables
            })

def fetch_and_insert_tvl(chain_name):
    # Use the modified get_tvl function
    tvl_data = get_most_recent_tvl(chain_name)
    for date, tvl in tvl_data.items():
        if not entry_exists(chain_name, date, 'TVL'):
            insert_data('TVL', {
                'chain_name': chain_name,
                'date': date,
                'TVL': tvl
            })

def fetch_and_insert_dex_volume(chain_name):
    dex_volume_data = get_dex_vol(chain_name)  # Ensure this function returns the most recent 24H, 7D, and 30D volume data
    # Prepare the data dictionary for insertion
    formatted_dex_volume_data = {
        'chain_name': chain_name,
        'volume_24h': dex_volume_data.get('24H', 0),
        'volume_7d': dex_volume_data.get('7D', 0),
        'volume_30d': dex_volume_data.get('30D', 0),
    }
    # Directly insert or replace the DEX volume data for the chain
    insert_data('Dex_Volume', formatted_dex_volume_data)

chains = ['Ethereum', 'Solana', 'Base', 'Sei', 'Sui', 'Injective', 'Avalanche', 'Optimism']

for chain in chains:
    fetch_and_insert_flows(chain)
    fetch_and_insert_stablecoins(chain)
    fetch_and_insert_tvl(chain)
    fetch_and_insert_dex_volume(chain)