import sqlite3
from datetime import datetime, timedelta
from prettytable import PrettyTable

def query_netflows_all_chains_html():
    # Connect to the database
    conn = sqlite3.connect('chain_data.db')
    cursor = conn.cursor()
    
    # List of chains to query
    chains = ['Ethereum', 'Solana', 'Base', 'Avalanche', 'Optimism']
    
    # Start of the HTML table
    html_table = """
    <table border="1">
        <caption>Total Inflows/Outflows Across Chains</caption>
        <tr>
            <th>Chain</th>
            <th>One Day Net Flow</th>
            <th>Seven Day Net Flow</th>
            <th>Thirty Day Net Flow</th>
            <th>Sixty Day Net Flow</th>
        </tr>
    """
    
    # Current date
    today = datetime.now().strftime('%Y-%m-%d')

    for chain in chains:
        # Query to calculate the net flows
        query = f"""
        SELECT
          ROUND(SUM(CASE WHEN date >= DATE('{today}', '-1 day') THEN net_inflow ELSE 0 END), 2) AS one_day_net_flow,
          ROUND(SUM(CASE WHEN date >= DATE('{today}', '-7 day') THEN net_inflow ELSE 0 END), 2) AS seven_day_net_flow,
          ROUND(SUM(CASE WHEN date >= DATE('{today}', '-30 day') THEN net_inflow ELSE 0 END), 2) AS thirty_day_net_flow,
          ROUND(SUM(CASE WHEN date >= DATE('{today}', '-60 day') THEN net_inflow ELSE 0 END), 2) AS sixty_day_net_flow
        FROM net_flows
        WHERE chain_name = ?
        """
        cursor.execute(query, (chain,))
        row = cursor.fetchone()

        # Add row to HTML table
        html_table += f"<tr><td>{chain}</td>"
        html_table += ''.join([f"<td>{'{:,}'.format(val) if isinstance(val, float) else val}</td>" for val in row])
        html_table += "</tr>"
    
    # Close the HTML table
    html_table += "</table>"
    
    # Close the database connection
    conn.close()
    
    # Return the HTML table string
    
    return html_table

def query_stablecoins_and_changes_html():
    conn = sqlite3.connect('chain_data.db')
    cursor = conn.cursor()
    chains = ['Ethereum', 'Solana', 'Base', 'Avalanche', 'Optimism', 'Sui']
    
    # Start of the HTML tables
    counts_html = """
    <table border="1">
        <caption>Stablecoin Counts & Changes</caption>
        <tr>
            <th>Chain</th>
            <th>Most Recent Total</th>
            <th>7 Day Change</th>
            <th>30 Day Change</th>
            <th>60 Day Change</th>
        </tr>
    """
    
    percent_change_html = """
    <table border="1">
        <caption>Stablecoin % Changes</caption>
        <tr>
            <th>Chain</th>
            <th>7 Day % Change</th>
            <th>30 Day % Change</th>
            <th>60 Day % Change</th>
        </tr>
    """
    
    for chain in chains:
        cursor.execute("SELECT date, total_stables FROM Stablecoins WHERE chain_name = ? ORDER BY date DESC", (chain,))
        rows = cursor.fetchall()
        
        if not rows:
            continue
        
        most_recent_count = rows[0][1]
        dates_counts = {row[0]: row[1] for row in rows}
        
        # Calculate changes and percent changes
        changes = {}
        for days in [7, 30, 60]:
            past_date = (datetime.strptime(rows[0][0], '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
            past_count = dates_counts.get(past_date, most_recent_count)  # Use most recent count if no exact match
            change = most_recent_count - past_count
            percent_change = (change / past_count) * 100 if past_count else 0
            changes[days] = (change, percent_change)
        
        # Add rows to the HTML tables
        counts_html += f"<tr><td>{chain}</td><td>{most_recent_count:,.2f}</td><td>{changes[7][0]:,.2f}</td><td>{changes[30][0]:,.2f}</td><td>{changes[60][0]:,.2f}</td></tr>"
        percent_change_html += f"<tr><td>{chain}</td><td>{changes[7][1]:.2f}%</td><td>{changes[30][1]:.2f}%</td><td>{changes[60][1]:.2f}%</td></tr>"
    
    # Close the HTML tables
    counts_html += "</table>"
    percent_change_html += "</table>"
    
    conn.close()
    
    # Return the HTML table strings
    return counts_html, percent_change_html

def query_tvl_and_changes_html():
    conn = sqlite3.connect('chain_data.db')  # Adjust the path as necessary
    cursor = conn.cursor()
    chains = ['Ethereum', 'Solana', 'Base', 'Avalanche', 'Optimism', 'Sui', 'Sei', 'Injective']
    
    # Start of the HTML tables
    counts_html = """
    <table border="1">
        <caption>Total Value Locked & Changes</caption>
        <tr>
            <th>Chain</th>
            <th>Most Recent Count</th>
            <th>7 Day Change</th>
            <th>30 Day Change</th>
            <th>60 Day Change</th>
        </tr>
    """
    
    percent_change_html = """
    <table border="1">
        <caption>Total Value Locked % Changes</caption>
        <tr>
            <th>Chain</th>
            <th>7 Day % Change</th>
            <th>30 Day % Change</th>
            <th>60 Day % Change</th>
        </tr>
    """
    
    for chain in chains:
        cursor.execute("SELECT date, TVL FROM TVL WHERE chain_name = ? ORDER BY date DESC", (chain,))
        rows = cursor.fetchall()
        
        if not rows:
            continue
        
        most_recent_count = rows[0][1]
        dates_counts = {row[0]: row[1] for row in rows}
        
        changes = {}
        for days in [7, 30, 60]:
            past_date = (datetime.strptime(rows[0][0], '%Y-%m-%d') - timedelta(days=days)).strftime('%Y-%m-%d')
            past_count = dates_counts.get(past_date, most_recent_count)
            change = most_recent_count - past_count
            percent_change = (change / past_count) * 100 if past_count else 0
            changes[days] = (change, percent_change)
        
        counts_html += f"<tr><td>{chain}</td><td>{most_recent_count:,.2f}</td><td>{changes[7][0]:,.2f}</td><td>{changes[30][0]:,.2f}</td><td>{changes[60][0]:,.2f}</td></tr>"
        percent_change_html += f"<tr><td>{chain}</td><td>{changes[7][1]:.2f}%</td><td>{changes[30][1]:.2f}%</td><td>{changes[60][1]:.2f}%</td></tr>"
    
    counts_html += "</table>"
    percent_change_html += "</table>"
    
    conn.close()
    
    return counts_html, percent_change_html

def query_dex_volume_html():
    conn = sqlite3.connect('chain_data.db')  # Adjust the path as necessary
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM Dex_volume")
    rows = cursor.fetchall()
    conn.close()
    
    dex_volume_html = """
    <table border="1">
        <caption>DEX Volume</caption>
        <tr>
            <th>Chain</th>
            <th>24H Volume</th>
            <th>7D Volume</th>
            <th>30D Volume</th>
        </tr>
    """
    
    for row in rows:
        chain, volume_24h, volume_7d, volume_30d = row
        dex_volume_html += f"<tr><td>{chain}</td><td>{volume_24h:,.2f}</td><td>{volume_7d:,.2f}</td><td>{volume_30d:,.2f}</td></tr>"
    
    dex_volume_html += "</table>"
    
    return dex_volume_html
