from stellar_sdk import Server
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import json
import time

# Connect to the Horizon API
server = Server("https://horizon.stellar.org")

# Calculate the start and end times for the query
now = datetime.utcnow()
start_time = now - timedelta(days=14)
# start_time = now - timedelta(hours=3)
print('getting between', start_time, now)

# Get a list of all transactions in the specified time range
transactions = server.transactions().order(desc=True).limit(200).call()

# with open('transactions.json', 'w') as f:
#     json.dump(transactions, f)
accounts = set()

# Iterate through all pages of transactions
while transactions["_links"].get("next"):
    for tx in transactions["_embedded"]["records"]:
        tx_created_at = datetime.fromisoformat(tx["created_at"][:-1])
        difference = now - tx_created_at
        if tx_created_at < start_time:
            # Exit the loop if the transaction time is less than the start time
            transactions["_links"]["next"] = None
            break
        if difference.days == 1:
            now = tx_created_at
            print('time sleeping 5 mins')
            time.sleep(300)
        if "account" in tx:
            accounts.add(tx["account"])
        if "source_account" in tx:
            accounts.add(tx["source_account"])
        if "operations" in tx:
            for op in tx["operations"]:
                if "account" in op:
                    accounts.add(op["account"])
        if "from" in tx:
            accounts.add(tx["from"])
            print(tx)
            break
        if "to" in tx:
            accounts.add(tx["to"])
        if "seller" in tx:
            accounts.add(tx["seller"])
        if "buyer" in tx:
            accounts.add(tx["buyer"])

    if transactions["_links"].get("next"):
        # Get the next page of transactions
        url = transactions["_links"]["next"]["href"]
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        cursor_value = query_params['cursor'][0]

        print('getting next transactions', cursor_value)
        next_transactions = server.transactions().order(desc=True).limit(200).cursor(cursor_value).call()
        transactions = next_transactions

print('saving to text')

# Open a file for writing
with open('accounts.txt', 'w') as f:
    # Write each account to a new line in the file
    for account in accounts:
        f.write(account + '\n')

print('finished')