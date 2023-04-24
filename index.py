from stellar_sdk import Server
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import json
import time

# Connect to the Horizon API
server = Server("https://horizon.stellar.org")

# Calculate the start and end times for the query
end_time = datetime(2023, 4, 24, 23, 59, 59, 999999)
start_time = end_time - timedelta(days=4)
diff_time = end_time
# start_time = end_time - timedelta(hours=3)

endiso_time = end_time.isoformat()
startiso_time = start_time.isoformat()

print('getting between', start_time, end_time)

# Get a list of all transactions in the specified time range
transactions = server.transactions().order(desc=True).limit(200).call()

# with open('transactions.json', 'w') as f:
#     json.dump(transactions, f)
accounts = set()

# Iterate through all pages of transactions
while transactions["_links"].get("next"):
    print(len(transactions["_embedded"]["records"]))
    for tx in transactions["_embedded"]["records"]:
        tx_created_at = datetime.fromisoformat(tx["created_at"][:-1])
        print(tx_created_at, end_time)
        difference = diff_time - tx_created_at
        if tx_created_at < start_time:
            # Exit the loop if the transaction time is less than the start time
            transactions["_links"]["next"] = None
            break
        if difference.days == 1:
            diff_time = tx_created_at
            print('time sleeping 5 mins')
            time.sleep(300)
        if tx_created_at <= end_time:
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
        next_transactions = server.transactions().limit(200).cursor(cursor_value).call()
        transactions = next_transactions

print('saving to text')

# Open a file for writing
with open('accounts.txt', 'w') as f:
    # Write each account to a new line in the file
    for account in accounts:
        f.write(account + '\n')

print('finished')