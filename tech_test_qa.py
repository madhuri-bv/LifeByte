import pandas as pd

trades_df = pd.read_csv('trades.csv')
users_df = pd.read_csv('users.csv')

# unexpected strings in the 'symbol' and 'currency' columns
unexpected_symbols = trades_df[~trades_df['symbol'].str.isalpha()]['symbol']
print("Unexpected Symbols:", unexpected_symbols)
unexpected_currencies = users_df [~users_df ['currency'].str.isalpha()]['currency']
print("Unexpected Currencies:", unexpected_currencies)

# unexpected numerical values in the 'volume' column
unexpected_volume_values = trades_df[~trades_df['volume'].astype(str).str.replace('.', '', 1).str.isnumeric()]
print("Unexpected Volume Values:", unexpected_volume_values)

# unexpected dates in the 'dt_report' column
unexpected_dates = trades_df[~pd.to_datetime(trades_df['dt_report'], errors='coerce').notnull()]
print("Unexpected Dates:", unexpected_dates)

# login_hash in trades table but not in the users table
invalid_logins = trades_df[~trades_df['login_hash'].isin(users_df['login_hash'])]['login_hash']
print("Invalid Logins:", invalid_logins)

# server_hash in trades table but not in the users table
invalid_servers = trades_df[~trades_df['server_hash'].isin(users_df['server_hash'])]['server_hash']
print("Invalid Servers:", invalid_servers)

# login_hash in users table but not in the trades table
unused_logins = users_df[~users_df['login_hash'].isin(trades_df['login_hash'])]['login_hash']
print("Unused Logins:", unused_logins)

# server_hash in users table is not used in the trades table
unused_servers = users_df[~users_df['server_hash'].isin(trades_df['server_hash'])]['server_hash']
print("Unused Servers:", unused_servers)

