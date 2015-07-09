import psycopg2 as ps
from .config import g

# ############################################################################
# DAILY TRADE CALCULATIONS
# ############################################################################
def func_side_desc(row):
    if row['Side'] == 'B':
        return 'Buy'
    else:
        return 'Sell'

def func_calculated_quantity(row):
    if row['Side'] == 'B':
        return row['Qty'] * 1
    else:
        return row['Qty'] * -1

def func_calculated_principal(row):
    if row['Side'] == 'B':
        return row['Principal'] * 1
    else:
        return row['Principal'] * -1

def func_ticket_fee(row):
    if row['Prime'] == '':
        return .0011 * row['Qty']
    else:
        return 0

def func_total_fee(row):
    return row['ticket_fee'] + row['ECN Fee'] + row['SEC Fee']

def func_away_ticket(row):
    if row['Commission'] == '0':
        return 0
    else:
        return 15

def func_total_cost(row):
    return row['total_fee'] + row['away_ticket'] # + row['Commission'] # adding this back would double count commission

def func_calculated_net(row):
    return row['calculated_principal'] - row['total_cost']

# ############################################################################
# PSYCOPG2 QUICK FUNCTIONS
# ############################################################################
def db_write(sql_cmd):
    conn = ps.connect(host=g['POSTGRES_HOST'],
                      port='5432',
                      user=g['POSTGRES_USER'],
                      password=g['POSTGRES_PWD'],
                      database=g['POSTGRES_DB'])
    cur = conn.cursor()
    cur.execute(sql_cmd)
    conn.commit()
    # RETURN A STATUS MESSAGE HERE

def db_read(sql_cmd):
    conn = ps.connect(host=g['POSTGRES_HOST'],
                      port='5432',
                      user=g['POSTGRES_USER'],
                      password=g['POSTGRES_PWD'],
                      database=g['POSTGRES_DB'])
    cur = conn.cursor()
    cur.execute(sql_cmd)
    return cur

