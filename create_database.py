import mysql.connector
from mysql.connector import errorcode
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name
from config import bid_levels, ask_levels, get_vix, get_cot, get_stock_volume, event_list, event_values
from config import volume_MA_periods

# Connect to MySQL server
try:
    cnx = mysql.connector.connect(host=mysql_hostname, user=mysql_user, password=mysql_password)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("User name or password incorrect")
    else:
        print(err)
    # Close connection
    cnx.close()
    print('Connection closed')

# Instantiate cursor object
cursor = cnx.cursor()

# Create database if not exists
cursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(mysql_database_name))

# Use database
cursor.execute("USE {}".format(mysql_database_name))

# Define SQL statements to create a table
deep_statement = "".join([", bid_{:d}_size SMALLINT".format(level) for level in range(bid_levels)]) + \
    "".join([", bid_{:d} FLOAT(6,2)".format(level) for level in range(bid_levels)]) + \
    "".join([", ask_{:d}_size SMALLINT".format(level) for level in range(ask_levels)]) + \
    "".join([", ask_{:d} FLOAT(6,2)".format(level) for level in range(ask_levels)]) + \
    ", bids_ord_WA FLOAT(6,4), asks_ord_WA FLOAT(6,4)" + \
    ", vol_imbalance FLOAT(7,4)" + \
    ", delta SMALLINT" + \
    ", micro_price FLOAT(6,2)" + \
    ", spread FLOAT(6,4)" + \
    ", session_start TINYINT" + \
    ", day_1 TINYINT" + \
    ", day_2 TINYINT" + \
    ", day_3 TINYINT" + \
    ", day_4 TINYINT" + \
    ", week_1 TINYINT" + \
    ", week_2 TINYINT" + \
    ", week_3 TINYINT" + \
    ", week_4 TINYINT"

vix_statement = ", VIX FLOAT(5,2)" if get_vix else ""

vol_statement = ", 1_open FLOAT(6,2), 2_high FLOAT(6,2), 3_low FLOAT(6,2), 4_close FLOAT(6,2), 5_volume INT, wick_prct FLOAT(6,4)"\
    if get_stock_volume else ""

cot_statement = ", Asset_long_pos MEDIUMINT" + \
    ", Asset_long_pos_change FLOAT(6,1)" + \
    ", Asset_long_open_int FLOAT(4,1)" + \
    ", Asset_short_pos MEDIUMINT" + \
    ", Asset_short_pos_change FLOAT(6,1)" + \
    ", Asset_short_open_int FLOAT(4,1)" + \
    ", Leveraged_long_pos MEDIUMINT" + \
    ", Leveraged_long_pos_change FLOAT(6,1)" + \
    ", Leveraged_long_open_int FLOAT(4,1)" + \
    ", Leveraged_short_pos MEDIUMINT" + \
    ", Leveraged_short_pos_change FLOAT(6,1)" + \
    ", Leveraged_short_open_int FLOAT(4,1)" if get_cot else ""

ind_statement = "".join([", {}_{} FLOAT(5,1)".format(event, value) for event in event_list for value in event_values])

main_statement = "CREATE TABLE IF NOT EXISTS " + mysql_table_name  + "(Timestamp DATETIME PRIMARY KEY"\
    + deep_statement + vix_statement + vol_statement + cot_statement + ind_statement + ")"

# Create table if not exists
cursor.execute(main_statement)

# Create VIEW with calculated Volume Moving Averages
if volume_MA_periods:
    volume_MA_statement = ""
    vol_view_fields = ""
    for period in volume_MA_periods:
        volume_MA_statement += "AVG(5_volume) OVER (ORDER BY Timestamp ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS vol_MA{}, "\
            .format(period - 1, period)
        vol_view_fields += ", vol_MA{}".format(period)
    volume_MA_statement = volume_MA_statement.strip(", ")

    volume_MA_statement = "CREATE OR REPLACE VIEW vol_MA(Timestamp{}) AS SELECT Timestamp, ".format(vol_view_fields)\
        + volume_MA_statement + " FROM {};".format(mysql_table_name)

    cursor.execute(volume_MA_statement)

