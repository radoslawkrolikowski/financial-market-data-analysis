import mysql.connector
from mysql.connector import errorcode
from config import mysql_user, mysql_password, mysql_hostname, mysql_database_name, mysql_table_name
from config import bid_levels, ask_levels, get_vix, get_cot, get_stock_volume, event_list, event_values
from config import volume_MA_periods, price_MA_periods, delta_MA_periods, bollinger_bands_period, bollinger_bands_std, stochastic_oscillator

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

# Define SQL statements used to create a table
deep_statement = "".join([", bid_{:d}_size SMALLINT".format(level) for level in range(bid_levels)]) + \
    "".join([", bid_{:d} FLOAT(6,2)".format(level) for level in range(1, bid_levels)]) + \
    "".join([", ask_{:d}_size SMALLINT".format(level) for level in range(ask_levels)]) + \
    "".join([", ask_{:d} FLOAT(6,2)".format(level) for level in range(1, ask_levels)]) + \
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

main_statement = "CREATE TABLE IF NOT EXISTS " + mysql_table_name  + "(ID MEDIUMINT KEY AUTO_INCREMENT, Timestamp DATETIME"\
    + deep_statement + vix_statement + vol_statement + cot_statement + ind_statement + ");"

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

# VIEW for Price Moving Averages
if price_MA_periods:
    price_MA_statement = ""
    price_view_fields = ""
    for period in price_MA_periods:
        price_MA_statement += "AVG(4_close) OVER (ORDER BY Timestamp ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS price_MA{}, "\
            .format(period - 1, period)
        price_view_fields += ", price_MA{}".format(period)
    price_MA_statement = price_MA_statement.strip(", ")

    price_MA_statement = "CREATE OR REPLACE VIEW price_MA(Timestamp{}) AS SELECT Timestamp, ".format(price_view_fields)\
        + price_MA_statement + " FROM {};".format(mysql_table_name)

    cursor.execute(price_MA_statement)

# VIEW for Delta indicator Moving Averages
if delta_MA_periods:
    delta_MA_statement = ""
    delta_view_fields = ""
    for period in delta_MA_periods:
        delta_MA_statement += "AVG(delta) OVER (ORDER BY Timestamp ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS delta_MA{}, "\
            .format(period - 1, period)
        delta_view_fields += ", delta_MA{}".format(period)
    delta_MA_statement = delta_MA_statement.strip(", ")

    delta_MA_statement = "CREATE OR REPLACE VIEW delta_MA(Timestamp{}) AS SELECT Timestamp, ".format(delta_view_fields)\
        + delta_MA_statement + " FROM {};".format(mysql_table_name)

    cursor.execute(delta_MA_statement)

# Create VIEW for Bollinger Bands
# LaTex formula:
# upper_BB = (AVG_{n}(P_{close}) + N_{std} \cdot STD_{n}(P_{close})) - P_{close}
# lower_BB_dist = P_{close} - (AVG_{n}(P_{close}) - N_{std} \cdot STD_{n}(P_{close}))
# upper_BB_dist represents distance between upper band and current price
# lower_BB_dist represents difference between current price and lower band
if bollinger_bands_period and bollinger_bands_std:
    BB_statement = "CREATE OR REPLACE VIEW bollinger_bands(Timestamp, upper_BB_dist, lower_BB_dist) AS SELECT Timestamp, " + \
      "(BB_avg + {} * BB_std) - 4_close AS upper_BB_dist, ".format(bollinger_bands_std) + \
      "4_close - (BB_avg - {} * BB_std) AS lower_BB_dist ".format(bollinger_bands_std) + \
      "FROM (SELECT Timestamp, 4_close, " + \
      "STD(4_close) OVER (ORDER BY Timestamp ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS BB_std, ".format(bollinger_bands_period - 1) + \
      "AVG(4_close) OVER (ORDER BY Timestamp ROWS BETWEEN {} PRECEDING AND CURRENT ROW) AS BB_avg ".format(bollinger_bands_period - 1) + \
      "FROM {}) AS S;".format(mysql_table_name)

    cursor.execute(BB_statement)

# Create VIEW for Stochastic Oscillator
# LaTex formula:
# \frac{P_{close} - MIN_{14}(P_{close})}{MAX_{14}(P_{close}) - MIN_{14}(P_{close})}
# Returns values in range 0-1
if stochastic_oscillator:
    SO_statement = "CREATE OR REPLACE VIEW stochastic_oscillator(Timestamp, stoch) AS SELECT Timestamp, " + \
      "((4_close - min14) / (max14 - min14)) AS stoch FROM (SELECT Timestamp, 4_close, " + \
      "MIN(4_close) OVER (ORDER BY Timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS min14, " + \
      "MAX(4_close) OVER (ORDER BY Timestamp ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS max14 " + \
      "FROM {}) AS S;".format(mysql_table_name)

    cursor.execute(SO_statement)

# Select columns of the main table and chosen VIEWS
cursor.execute("DESCRIBE {};".format(mysql_table_name))
SD_columns = cursor.fetchall()
SD_columns = "".join([", sd.{}".format(name[0]) for name in SD_columns if name[0] != "Timestamp"]).strip(", ")

BB_columns = ""

if bollinger_bands_period and bollinger_bands_std:
    cursor.execute("DESCRIBE bollinger_bands;")
    BB_columns = cursor.fetchall()
    BB_columns = "".join([", bb.{}".format(name[0]) for name in BB_columns if name[0] != "Timestamp"])

vol_columns = ""

if volume_MA_periods:
    cursor.execute("DESCRIBE vol_MA;")
    vol_columns = cursor.fetchall()
    vol_columns = "".join([", vol.{}".format(name[0]) for name in vol_columns if name[0] != "Timestamp"])

price_columns = ""

if price_MA_periods:
    cursor.execute("DESCRIBE price_MA;")
    price_columns = cursor.fetchall()
    price_columns = "".join([", p.{}".format(name[0]) for name in price_columns if name[0] != "Timestamp"])

delta_columns = ""

if delta_MA_periods:
    cursor.execute("DESCRIBE delta_MA;")
    delta_columns = cursor.fetchall()
    delta_columns = "".join([", d.{}".format(name[0]) for name in delta_columns if name[0] != "Timestamp"])

join_statement = "SELECT " + SD_columns + BB_columns + vol_columns + price_columns + delta_columns + \
    " FROM " + mysql_table_name + " sd"

if bollinger_bands_period and bollinger_bands_std:
    join_statement += " JOIN bollinger_bands bb ON sd.Timestamp = bb.Timestamp"

if volume_MA_periods:
    join_statement += " JOIN vol_MA vol ON sd.Timestamp = vol.Timestamp"

if price_MA_periods:
    join_statement += " JOIN price_MA p ON sd.Timestamp = p.Timestamp"

if delta_MA_periods:
    join_statement += " JOIN delta_MA d ON sd.Timestamp = d.Timestamp"

join_statement += ";"

# Create new table that consists of the main table columns and all created views
# This approach requires inserting new values to the table manually!

# new_table_name = "stock_data_full"

# cursor.execute("CREATE TABLE IF NOT EXISTS {} LIKE {};".format(new_table_name, mysql_table_name))

# cursor.execute("DESCRIBE {};".format(new_table_name))
# tab_columns = cursor.fetchall()
# tab_columns = [name[0] for name in tab_columns]

# if bollinger_bands_period and bollinger_bands_std:
#     cursor.execute("DESCRIBE bollinger_bands;")
#     BB_columns = cursor.fetchall()
#     BB_columns = [name[0] for name in BB_columns if name[0] != "Timestamp"]

#     # Check if VIEW columns don't already exist in new table
#     if not any([name in tab_columns for name in BB_columns]):
#         add_cols = "".join(["ADD COLUMN {} FLOAT(6,2), ".format(name) for name in BB_columns]).strip(", ")

#         cursor.execute("ALTER TABLE {} {};".format(new_table_name, add_cols))

# if volume_MA_periods:
#     cursor.execute("DESCRIBE vol_MA;")
#     vol_columns = cursor.fetchall()
#     vol_columns = [name[0] for name in vol_columns if name[0] != "Timestamp"]

#     if not any([name in tab_columns for name in vol_columns]):
#         add_cols = "".join(["ADD COLUMN {} FLOAT(14,2), ".format(name) for name in vol_columns]).strip(", ")

#         cursor.execute("ALTER TABLE {} {};".format(new_table_name, add_cols))

# if price_MA_periods:
#     cursor.execute("DESCRIBE price_MA;")
#     price_columns = cursor.fetchall()
#     price_columns = [name[0] for name in price_columns if name[0] != "Timestamp"]

#     if not any([name in tab_columns for name in price_columns]):
#         add_cols = "".join(["ADD COLUMN {} FLOAT(6,2), ".format(name) for name in price_columns]).strip(", ")

#         cursor.execute("ALTER TABLE {} {};".format(new_table_name, add_cols))

# if delta_MA_periods:
#     cursor.execute("DESCRIBE delta_MA;")
#     delta_columns = cursor.fetchall()
#     delta_columns = [name[0] for name in delta_columns if name[0] != "Timestamp"]

#     if not any([name in tab_columns for name in delta_columns]):
#         add_cols = "".join(["ADD COLUMN {} FLOAT(6,2), ".format(name) for name in delta_columns]).strip(", ")

#         cursor.execute("ALTER TABLE {} {};".format(new_table_name, add_cols))
