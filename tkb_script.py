# Author: Thomas Brown  
# Date: 1/16/21
# Description: Technical Assessment for Pricing Data Analyst Position

# Importing Libraries:
import pandas as pd
import sqlite3

# Connecting to the first DB (4 tables): 
conn = sqlite3.connect("/Users/Thomas/Desktop/Data Sources/Price_Data.sqlite3")
c = conn.cursor()

# Connecting to the second DB (1 table):
conn2 = sqlite3.connect("/Users/Thomas/Desktop/Data Sources/Register_Data.sqlite3")
c2 = conn2.cursor()

# Setting up the Register Data so it can be joined with the price data:
c2.execute("""SELECT ItemNumber || "-" || PriceZoneNumber AS ItemZone, *
              FROM currentprices
              WHERE PriceZoneType = 'Bev'
              OR PriceZoneType = 'Food';""")
df_reg = pd.DataFrame(c2.fetchall())
df_reg.columns = [i[0] for i in c2.description]
# I added 'Item Zone as a better way to do inner joins on Item Number and Zone Number' when it comes to comparing this table to the Price Database
# I excluded Merch and Drink as the instructions only call for comparisons between 

# Adding currentprices to the main db for easier joins
df_reg.to_sql("currentprices", conn, index = False)

# Simplifying the Price DataBase into one 'source of truth' table:
# First I'm combining the foodprice and bevprice tables (each joined with zonenumbers for ZoneNumber) so I can do an inner join. . . 
# . . . with itemnumbers so I can create the same field, ItemZone, as I created in the currentprices table for the joins. . . 
#. . . necessary for the main tables I'll be creating. It'll act as a key.
# The following table has everything needed from the Price DB side of the equation. All that's left is joins with the Register DB.

c.execute("""WITH foodbev AS (
             SELECT f.TierSize, f.FoodZoneName AS ZoneName, 
             z.ZoneNumber, f.Price
             FROM foodprice f
             INNER JOIN zonenumbers z
             ON f.FoodZoneName = z.ZoneName
             
             UNION ALL
             
             SELECT b.TierSize, b.BevZoneName AS ZoneName, z.ZoneNumber, b.Price
             FROM bevprice b
             INNER JOIN zonenumbers z
             ON b.BevZoneName = z.ZoneName)
             
                 SELECT CAST(i.ItemNumber AS INT) || "-" || fb.ZoneNumber AS ItemZone,
                 i.ItemNumber, fb.ZoneName, fb.ZoneNumber, fb.Price as PriceData, i.FoodOrBev
                 FROM itemnumbers i
                 INNER JOIN foodbev fb
                 ON i.TierSize = fb.TierSize;""")
df_price_data = pd.DataFrame(c.fetchall())
df_price_data.columns = [i[0] for i in c.description]

# Since this is the table I'll be referencing the most, I'll add it to the database
# I'm naming it 'pricedata' to stick to naming conventions and indicate that this is the source of truth for the price db
df_price_data.to_sql("pricedata", conn, index = False)
# Now I have all the necessary tables ready so I can move on to the questions

# Table 1:
# “Price_Deltas.csv”: a csv that contains any beverage or food products that have a price mismatch between Price_Data and Register_Data
c.execute("""SELECT p.ItemNumber, r.PriceZoneNumber, r.PriceZoneType, r.Price AS PriceRegister,
             p.PriceData, 
             ABS(p.PriceData - r.Price) AS PriceDelta
             FROM pricedata p
             INNER JOIN currentprices r
             ON 
             p.ItemZone = r.ItemZone
             WHERE PriceDelta > 0;""")
df1 = pd.DataFrame(c.fetchall())
df1.columns = [i[0] for i in c.description]
# For this table, I used an inner join so that I'm only looking at rows that contain values in both tables so I can be sure there's a mismatch.

# Exporting as .csv
df1.to_csv(r'/Users/Thomas/Desktop/Outputs/CSVs/Price_Deltas.csv', index = False)

# Table 2:
# “Missing_From_Price_Data.csv”: a csv that contains any beverage or food products that have a price listed in the Register_Data, but not present in the Price_Data
c.execute("""SELECT r.ItemNumber, r.PriceZoneNumber, r.PriceZoneType, r.Price AS PriceRegister,
             p.PriceData, 
             ABS(p.PriceData - r.Price) AS PriceDelta
             FROM currentprices r
             LEFT JOIN pricedata p
             ON 
             p.ItemZone = r.ItemZone
             WHERE p.PriceData IS NULL;""")
df2 = pd.DataFrame(c.fetchall())
df2.columns = [i[0] for i in c.description]
# For this second table, I used a left join starting with Register data but specified that I only want rows that have a price missing from the Price DB

# Exporting as .csv
df2.to_csv(r'/Users/Thomas/Desktop/Outputs/CSVs/Missing_From_Price_Data.csv', index = False)

# Table 3:
# “Missing_From_Register_Data.csv”: a csv that contains any beverage or food products that have a price listed in the Price_Data, but not present in the Register_Data
c.execute("""SELECT p.ItemNumber, p.ZoneNumber, p.FoodOrBev AS PriceZoneType, 
             r.Price AS PriceRegister,
             p.PriceData, 
             ABS(p.PriceData - r.Price) AS PriceDelta
             FROM pricedata p
             LEFT JOIN currentprices r
             ON 
             p.ItemZone = r.ItemZone
             WHERE r.Price IS NULL;""")
df3 = pd.DataFrame(c.fetchall())
df3.columns = [i[0] for i in c.description]
# For the third table, I used a left join starting with Price data but specified that I only want rows that have a price missing from the Register DB

# Exporting as .csv
df3.to_csv(r'/Users/Thomas/Desktop/Outputs/CSVs/Missing_From_Register_Data.csv', index = False)

# Finally, I'll now create and populate the Audit DB to store the 3 tables I created:
conn3 = sqlite3.connect("Audits.sqlite3")
c3 = conn3.cursor()

df1.to_sql("Price_Deltas", conn3, index = False)
df2.to_sql("Missing_From_Price_Data", conn3, index = False)
df3.to_sql("Missing_From_Register_Data", conn3, index = False)

# Thanks for reading - I had a great time going through this challenge!

# Thomas Brown