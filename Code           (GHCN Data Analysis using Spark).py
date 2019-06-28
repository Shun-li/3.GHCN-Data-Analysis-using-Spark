
hdfs dfs -du -h hdfs:///data/ghcnd

hdfs dfs -du -h hdfs:///data/ghcnd/daily


./start-pyspark.sh 
# Imports

from pyspark.sql.types import *
import pyspark.sql.functions as F

# Load

schema_daily = StructType([
    StructField("ID1", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", StringType(), True)
])

daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily/2017.csv.gz")).limit(1000)








daily.show(5,False)



schema_station = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", IntegerType(), True),
    StructField("LONGITUDE", IntegerType(), True),
    StructField("ELEVATION", IntegerType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN FLAG", StringType(), True),
    StructField("HCN/CRN FLAG", StringType(), True),
    StructField("WMO ID", StringType(), True)
])
station = (
    spark.read.text("hdfs:///data/ghcnd/stations")
)
from pyspark.sql.functions import trim

stations = station.select(
              station.value.substr(1,11).alias('ID'),
              station.value.substr(13,8).alias('LATITUDE'),
              station.value.substr(22,9).alias('LONGITUDE'),
              station.value.substr(32,6).alias('ELEMENT'),
              station.value.substr(39,2).alias('STATE'),
              station.value.substr(42,30).alias('NAME'),
              station.value.substr(73,3).alias('GSN_FLAG'),
              station.value.substr(77,3).alias('HCN_CRN_FLAG'),
              trim(station.value.substr(81,5)).alias('WMO_ID')
)


stations1 = stations.withColumn(
           "WMO_ID1",
           trim(stations.WMO_ID)
)

stations1_null = stations1.filter(stations1.WMO_ID1 =="")
stations1_null.count()

stations1.show(5,False)



schema_countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME1", StringType(), True)
])


countries = (
    spark.read.text("hdfs:///data/ghcnd/countries")
)

countries = countries.select(
              countries.value.substr(1,2).alias('CODE'),
              countries.value.substr(4,47).alias('NAME1')
)


stations = stations.withColumn(
           "COUNTRY_CODE",
           stations.ID.substr(1,2)
)




stations.show(5,False)

station_country = stations.join(
               countries, stations.COUNTRY_CODE == countries.CODE, "left").distinct()



schema_state = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True)
])
state = (
    spark.read.text("hdfs:///data/ghcnd/states")
)

states = state.select(
              state.value.substr(1,2).alias('CODE'),
              state.value.substr(4,47).alias('NAME3')
)


station_state = stations.join(states,stations.STATE == states.CODE,"left")




schema_inventory = StructType([
             StructField("ID2", StringType(), True),
             StructField("LATITUDE", IntegerType(), True),
             StructField("LONGITUDE", IntegerType(), True),
             StructField("ELEMENT", StringType(), True),
             StructField("FIRSTYEAR", IntegerType(), True),
             StructField("LASTYEAR", IntegerType(), True)
])




inventory = (spark.read.text("hdfs:///data/ghcnd/inventory"))
 
inventory = inventory.select(
            inventory.value.substr(1,11).alias('ID2'),
            inventory.value.substr(13,8).alias('LATITUDE'),
            inventory.value.substr(22,9).alias('LONGITUDE'),
            inventory.value.substr(32,4).alias('ELEMENT'),
            inventory.value.substr(37,4).alias('FIRSTYEAR'),
            inventory.value.substr(42,4).alias('LASTYEAR')
        )


#### Method1:

inventory_year = inventory.groupBy('ID2').agg({'FIRSTYEAR': 'min','LASTYEAR': 'max'})

### Method 2:

inventory_year = inventory.groupBy('ID2').agg(F.min("FIRSTYEAR").alias("FirstYear"),
                                            F.max("LASTYEAR").alias('LastYear')
                                            )



#### Method1:



inventory_element = inventory.groupBy('ID2').agg((F.count("ELEMENT").alias("DISTINCT_ELEMENT")))



inventory_core_1 = inventory.filter(inventory.ELEMENT == "PRCP").distinct()
inventory_core_2 = inventory.filter(inventory.ELEMENT == "SNOW").distinct()
inventory_core_3 = inventory.filter(inventory.ELEMENT == "SNWD").distinct()
inventory_core_4 = inventory.filter(inventory.ELEMENT == "TMAX").distinct()
inventory_core_5 = inventory.filter(inventory.ELEMENT == "TMIN").distinct()
inventory_core = inventory_core_1.unionAll(inventory_core_2)
inventory_core = inventory_core.unionAll(inventory_core_3)
inventory_core = inventory_core.unionAll(inventory_core_4)
inventory_core = inventory_core.unionAll(inventory_core_5)

inventory_core = inventory_core.groupBy('ID2').agg((F.count("ELEMENT").alias("DISTINCT_CORE_ELEMENT")))



inventory_other_1 = inventory.filter(inventory.ELEMENT != "PRCP").distinct()
inventory_other_2 = inventory_other_1.filter(inventory_other_1.ELEMENT != "SNOW").distinct()
inventory_other_3 = inventory_other_2.filter(inventory_other_2.ELEMENT != "SNWD").distinct()
inventory_other_4 = inventory_other_3.filter(inventory_other_3.ELEMENT != "TMAX").distinct()
inventory_other = inventory_other_4.filter(inventory_other_4.ELEMENT != "TMIN").distinct()


inventory_other = inventory_other.groupBy('ID2').agg((F.count("ELEMENT").alias("DISTINCT_OTHER_ELEMENT")))




##### Method2:



core_elements = ["PRCP","SNOW","SNWD","TMAX","TMIN"]
inventory = inventory.withColumn(
                "core_flag",
                F.when(inventory.ELEMENT.isin(core_elements),1)
                .otherwise(0)
                )
inventory_enriched = inventory.groupBy('ID2').agg(F.count("ELEMENT").alias("elements_count"),
                                            F.sum("core_flag").alias('core_elements'),
                                            (F.count("ELEMENT") - F.sum('core_flag')).alias("other_elements")
                                            )





inventory_core_all = inventory_enriched.filter(inventory_enriched.core_elements == 5)
inventory_core_all.count()





station_inventory = stations.join(inventory_enriched,stations.ID == inventory_enriched.ID2,"left")

station_inventory_daily = station_inventory.join(daily,station_inventory.ID == daily.ID1,"left")
station_inventory_daily = station_inventory_daily.drop(station_inventory_daily.ID1)
station_inventory_daily = station_inventory_daily.drop(station_inventory_daily.ID2)

station_inventory_daily.show(5,False)




#####Part2

################## Analysis  #############

./start-pyspark.sh -e 4 -c 2 -w 4 -m 4

#Q1:
##(a)
stations.count()


inventory_year = inventory.filter((inventory.FIRSTYEAR == 2017) | (inventory.LASTYEAR == 2017))
inventory_year_1 = inventory_year.groupBy('ID2').agg((F.count("ELEMENT").alias("DISTINCT_OTHER_ELEMENT")))
inventory_year_1.count()


stations.filter(stations.GSN_FLAG == "GSN").count()# Number of GSN
stations.filter(stations.HCN_CRN_FLAG == "HCN").count()# Number of HCN -----@1
stations.filter(stations.HCN_CRN_FLAG == "CRN").count()# Number of CRN -----@2

stations1 = stations.withColumn(
           "HCN_CRN_FLAG1",
           trim(stations.HCN_CRN_FLAG))
stations1.filter(stations1.HCN_CRN_FLAG1 == "").count()# Number of (null)----@3


a = stations.filter(stations.HCN_CRN_FLAG != "HCN")
a.filter(a.HCN_CRN_FLAG != "CRN").count() # Number of (not HCN & not CRN)----@4


stations.count() # Number of Total------@4

# @1 + @2 + @3 = @4



stations.filter(
                (stations.GSN_FLAG == "GSN") 
              & (stations.HCN_CRN_FLAG == "HCN")).count()


stations.filter(
                (stations.GSN_FLAG == "GSN") 
              & (stations.HCN_CRN_FLAG == "CRN")).count()

##(b)
country_station_number = station_country.groupBy(
    'NAME1').agg((F.count("ID").alias("Count_of_stations")))
country_station_number.show()


m = country_station_number.withColumnRenamed("NAME1", "NAME")
countries = countries.join(m,countries.NAME1 == m.NAME,"left").drop("NAME1")



state_station_number = station_state.groupBy('NAME3').agg((F.count("ID").alias("Count of stations")))
state_station_number.show()

n = state_station_number.withColumnRenamed("NAME3", "NAME")
states = states.join(n,states.NAME3 == n.NAME,"left").drop("NAME3")


stations.filter(stations.LONGITUDE < 0).count()




##(c)

Territories_of_USA = countries.filter(
      (countries.NAME.endswith("[United States] "))
     |(countries.NAME.endswith("[United States]"))
     |(countries.NAME.endswith("[United States]  "))
     |(countries.NAME.endswith("[United States} "))
     )




#Q2:


from pyspark.sql.functions import udf
from math import radians, cos, sin, asin, sqrt

#udf
def haversine(lon1, lat1, lon2, lat2):
    """Calcute the  great circle distance between two points on the earth """
    lon1, lat1, lon2, lat2 = map(radians,[lon1,lat1,lon2,lat2])
    # haversine
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2  + cos(lat1) *sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371
    return (c*r *1000)


#Test:
station1 = stations.filter(stations.ID =="ACW00011604")
station2 = stations.filter(stations.ID =="AE000041196")

lon2 =   174.9830 #station1.LONGITUDE
lat2 =  -40.9000  #station1.LATITUDE
lon1 =  174.8000  #station2.LONGITUDE
lat1 =  -41.3330   #station2.LATITUDE
distance = haversine(lon1, lat1, lon2, lat2)





NZ_Stations = stations.filter(stations.COUNTRY_CODE == 'NZ')

NZ_Stations.show(50,False)

LATITUDE_LONGITUDE =[
                     (-35.1000,173.2670),
                     (-38.6500,177.9830),
                     (-40.9000,174.9830),
                     (-46.4170,168.3330),
                     (-29.2500,-177.9170),
                     (-39.0170,174.1830),
                     (-42.7170,170.9830),
                     (-44.5170,169.9000),
                     (-52.5500,169.1670),
                     (-43.9500,-176.5670),
                     (-37.0000,174.8000),
                     (-41.3330,174.8000),
                     (-42.4170,173.7000),
                     (-43.4890,172.5320),
                     (-50.4830,166.3000)
                    ]




result = []
for (lat1,lon1) in LATITUDE_LONGITUDE:
    for (lat2,lon2) in LATITUDE_LONGITUDE:
        distance = haversine(lon1, lat1, lon2, lat2)
        result.append(distance)

result2 = []
for i in result:
    if i not in result2:
        result2.append(i)
result2.remove(0)

a = result.index(min(result2)) 

b = len(LATITUDE_LONGITUDE)

station1_index = int((a+1)/b + 1)####the index of row (station1) in NZ_station
station2_index = (a+1)%b  ####the index of row (station2) in NZ_station





#Q3:
hadoop fs -mkdir <hdfs:///user/sli171/output/ghcnd>

 
 #(a)



hdfs getconf -confKey "dfs.blocksize"
#get a specific key from the configuration

hdfs dfs -du -h hdfs:///data/ghcnd/daily/2017.csv.gz

hdfs dfs -du -h hdfs:///data/ghcnd/daily/2010.csv.gz


 #the individual block sizes for the year 2010
 
hdfs  fsck data/ghcnd/daily/2010.csv.gz
hdfs fsck ///data/ghcnd/daily/2017.csv.gz -files -blocks -locations -racks

 #(b)

 # Imports

from pyspark.sql.types import *
import pyspark.sql.functions as F

# Load


##2010

schema_daily = StructType([
    StructField("ID1", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", StringType(), True)
])

daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily/2010.csv.gz"))

daily.count()



##2017

schema_daily = StructType([
    StructField("ID1", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", StringType(), True)
])

daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily/2017.csv.gz"))

daily.count()


## 2010 --- 2015
daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily/201[0-5].csv.gz"))

daily.count()





##4

#####  a
from pyspark.sql.types import *
import pyspark.sql.functions as F

# Load

schema_daily = StructType([
    StructField("ID1", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", FloatType(), True),
    StructField("MEASUREMENT FLAG", StringType(), True),
    StructField("QUALITY FLAG", StringType(), True),
    StructField("SOURCE FLAG", StringType(), True),
    StructField("OBSERVATION TIME", StringType(), True)
])

daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily"))

daily.count()


#####   b

core_elements = ["PRCP","SNOW","SNWD","TMAX","TMIN"]

daily_core_1 = daily.filter(daily.ELEMENT == "PRCP").distinct()
daily_core_2 = daily.filter(daily.ELEMENT == "SNOW").distinct()
daily_core_3 = daily.filter(daily.ELEMENT == "SNWD").distinct()
daily_core_4 = daily.filter(daily.ELEMENT == "TMAX").distinct()
daily_core_5 = daily.filter(daily.ELEMENT == "TMIN").distinct()
daily_core = daily_core_1.unionAll(daily_core_2)
daily_core = daily_core.unionAll(daily_core_3)
daily_core = daily_core.unionAll(daily_core_4)
daily_core = daily_core.unionAll(daily_core_5)
daily_core.show(5,False)


daily_core_all = daily_core.groupBy('ELEMENT').agg((F.count("ELEMENT").alias("DISTINCT_CORE_ELEMENT")))

daily_core_all.show(5,False)




##### c

















##### d

daily = (
         sqlContext
         .read
         .format("com.databricks.spark.csv")
         .option("header", "false")
         .schema(schema_daily)
         .load("hdfs:///data/ghcnd/daily"))

dail_tmax = daily.filter(daily.ELEMENT == "TMAX").distinct()
dail_tmin = daily.filter(daily.ELEMENT == "TMIN").distinct()
daily_tmax_tmin = dail_tmax.unionAll(dail_tmin)
daily_tmax_tmin_nz = daily_tmax_tmin.filter(daily_tmax_tmin.ID1.startswith("NZ"))
daily_tmax_tmin_nz.show(5,False)

daily_tmax_tmin_nz.count()

daily_tmax_tmin_nz.write.format("csv").save("hdfs:///user/sli171/outputs/ghcnd/daily_tmax_tmin_nz")


##Copy to local:

hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/ghcnd/daily_tmax_tmin_nz


daily_year_nz = daily_tmax_tmin_nz.select(daily_tmax_tmin_nz.ID1,
                    (daily_tmax_tmin_nz.DATE[1:4]).alias("YEAR")                                 
                     )
daily_year_nz.show(5,False)

year_summary = daily_year_nz.groupBy('YEAR').agg((F.count("YEAR").alias("DISTINCT_YEAR")))
year_summary.show(5,False)
year_summary.count()




##save:

daily_tmax_tmin_nz.write.format("csv").save("hdfs:///user/sli171/outputs/ghcnd/daily_tmax_tmin_nz")


##Copy to local:

hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/ghcnd/daily_tmax_tmin_nz




##### e
daily_prcp = daily.filter(daily.ELEMENT == "PRCP").distinct()

daily_prcp_year_country = daily_prcp.select(daily_prcp.ID1,
                                           (daily_prcp.ID1[1:2]).alias("COUNTRY_CODE"),
                                           (daily_prcp.DATE[1:4]).alias("YEAR"),
                                           daily_prcp.VALUE                                 
                                           )

daily_prcp_year_country.show(5,False)

dail_prcp_country = daily_prcp_year_country.groupBy('COUNTRY_CODE',"YEAR").agg((F.mean("VALUE").alias("Average_Value")))
dail_prcp_country.show(5,False)




##save:

dail_prcp_country.write.format("csv").save("hdfs:///user/sli171/outputs/ghcnd/dail_prcp_country")


##Copy to local:

hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/ghcnd/dail_prcp_country 






























































