# Databricks notebook source
import os
import urllib.parse
import glob
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import re
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.types import DecimalType,StringType,ArrayType,FloatType,DoubleType,IntegerType
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix

# COMMAND ----------

# spark = SparkSession.builder.appName("VirtualLocation").getOrCreate()
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.databricks.delta.formatCheck.enabled","false")

# COMMAND ----------

storage_account_name = 'anmolproddatalakestorage'
storage_account_access_key = "YAkMH3wU+Z2C7zcSsJ9fGbcVxmvmOmp7LdVHbFbo3ArfHfwVdA/qqGH2Uhe3xX5SL5JCuou+0Ocy+AStxabb3g=="
spark.conf.set('fs.azure.account.key.'+storage_account_name+'.blob.core.windows.net', storage_account_access_key)
blob_container = "anmol-prod"

# COMMAND ----------

# MAGIC %md # Functions

# COMMAND ----------

#############################################################################################################################################################
def read_csv(table_name,_delimeter):
    file_path = "wasbs://"+blob_container+"@"+storage_account_name+".blob.core.windows.net"+"/SAP/"+table_name
    df = spark.read.option("quote","").option("recursiveFileLookup", "true").format("csv").load(file_path, inferSchema=False, header = True, delimiter = _delimeter , quote="\u0000") #.option('multiLine', 'true')
    df = df.select([trim(col(c)).alias(c) for c in df.columns])
    df = df.select([regexp_replace(col(c), r'^[0]*', '').alias(c) for c in df.columns])
    # df = df.select([col(col).alias(col.replace(' ', '_')) for col in df.columns])
    return df
#############################################################################################################################################################

def write_parquet(_df,table_name):
    file_path = "wasbs://"+blob_container+"@"+storage_account_name+".blob.core.windows.net"+"/curated/"+table_name
    _df.write.mode('overwrite').parquet(file_path)
    print("The table ",table_name," has been loaded")
#############################################################################################################################################################

def write_curated(table_name,_delimeter):
        write_parquet(read_csv(table_name,_delimeter),table_name)

#############################################################################################################################################################
def read_curated_data(name):
    file_path = r"wasbs://"+blob_container+"@"+storage_account_name+".blob.core.windows.net"+"/curated/"+ name
    df = spark.read.parquet(file_path)
    print(f'{name} table loaded in df from parquet')
    return df

# COMMAND ----------

# MAGIC %md #Table Loading

# COMMAND ----------

table_list = ["T151T","KNA1","MARA","ZCUST_ADD_DETAIL","MARC","CEPC","VBRP","VBRK", "KNVV"] 
for table in table_list:
    try:
        write_curated(table,"^")
    except Exception as error:
           print(f"Error loop : {str(error)}")

# COMMAND ----------

#Manual file "," comma separated delimeter
write_curated("focus_brand",",")
write_curated("customer_budget",",")
write_curated("Asm_Budget",",")
write_curated("material_budget",",")

# COMMAND ----------

T151T_read = read_curated_data("T151T")
KNA1_read = read_curated_data("KNA1")
MARA_read = read_curated_data("MARA")
VBRP_read = read_curated_data("VBRP")
VBRK_read = read_curated_data("VBRK")
ZCUST_ADD_DETAIL_read = read_curated_data("ZCUST_ADD_DETAIL")
MARC_read = read_curated_data("MARC")
CEPC_read = read_curated_data("CEPC")
focus_brand_read = read_curated_data("focus_brand")
customer_budget = read_curated_data("customer_budget")
asm_budget = read_curated_data("Asm_Budget")
material_budget = read_curated_data("material_budget")
KNVV_read = read_curated_data("KNVV")

# COMMAND ----------

display(KNA1_read.select("KUNNR").distinct())

# COMMAND ----------

# MAGIC %md ### Material Budget Logic 

# COMMAND ----------

from pyspark.sql.functions import lit, explode, sequence, expr
from pyspark.sql.types import DateType, IntegerType

fiscal_year_start_month = 4  # April (change as per your fiscal year start)

df = material_budget.withColumn("Current_Date", current_date())

df = df.withColumn("Fiscal_Year", (year(col("Current_Date")) 
                                    + ((month(col("Current_Date")) >= fiscal_year_start_month).cast("int")))-1).drop("Current_Date")

# Define the month fiscal year starts (April for this example)
start_month_of_fiscal_year = 4 

# Add date columns for beginning and end of fiscal year
df = df.withColumn("Start", expr(f"concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01')").cast("date"))
df = df.withColumn("End", expr(f"add_months(concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01'), 11)"))

# Create sequence of months within each fiscal year and explode it into separate rows
material_budget_1 = df.withColumn("Month", explode(sequence(to_date(col("Start")), to_date(col("End")), expr("interval 1 month")))).withColumn("Month_formatted",to_date(col("Month"),"yyyy-MM-dd"))

# COMMAND ----------

# display(material_budget_1)

# COMMAND ----------

# MAGIC %md ### Material Budget Ingestion

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

material_budget_1.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "material_budget_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

KNVV  = KNVV_read.select("KUNNR", "VKORG", "KDGRP")
KNVV = KNVV.join(T151T_read, "KDGRP","left").select(KNVV['*'], T151T_read["KTEXT"])

# COMMAND ----------

# display(KNVV)

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

KNVV.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "customer_group_desc") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

    #   customer_budget.columns

# COMMAND ----------

from pyspark.sql.functions import lit, explode, sequence, expr
from pyspark.sql.types import DateType, IntegerType

fiscal_year_start_month = 4  # April (change as per your fiscal year start)

df = customer_budget.withColumn("Current_Date", current_date())

df = df.withColumn("Fiscal_Year", (year(col("Current_Date")) 
                                    + ((month(col("Current_Date")) >= fiscal_year_start_month).cast("int")))-1).drop("Current_Date")

# Define the month fiscal year starts (April for this example)
start_month_of_fiscal_year = 4 

# Add date columns for beginning and end of fiscal year
df = df.withColumn("Start", expr(f"concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01')").cast("date"))
df = df.withColumn("End", expr(f"add_months(concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01'), 12)"))

# Create sequence of months within each fiscal year and explode it into separate rows
customer_budget_1 = df.withColumn("Month", explode(sequence(to_date(col("Start")), to_date(col("End")), expr("interval 1 month")))).withColumn("Month_formatted",to_date(col("Month"),"yyyy-MM-dd"))

# COMMAND ----------

from pyspark.sql.functions import lit, explode, sequence, expr
from pyspark.sql.types import DateType, IntegerType

fiscal_year_start_month = 4  # April (change as per your fiscal year start)

df = asm_budget.withColumn("Current_Date", current_date())

df = df.withColumn("Fiscal_Year", (year(col("Current_Date")) 
                                    + ((month(col("Current_Date")) >= fiscal_year_start_month).cast("int")))-1).drop("Current_Date")

# Define the month fiscal year starts (April for this example)
start_month_of_fiscal_year = 4 

# Add date columns for beginning and end of fiscal year
df = df.withColumn("Start", expr(f"concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01')").cast("date"))
df = df.withColumn("End", expr(f"add_months(concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01'), 12)"))

# Create sequence of months within each fiscal year and explode it into separate rows
asm_budget_1 = df.withColumn("Month", explode(sequence(to_date(col("Start")), to_date(col("End")), expr("interval 1 month")))).withColumn("Month_formatted",to_date(col("Month"),"yyyy-MM-dd"))

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

asm_budget_1.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "asm_budget_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %md #Part 1 Customer Master

# COMMAND ----------



# COMMAND ----------

KNA1 = KNA1_read.withColumn("block_status", when(col("AUFSD").cast('int') == 1, "blocked").otherwise("unblocked"))
KNA1 = KNA1.select([trim(col(c)).alias(c) for c in KNA1.columns])
KNA1 = KNA1.select('KUNNR','NAME1', 'REGIO', 'AUFSD', 'ZZDT','ZZTN','ZZGP' ,'ZZML', 'ZZONE','block_status')
KNA1 = KNA1.withColumnRenamed('KUNNR', 'customer_code').withColumnRenamed('NAME1', 'customer_name')\
.withColumnRenamed('REGIO', 'state_code').withColumnRenamed('AUFSD', 'central_block_code')\
.withColumnRenamed('ZZDT', 'district').withColumnRenamed('ZZTN', 'town_village')\
.withColumnRenamed('ZZGP', 'region').withColumnRenamed('ZZML', 'block_tehsil')\
.withColumnRenamed('ZZONE', 'zone')

# COMMAND ----------

state_mapping_data = [('1',	'ANDHRA PRADESH'),
                      ('2','ARUNACHAL PRADESH'),
                      ('3','ASSAM'),
                      ('4','BIHAR'),
                      ('5','GOA'),
                      ('6','GUJARAT'),             
('7','HARYANA'),             
('8','HIMACHAL PRADESH'),    
('9','JAMMU AND KASHMIR'),   
('10','KARNATAKA'),           
('11','KERALA'),              
('12','MADHYA PRADESH'),      
('13','MAHARASHTRA'),         
('14','MANIPUR'),             
('15','MEGHALAYA'),           
('16','MIZORAM'),             
('17','NAGALAND'),            
('18','ODISHA'),              
('19','PUNJAB'),              
('20','RAJASTHAN'),           
('21','SIKKIM'),              
('22','TAMIL NADU'),          
('23','TRIPURA'),             
('24','UTTAR PRADESH'),       
('24A','UP-NCR'),              
('25','WEST BENGAL'),         
('26','ANDAMAN AND NICOBAR'), 
('27','CHANDIGARH'),          
('28','DADRA AND NAGAR HAVE'),
('29','Daman and Diu'),       
('30','DELHI'),               
('31','LAKSHADWEEP'),         
('32','PONDICHERRY'),         
('33','CHHATTISGARH'),        
('34','JHARKHAND'),           
('35','UTTARAKHAND'),         
('36','TELANGANA'), 
('37','Not defined in SAP'),
('38','LADAKH'),
('A1','Not defined in SAP'),
('A2','Not defined in SAP'),
('A3','Not defined in SAP'),
('A4','Not defined in SAP'),
('KM','SRINAGAR'), 
]

# COMMAND ----------

state_mapping_schema = ["state_code", "state_name"]
state_mapping = spark.createDataFrame(state_mapping_data, schema=state_mapping_schema)

# COMMAND ----------

KNA1 = KNA1.join(state_mapping, "state_code", "left")

# COMMAND ----------

#display(KNA1.select("state_code","state_name").distinct())

# COMMAND ----------

columns_to_proper_case = ["customer_name", "district", "town_village","region","block_tehsil","state_name" ]
for column in columns_to_proper_case:
    KNA1 = KNA1.withColumn(column, initcap(col(column)))

# COMMAND ----------

sales_base_data_kna1 = ZCUST_ADD_DETAIL_read

# COMMAND ----------

sales_force_dict = {"KUNNR":"customer_code", "KUNR1":"ss_custome", "FSSIV":"fssi_valid", "INPLN":"invst._amt", "WAERS":"crcy", "ASSDT":"asscn._dat", "ASSYR":"no._of_yrs", "BSCTS":"biscuits", "CKNCK":"cakes", "BOTH0":"cookies", "RUSK0":"rusk", "PANCD":"pan_card", "FOODL":"food_licen", "GSTIN":"gstin", "PDC_1":"pdc_chq._1", "PDC_2":"pdc_chq._2", "PDC_3":"pdc_chq._3", "BANKN":"bank_name", "MSME1":"miscellane", "SONAME":"so_name", "EMPCD1":"so_emp_code", "SMTP_1":"so_email", "DATE1":"date", "ASMNAME":"asm_name", "EMPCD2":"asm_emp_code", "SMTP_2":"asm_email", "DATE2":"asm_code_creation_date", "RSMNAME":"rsm_name", "EMPCD3":"rsm_emp_code", "SMTP_3":"rsm_email", "DATE3":"rsm_code_creation_date", "NWUPD":"new/update", "REGDT":"reg._date", "ZSMNAME":"zsm_name", "EMPCD5":"zsm_emp_code", "ZSMMAIL":"zsm_email", "DATE5":"zsm_code_creation_date", "NSMNAME":"nsm_name", "EMPCD6":"nsm_emp_code", "NSMMAIL":"nsm_email", "DATE6":"nsm_code_creation_date", "DSM_CODE":"dsm_code", "DSM_NAME":"dsm_name", "DSM_MAIL":"dsm_mail", "SR_CODE":"sr_code", "SR_NAME":"sr_name", "SR_MAIL":"sr_mail","MANDT":"company_code"}

# COMMAND ----------

for old_col, new_col in sales_force_dict.items():
    sales_base_data_kna1 = sales_base_data_kna1.withColumnRenamed(old_col, new_col)

# COMMAND ----------

sales_base_data_kna1 = sales_base_data_kna1.select("customer_code","so_name","asm_name","rsm_name","zsm_name","so_email","asm_email","rsm_email","zsm_email")

# COMMAND ----------

KNA1 = KNA1.join(sales_base_data_kna1,['customer_code'],'left')

# COMMAND ----------

# driver = "org.mariadb.jdbc.Driver"
# database_host = "anmolmysqldb-flex.mysql.database.azure.com"
# database_port = "3306"
# database_name = "anmol_dev"

# url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

# KNA1.write \
#   .format("jdbc") \
#   .option("driver", driver) \
#   .option("url", url) \
#   .option("dbtable", "customer_master_prod") \
#   .option("user", "dbadmin") \
#   .option("password", "Welcome2tata#") \
#   .option("sslmode", "require") \
#   .mode("overwrite") \
#   .save()

# COMMAND ----------

#display(KNA1)

# COMMAND ----------

# MAGIC %md #Material Master

# COMMAND ----------

mara_dict = {"MANDT":"company_code","MATNR": "material_number", "ERSDA": "created_on", "CREATED_AT_TIME": "created_on_time", "ERNAM": "name_of_person_who_created_the_object", "LAEDA": "date_of_last_change", "AENAM": "name_of_person_who_changed_object", "VPSTA": "maintenance_status_of_complete_material", "PSTAT": "maintenance_status", "LVORM": "flag_material_for_deletion_at_client_level", "MTART": "material_type", "MBRSH": "industry_sector", "MATKL": "material_group", "BISMT": "old_material_number", "MEINS": "base_unit_of_measure", "BSTME": "purchase_order_unit_of_measure", "BRGEW": "gross_weight", "NTGEW": "net_weight", "GEWEI": "weight_unit", "VOLUM": "volume", "VOLEH": "volume_unit", "DISST": "low-level_code", "TRAGR": "transportation_group", "STOFF": "hazardous_material_number", "SPART": "division", "WESCH": "quantity:_number_of_gr/gi_slips_to_be_printed", "GEWTO": "excess_weight_tolerance_for_handling_unit", "VOLTO": "excess_volume_tolerance_of_the_handling_unit", "VABME": "variable_purchase_order_unit_active", "KZREV": "revision_level_has_been_assigned_to_the_material", "KZKFG": "configurable_material", "XCHPF": "batch_management_requirement_indicator", "VHART": "packaging_material_type", "FUELG": "maximum_level_(by_volume)", "STFAK": "stacking_factor", "MAGRV": "material_group_packaging_materials", "EXTWG": "external_material_group", "MHDRZ": "minimum_remaining_shelf_life", "MHDHB": "total_shelf_life", "MHDLP": "storage_percentage", "INHME": "content_unit", "INHAL": "net_contents", "VPREH": "comparison_price_unit", "IPRKZ": "period_indicator_for_shelf_life_expiration_date", "RDMHD": "rounding_rule_for_calculation_of_sled", "PRZUS": "indicator:_product_composition_printed_on_packaging", "MTPOS_MARA": "general_item_category_group", "BFLME": "generic_material_with_logistical_variants", "MATFI": "material_is_locked", "CMREL": "relevant_for_configuration_management", "BBTYP": "assortment_list_type", "SLED_BBD": "expiration_date", "MAXC": "maximum_allowed_capacity_of_packaging_material", "MAXC_TOL": "overcapacity_tolerance_of_the_handling_unit", "LAST_CHANGED_TIME": "chgd.time", "MATNR_EXTERNAL": "material", "CHML_CMPLNC_RLVNCE_IND": "chemicalcompliancerelevanceindicator", "SCM_MATID_GUID16": "product", "SCM_MATID_GUID22": "productid", "OVERHANG_TRESH": "threshold", "BRIDGE_TRESH": "bridge", "BRIDGE_MAX_SLOPE": "max.slope", "/SAPMP/KADU": "cabledia.", "/SAPMP/ABMEIN": "u.fordim.", "/SAPMP/KADP": "ballow", "/SAPMP/BRAD": "bendfactor", "/SAPMP/SPBI": "innerwidth", "/SAPMP/TRAD": "outerdia.", "/SAPMP/KEDU": "coredia.", "/SAPMP/SPTR": "loadcap.", "/SAPMP/FBDK": "rofthick.", "/SAPMP/FBHK": "rofheight", "/SAPMP/MIFRR": "clearance", "/STTPEC/SERTYPE": "ser.type", "RIC_ID": "ricid", "ZZBRD": "brand", "ZZPBD": "prodbrand", "ZZMRP": "mrp", "ZZPCT": "prodcat", "ZZSZE": "packsize", "ZZFMT": "format", "ZZMRPCB": "mrp/cb", "ZZNOBIS": "no_bisc_pk", "ZZSECPK": "sec.pack", "ZZSECQTY": "secondarypackqty", "ZZSECTYP": "sec_pktyp", "ZZTRADPT": "tradept", "ZZREMARK": "remarks", "ZZBCODE": "barcode", "ZZBFLV": "brandflav", "MaterialDescription": "materialdescription", "Materialdescription": "materialdescription"}

# COMMAND ----------

mara_1 = MARA_read.select("MATNR",	"ERSDA",	"CREATED_AT_TIME",	"ERNAM",	"LAEDA",	"AENAM",	"VPSTA",	"PSTAT",	"LVORM",	"MTART",	"MBRSH",	"MATKL",	"BISMT",	"MEINS",	"BSTME",	"BRGEW",	"NTGEW",	"GEWEI",	"VOLUM",	"VOLEH",	"DISST",	"TRAGR",	"STOFF",	"SPART",	"WESCH",	"GEWTO",	"VOLTO",	"VABME",	"KZREV",	"KZKFG",	"XCHPF",	"VHART",	"FUELG",	"STFAK",	"MAGRV",	"EXTWG",	"MHDRZ",	"MHDHB",	"MHDLP",	"INHME",	"INHAL",	"VPREH",	"IPRKZ",	"RDMHD",	"PRZUS",	"MTPOS_MARA",	"BFLME",	"MATFI",	"CMREL",	"BBTYP",	"SLED_BBD",	"MAXC",	"MAXC_TOL",	"LAST_CHANGED_TIME",	"MATNR_EXTERNAL",	"CHML_CMPLNC_RLVNCE_IND",	"SCM_MATID_GUID16",	"SCM_MATID_GUID22",	"OVERHANG_TRESH",	"BRIDGE_TRESH",	"BRIDGE_MAX_SLOPE",	"/SAPMP/KADU",	"/SAPMP/ABMEIN",	"/SAPMP/KADP",	"/SAPMP/BRAD",	"/SAPMP/SPBI",	"/SAPMP/TRAD",	"/SAPMP/KEDU",	"/SAPMP/SPTR",	"/SAPMP/FBDK",	"/SAPMP/FBHK",	"/SAPMP/MIFRR",	"/STTPEC/SERTYPE",	"RIC_ID",	"ZZBRD",	"ZZPBD",	"ZZMRP",	"ZZPCT",	"ZZSZE",	"ZZFMT",	"ZZMRPCB",	"ZZNOBIS",	"ZZSECPK",	"ZZSECQTY",	"ZZSECTYP",	"ZZTRADPT",	"ZZREMARK",	"ZZBCODE",	"ZZBFLV")

# COMMAND ----------

for i in range(len(mara_1.columns)):
    try:
        mara_1 = mara_1.withColumnRenamed(mara_1.columns[i], mara_dict[mara_1.columns[i]])
    except:
        pass
mara = mara_1.withColumn("deletion_indicator",when(trim(col("flag_material_for_deletion_at_client_level")).rlike('X'),"deleted").otherwise("not deleted")).withColumn('MATNR_sanitized', regexp_replace('material_number', r'^[0]*', ''))

# COMMAND ----------

marc = MARC_read.select("MATNR","PRCTR").withColumn('MATNR_sanitized', regexp_replace('MATNR', r'^[0]*', ''))

# COMMAND ----------

mara_marc = mara.join(marc,["MATNR_sanitized"],"left")

# COMMAND ----------

cepc = CEPC_read.select("PRCTR","SEGMENT")

# COMMAND ----------

mara_marc_cepc = mara_marc.join(cepc,["PRCTR"],"left")

# COMMAND ----------

mara_marc_cepc = mara_marc_cepc.withColumn("new_segment",when(trim(col("segment")).isin("BISC","COOK"),"BISCUIT & COOKIES").otherwise(col("segment"))).withColumnRenamed("PRCTR","profit_centre").drop("MATNR","MATNR_sanitized").dropDuplicates()

# COMMAND ----------

columns_to_proper_case = ["prodbrand", "brandflav","segment","new_segment"]
for column in columns_to_proper_case:
    mara_marc_cepc = mara_marc_cepc.withColumn(column, initcap(col(column)))

# COMMAND ----------

mara = mara_marc_cepc.drop('profit_centre')
mara = mara.dropDuplicates(['material_number'])

# COMMAND ----------

mara.count()

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

mara.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "material_master_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

#display(mara_marc_cepc)

# COMMAND ----------

# MAGIC %md #Sales Force Master

# COMMAND ----------

#KNA1.columns

# COMMAND ----------

sales_force = ZCUST_ADD_DETAIL_read.join(KNA1,ZCUST_ADD_DETAIL_read.KUNNR == KNA1.customer_code, 'left' ).select(ZCUST_ADD_DETAIL_read['*'], KNA1['customer_name'], KNA1['block_status'],KNA1['zone'],KNA1["region"],KNA1["state_code"],KNA1["town_village"],KNA1["district"],KNA1["state_name"])

# COMMAND ----------

sales_force_dict = {"KUNNR":"customer_code", "KUNR1":"ss_custome", "FSSIV":"fssi_valid", "INPLN":"invst._amt", "WAERS":"crcy", "ASSDT":"asscn._dat", "ASSYR":"no._of_yrs", "BSCTS":"biscuits", "CKNCK":"cakes", "BOTH0":"cookies", "RUSK0":"rusk", "PANCD":"pan_card", "FOODL":"food_licen", "GSTIN":"gstin", "PDC_1":"pdc_chq._1", "PDC_2":"pdc_chq._2", "PDC_3":"pdc_chq._3", "BANKN":"bank_name", "MSME1":"miscellane", "SONAME":"so_name", "EMPCD1":"so_emp_code", "SMTP_1":"so_email", "DATE1":"date", "ASMNAME":"asm_name", "EMPCD2":"asm_emp_code", "SMTP_2":"asm_email", "DATE2":"asm_code_creation_date", "RSMNAME":"rsm_name", "EMPCD3":"rsm_emp_code", "SMTP_3":"rsm_email", "DATE3":"rsm_code_creation_date", "NWUPD":"new/update", "REGDT":"reg._date", "ZSMNAME":"zsm_name", "EMPCD5":"zsm_emp_code", "ZSMMAIL":"zsm_email", "DATE5":"zsm_code_creation_date", "NSMNAME":"nsm_name", "EMPCD6":"nsm_emp_code", "NSMMAIL":"nsm_email", "DATE6":"nsm_code_creation_date", "DSM_CODE":"dsm_code", "DSM_NAME":"dsm_name", "DSM_MAIL":"dsm_mail", "SR_CODE":"sr_code", "SR_NAME":"sr_name", "SR_MAIL":"sr_mail","MANDT":"company_code"}

# COMMAND ----------

for old_col, new_col in sales_force_dict.items():
    sales_force = sales_force.withColumnRenamed(old_col, new_col)

# COMMAND ----------

#Manually added NSM details
sales_force_1 = sales_force.withColumn("nsm_name_lit",lit("Ram Sinha"))\
.withColumn("nsm_email_lit", lit("ram.sinha@anmolindustries.com"))

# COMMAND ----------

columns_to_proper_case = [ "so_name","asm_name","rsm_name","zsm_name","nsm_name","dsm_name","sr_name" ]
for column in columns_to_proper_case:
    sales_force_1 = sales_force_1.withColumn(column, initcap(col(column)))

# COMMAND ----------

#display(sales_force_1)

# COMMAND ----------

# {"KUNNR":"customer",
# "KUNR1":"ss_custome",
# "FSSIV":"fssi_valid",
# "INPLN":"invst._amt",
# "WAERS":"crcy",
# "ASSDT":"asscn._dat",
# "ASSYR":"no._of_yrs",
# "BSCTS":"biscuits",
# "CKNCK":"cakes",
# "BOTH0":"cookies",
# "RUSK0":"rusk",
# "PANCD":"pan_card",
# "FOODL":"food_licen",
# "GSTIN":"gstin",
# "PDC_1":"pdc_chq._1",
# "PDC_2":"pdc_chq._2",
# "PDC_3":"pdc_chq._3",
# "BANKN":"bank_name",
# "MSME1":"miscellane",
# "SONAME":"so_name",
# "EMPCD1":"emp._code",
# "SMTP_1":"email_address",
# "DATE1":"date",
# "ASMNAME":"asm_name",
# "EMPCD2":"emp._code",
# "SMTP_2":"email_address",
# "DATE2":"asm_code_creation_date",
# "RSMNAME":"rsm_name",
# "EMPCD3":"rsm_employe_code",
# "SMTP_3":"rsm_email",
# "DATE3":"rsm_code_creation_date",
# "NWUPD":"new/update",
# "REGDT":"reg._date",
# "ZSMNAME":"zsm_name",
# "EMPCD5":"zsm_employe_code",
# "ZSMMAIL":"zsm_email",
# "DATE5":"zsm_code_creation_date",
# "NSMNAME":"nsm_name",
# "EMPCD6":"nsm_employe_code",
# "NSMMAIL":"nsm_email",
# "DATE6":"nsm_code_creation_date",
# "DSM_CODE":"dsm_code",
# "DSM_NAME":"dsm_name",
# "DSM_MAIL":"dsm_mail",
# "SR_CODE":"sr_code",
# "SR_NAME":"sr_name",
# "SR_MAIL":"sr_mail" }


# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

sales_force_1.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "sales_force_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

# MAGIC %md #Sales Data

# COMMAND ----------

#Loading VBRP table
vbrp_1 = VBRP_read.select(col("PRCTR").alias("profit_center"),col("WERKS").alias("plant"),col("PSTYV").alias("item_category"),col("VKORG_AUFT").alias("sales_org"),col("MWSBP").alias("tax_amount"),col("VBELN").alias("billing_document"),col("POSNR").alias("billing_item"),col("MATNR").alias("material_code"),col("ARKTX").alias("material_description"),col("FKIMG").alias("quantity"),col("VRKME").alias("UoM"),col("FKLMG").alias("billed_quantity"),col("NETWR").alias("net_value"),col("NTGEW").alias("qty_in_kg")).fillna(0,['net_value','tax_amount']).withColumn("billing_amount",col('net_value')+col('tax_amount'))

# COMMAND ----------

mara_1 = mara_marc_cepc.select(col("material_number").alias("material_code"),col("prodcat").alias("product_category"),col("prodbrand").alias("product_brand"),col("brandflav").alias("brand_flavour"),"mrp"	,col("packsize").alias("pack_size"),col("format").alias("pack_format"),"mrp/cb",col('no_bisc_pk').alias("no_of_bisc/pk"),col("secondarypackqty").alias("secondary_pack_qty"),col("sec_pktyp").alias("secondary_pkt_type"),col("material_group"),col("new_segment").alias("business_segment"))

# COMMAND ----------

vbrk_1 = VBRK_read.select(col("VBELN").alias("billing_document"),col("FKDAT").alias("billing_date"),col("FKART").alias("billing_type"),col("KDGRP").alias("customer_group"),col("KUNAG").alias("customer_code"),col("XBLNR").alias("ref_invoice_number"))

# COMMAND ----------

t151t_1 = T151T_read.select(col("KDGRP").alias("customer_group"),col("KTEXT").alias("customer_group_description"))

# COMMAND ----------

sales_force_1_1 = sales_force_1.select("customer_code","customer_name","region","state_code","town_village","district","zone","zsm_emp_code",col("zsm_name").alias("zsm"),"zsm_email","rsm_emp_code",col("rsm_name").alias("rsm"),"rsm_email","asm_emp_code",col("asm_name").alias("asm"),"asm_email","so_emp_code",col("so_name").alias("so"),"so_email").withColumn("town_villa",col("town_village")).drop("town_village")

# COMMAND ----------

base_sales_data = vbrk_1.join(vbrp_1,["billing_document"],"left").join(mara_1,["material_code"],"left").join(sales_force_1_1,["customer_code"],"left").join(t151t_1,["customer_group"],"left")

# COMMAND ----------

base_sales_data = base_sales_data.fillna(0,["mrp"]).withColumn('mrp_group',when(col("mrp").cast("int").isin(5,10),col("mrp")).when(col("mrp").between(11,20),lit("11-20")).when(col("mrp").between(21,30),lit("21-30")).when(col("mrp").between(31,40),lit("31-40")).when(col("mrp").between(41,50),lit("41-50")).when(col("mrp").between(51,60),lit("51-60")).when(col("mrp").between(61,70),lit("61-70")).when(col("mrp").between(71,80),lit("71-80")).when(col("mrp").between(81,90),lit("81-90")).when(col("mrp").between(91,100),lit("91-100")).when(col("mrp").between(101,110),lit("101-110")).when(col("mrp").between(111,120),lit("111-120")).when(col("mrp").between(121,130),lit("121-130")).when(col("mrp").between(131,140),lit("131-140")).when(col("mrp").between(141,150),lit("141-150")).when(col("mrp").between(151,160),lit("151-160")).when(col("mrp").between(161,170),lit("161-170")).when(col("mrp").between(171,180),lit("171-180")).when(col("mrp").between(181,190),lit("181-190")).when(col("mrp").between(191,200),lit("191-200")).when(col("mrp")>200,lit(">200")).otherwise(lit("NA")))

# COMMAND ----------

base_sales_data_2 = base_sales_data.filter(col("customer_group").isin("CP",	"DE",	"DL",	"EC",	"EX",	"IS",	"JM",	"MD",	"MS",	"MT",	"PL",	"RL",	"SN",	"SS")).withColumn("trade",when(col("customer_group").isin("OT","FD","S1","IS","CF","IU","1","D1","SN","MO","PN","ME","JM"),lit("Others(OT)")).when(col("customer_group").isin("DL",	"MD","MS","SS","RL","CP"),lit("General Trade(GT)")).when(col("customer_group").isin("MT","EC"),lit("Modern Trade(MT)")).when(col("customer_group").isin("DE","EX"),lit("Export(EX)")).when(col("customer_group").isin("PL"),lit("PL")).otherwise(lit("Others(OT)"))).withColumn("billing_amount", when((col("billing_type").isin("ZREM","RE","ZMRE","ZDR1"))&(col("billing_amount")>0),col("billing_amount")*-1).otherwise(col("billing_amount"))).withColumn("damage_value", when((col("billing_type").isin("ZREM","RE")),col("billing_amount")).otherwise(0)).withColumn("damage_value", sum(col("damage_value")).over(Window.partitionBy("customer_code", "material_code")))\
    .withColumn("damage_doc_count", when((col("billing_type").isin("ZREM","RE")),1).otherwise(0)).withColumn("damage_doc_count", sum(col("damage_doc_count")).over(Window.partitionBy("customer_code", "material_code"))).withColumn("average_billing_amount", avg(col("billing_amount")).over(Window.partitionBy("customer_code", "material_code")))

# COMMAND ----------

# MAGIC %md # Customer master added to incorporate additional columns for Part 1

# COMMAND ----------

KNA1_sales_info = base_sales_data_2.withColumn("months_billed",trunc(to_date(col("billing_date"),"yyyyMMdd"), "month")).withColumn("Set_bills",collect_set(col('months_billed')).over(Window.partitionBy("customer_code"))).withColumn("last_billed_date",max(col("billing_date")).over(Window.partitionBy("customer_code"))).withColumn('system_date',(to_date(current_date(),'yyyyMMdd'))).withColumn("last_billed_date",to_date(col("last_billed_date"),"yyyyMMdd")).withColumn('unbilled_days',datediff(col('system_date'),col("last_billed_date"))).select("customer_code","customer_group","customer_group_description","trade","last_billed_date","system_date","unbilled_days","Set_bills")
KNA1= KNA1.join(KNA1_sales_info,["customer_code"],"left").dropDuplicates()

# COMMAND ----------

#Temporary drop duplicate basis customer to resolve duplicates basis customer group 
KNA1 = KNA1.dropDuplicates(["customer_code"])

# COMMAND ----------

from datetime import date
from pyspark.sql.types import *

# COMMAND ----------

# display(customer_sp_df)

# COMMAND ----------

# # current fiscal year start and end
# current_year = year(current_date())
# fiscal_year_start = to_date(lit(f"{current_year}-04-01"), "yyyy-MM-dd")
# fiscal_year_end = to_date(lit(f"{current_year+1}-03-31"), "yyyy-MM-dd")
 
# # list of all months and last two digits of the year of the current fiscal year
# months = [lit(f"{m}{str(current_year)[2:]}") for m in ['Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar']]
 
# customer_sp_df = KNA1.withColumn("unbilled", lit(None))
 
# for i, month in enumerate(months, start=4):
#     customer_sp_df = customer_sp_df.withColumn(
#         "unbilled",
#         when(
#             (col("last_billed_date").between(fiscal_year_start, fiscal_year_end)) & 
#             (~col("months_billed").rlike(month)),concat(col("unbilled"), lit(','), month)
#         ).otherwise(F.col("unbilled"))
#     )

# COMMAND ----------

# MAGIC %md ### Logic Months Unbilled

# COMMAND ----------

KNA1 =  KNA1.withColumn("unbilled_ageing_bucket",when(col("unbilled_days").between(0,30),lit("0-30_days"))\
    .when(col("unbilled_days").between(31,45),lit("31-45_days")).when(col("unbilled_days").between(46,60),lit("46-60_days"))\
        .when(col("unbilled_days").between(61,75),lit("61-75_days")).when(col("unbilled_days").between(76,90),lit("76-90_days"))\
            .when(col("unbilled_days").between(91,120),lit("91-120_days")).when(col("unbilled_days")>120,lit("More than 120 days")).otherwise("Not Applicable"))

# COMMAND ----------

fiscal_year_start_month = 4  # April (change as per your fiscal year start)

df_cust = KNA1.withColumn("Current_Date", current_date())

df_cust = df_cust.withColumn("Fiscal_Year", (year(col("Current_Date")) 
                                    + ((month(col("Current_Date")) >= fiscal_year_start_month).cast("int")))-1).drop("Current_Date")

# Define the month fiscal year starts (April for this example)
start_month_of_fiscal_year = 4 

# Add date columns for beginning and end of fiscal year
df_cust = df_cust.withColumn("Start", expr(f"concat(Fiscal_Year, '-', {start_month_of_fiscal_year}, '-01')").cast("date"))
df_cust = df_cust.withColumn("End", expr(f"add_months(concat(Fiscal_Year, '-', {3}, '-01'), 12)"))
df_cust = df_cust.withColumn("End_Last",col("last_billed_date") )


# Create sequence of months within each fiscal year and explode it into separate rows
df_cust = df_cust.withColumn("unbilled_month", explode(sequence(to_date(col("Start")), to_date(col("End")), expr("interval 1 month")))).withColumn("Month_formatted",to_date(col("unbilled_month"),"yyyy-MM-dd"))#.filter(to_date(col("last_billed_date"),"yyyy-MM-dd")>=to_date(col("Start"),"yyyy-MM-dd")).withColumn("billed_month", explode(sequence(to_date(col("Start")), to_date(col("End_Last")), expr("interval 1 month"))))

# COMMAND ----------

# df_cust_1 = df_cust.select("customer_code",date_format(col("last_billed_date"),"MMM-yy"),date_format(col("unbilled_month"),"MMM-yy"),date_format(current_date(),"MMM-yy")).withColumn("month_pending",when(col("all_months")>col("current_month"),lit("")).otherwise(col("all_months")))

# COMMAND ----------

df_cust_1 = df_cust.withColumn("current_date",current_date()).select("customer_code","unbilled_month","current_date","Set_bills").withColumn("month_curr", trunc(to_date(col("current_date"),"yyyy-MM-dd"), "month")).withColumn("month_unbilled", trunc(to_date(col("unbilled_month"),"yyyy-MM-dd"), "month")).withColumn("month_pending",when(col("month_curr")<col("month_unbilled"),col("month_unbilled")).otherwise(to_date(lit("1900-01-01"),"yyyy-MM-dd"))).drop("unbilled_month","current_date").dropDuplicates()#.withColumn("month_last", trunc(to_date(col("billed_month"),"yyyy-MM-dd"), "month"))

# COMMAND ----------

df_cust_2 = df_cust_1.withColumn("Set_all",collect_set(col('month_unbilled')).over(Window.partitionBy("customer_code"))).withColumn("Set_exclude",collect_set(col('month_pending')).over(Window.partitionBy("customer_code"))).drop("month_pending","month_unbilled","month_curr").dropDuplicates()#.withColumn("Set_bills",collect_set(col('month_last')).over(Window.partitionBy("customer_code")))

# COMMAND ----------

df_cust_3 = df_cust_2.withColumn('array_main', array_except("Set_all", "Set_exclude")).withColumn('array_unbilled', array_except("array_main", "Set_bills")).drop("Set_all","Set_bills","Set_exclude").withColumn("size", size(col('array_unbilled'))).filter(col("size") >= 1)

# COMMAND ----------

# Assuming 'date_array' is the array of dates column in DataFrame df
df_cust_4 = df_cust_3.withColumn('sorted_date_array_string',array_sort(expr("transform(array_unbilled, x -> date_format(x, 'yyyy-MM-dd'))")))\
    .withColumn('sorted_date_array_string_2',array_sort(expr("transform(array_main, x -> date_format(x, 'yyyy-MM-dd'))")))

# Converting back to date
df_cust_4 = df_cust_4.withColumn('sorted_date_array', 
                    expr("transform(sorted_date_array_string, x -> date_format(to_date(x, 'yyyy-MM-dd'),'MMM-yy'))")).withColumn('unbilled_months', concat_ws(",", F.col('sorted_date_array'))).withColumn('sorted_date_array_2', 
                    expr("transform(sorted_date_array_string_2, x -> date_format(to_date(x, 'yyyy-MM-dd'),'MMM-yy'))")).withColumn('all_months', concat_ws(",", F.col('sorted_date_array_2')))

df_cust_4 = df_cust_4.select("customer_code","unbilled_months","all_months").withColumn("X",lit("x")).dropDuplicates()

# COMMAND ----------

unbill_cust = df_cust_4.select("customer_code","unbilled_months")
all_month_cust = df_cust_4.select("X","all_months").dropDuplicates()

# COMMAND ----------

KNA1_1 = KNA1.withColumn("X",lit("x")).join(unbill_cust,["customer_code"],"left").join(all_month_cust,["X"],"left")

# COMMAND ----------

KNA1_2 = KNA1_1.withColumn("unbilled_months",when(col("unbilled_months").isNotNull(),col("unbilled_months")).otherwise(col("all_months"))).drop("X","all_months","Set_bills")

# COMMAND ----------

#display(KNA1_2)

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

KNA1_2.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "customer_master_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

focus_brand_1 = focus_brand_read.fillna("",["region","product_brand"]).select(lower(concat("region","product_brand")).alias("key")).withColumn("type",lit("Growth Driver Brand")).dropDuplicates()

# COMMAND ----------

#Addition of Dummy columns to be replaced with actual data when available
base_sales_data_3 = base_sales_data_2.withColumn("brand_billed",lit("")).withColumn("distributors",lit("")).withColumn("distributors_category",lit("")).withColumn("customer_budget",lit("")).withColumn("focus_brand",lit("")).withColumn("state_name",lit("")).fillna("",["region","product_brand"]).withColumn("key",lower(concat("region","product_brand")))

# COMMAND ----------

base_sales_data_4 = base_sales_data_3.join(focus_brand_1,['key'],'left').fillna("Others",["type"])

# COMMAND ----------

# base_sales_data_4 = base_sales_data_3.select("product_brand","billing_amount").withColumn("product_brand_total_billing_amount",sum(col("billing_amount")).over(Window.partitionBy(col("product_brand")))).drop("billing_amount").dropDuplicates().withColumn('Cumulative_Amount',sum('product_brand_total_billing_amount').over(Window.partitionBy('product_brand').orderBy(col("product_brand_total_billing_amount").desc()).rowsBetween(-sys.maxsize, 0)))

# COMMAND ----------

# from pyspark.sql.functions import col, sum as _sum

# # Calculate total sales
# total_sales = base_sales_data_4.agg(_sum(col("billing_amount"))).collect()[0][0]

# # Calculate cumulative sales per product
# windowval = Window.partitionBy("product_brand").orderBy(col("billing_amount").desc()).rowsBetween(Window.unboundedPreceding, 0)
# sales_df = base_sales_data_4.withColumn('cumulative_sales', _sum('billing_amount').over(windowval))

# # Calculate cumulative sales percentage
# sales_df = sales_df.withColumn('cumulative_percentage', (col('cumulative_sales') / total_sales) * 100)

# # Categorize products into 80% and 20% based on cumulative sales percentage, assuming 20% of products amount to 80% of sales
# sales_df = sales_df.withColumn('80/20 rule', 
#                                when(col('cumulative_percentage') <= 80, '20%_products').otherwise('80%_products')).drop("cumulative_sales","cumulative_percentage")

# COMMAND ----------


from pyspark.sql.functions import sum as spark_sum, col, when

# Assuming df is your DataFrame
base_1 = base_sales_data_4.withColumn("billing_fiscal_year", when(month(to_date(col("billing_date"),"yyyyMMdd")) >= 4, year(to_date(col("billing_date"),"yyyyMMdd"))).otherwise(year(to_date(col("billing_date"),"yyyyMMdd")) - 1))


# Get the sum of 'billing_amount' for each 'product_brand' in each 'fiscal_year'
df = base_1.groupBy('billing_fiscal_year', 'product_brand').agg(spark_sum('billing_amount').alias('total_amount'))

# Calculate the cumulative percentage of 'total_amount'
windowSpec = Window.partitionBy('billing_fiscal_year').orderBy(df['total_amount'].desc())
df = df.withColumn('cumulative_sum', spark_sum('total_amount').over(windowSpec))
df = df.withColumn('cumulative_perc', (col('cumulative_sum') / spark_sum('total_amount').over(Window.partitionBy('billing_fiscal_year').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))) * 100).withColumn('80/20 rule', when(col('cumulative_perc') <= 80, '80').otherwise('20'))

# Pareto segmentation into 80% and 20%
sales_df = base_1.join(df,["billing_fiscal_year","product_brand"],"left").select(base_1["*"],df['80/20 rule'])


# COMMAND ----------

# from pyspark.sql.functions import col, sum as spark_sum, when, year

# # Assuming df is your DataFrame
# df = base_sales_data_4.withColumn("billing_fiscal_year", when(month(to_date(col("billing_date"),"yyyyMMdd")) >= 4, year(to_date(col("billing_date"),"yyyyMMdd"))).otherwise(year(to_date(col("billing_date"),"yyyyMMdd")) - 1)).withColumn('product_brand_x',regexp_replace('product_brand', r'[^a-zA-Z0-9]', ''))


# # Define a window 
# windowSpec = Window.partitionBy('billing_fiscal_year',"product_brand_x").orderBy(df['billing_amount'].desc())

# # Add cumulative sum and percentage
# df = df.withColumn('cumulative_sum', spark_sum('billing_amount').over(windowSpec))
# df = df.withColumn('total_sum', spark_sum('billing_amount').over(Window.partitionBy('billing_fiscal_year').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)))
# df = df.withColumn('cumulative_perc', (col('cumulative_sum')/col('total_sum'))*100)

# # Pareto Segmentation
# sales_df = df.withColumn('80/20 rule', when(col('cumulative_perc') <= 80, '80%_products').otherwise('20%_products'))


# COMMAND ----------

#display(sales_df.select("billing_fiscal_year","product_brand","cumulative_sum","80/20 rule","cumulative_perc").distinct())

# COMMAND ----------

col_Primary_sales = ["tax_amount",	"quantity", 'billed_quantity', 'qty_in_kg',"net_value"]
for i in col_Primary_sales:
    sales_df = sales_df.withColumn('col name2', regexp_replace(i, '[,]', '')).drop(i).withColumnRenamed('col name2',i)
    sales_df = sales_df.withColumn(i, regexp_replace(col(i),'[^\sa-zA-Z0-9\.]', '')).fillna(0,[i]).withColumn(i, col(i).cast(DecimalType(38,2)))

# COMMAND ----------

# MAGIC %md ## Calculating first and last day

# COMMAND ----------


sales_df = sales_df.withColumn("first_date_month", trunc(to_date(col("billing_date"),"yyyyMMdd"), "month")).withColumn("last_day_of_month", last_day(to_date(col("billing_date"),"yyyyMMdd"))).dropDuplicates()

#.filter((col("billing_month")== to_date(lit('2024-01-01'), 'y-d-M'))|(col("billing_month")== to_date(lit('2023-12-01')))|(col("billing_month")== to_date(lit('2023-01-01')))|(col("billing_month")== to_date(lit('2022-12-01'))))

# COMMAND ----------

sales_df_1 = sales_df.withColumn("X",lit("x")).withColumn("total_billing_amount", sum(col("billing_amount")).over(Window.partitionBy("X"))).withColumn("brand_billing_amount", sum(col("billing_amount")).over(Window.partitionBy("product_brand"))).withColumn("contribution", (col("brand_billing_amount")/col("total_billing_amount"))*100).drop("total_billing_amount")

# COMMAND ----------

# MAGIC %md # MTD

# COMMAND ----------

sales_df_22 = sales_df_1.withColumn("exception_key", concat_ws("_",col("customer_code"), col("material_code")))\
    .withColumn("billing_date", to_date(col("billing_date"),"yyyyMMdd")).withColumn("year", year(col("billing_date"))).withColumn("month", month(col("billing_date")))

# COMMAND ----------

sales_df_23 = sales_df_22.withColumn('current_date',(to_date(current_date(),'yyyyMMdd'))).withColumn("new_end_date", least(col("current_date"), col("last_day_of_month"))).withColumn("new_billing_value", when(col("billing_date").between(col("first_date_month"), col("new_end_date")), col("billing_amount") ).otherwise(0)).withColumn("new_qty_in_kg_value", when(col("billing_date").between(col("first_date_month"), col("new_end_date")), col("qty_in_kg") ).otherwise(0)).withColumn("new_billing_qty_value", when(col("billing_date").between(col("first_date_month"), col("new_end_date")), col("billed_quantity") ).otherwise(0))

# COMMAND ----------

#temp_filter delete after use
# sales_df_23 = sales_df_23.filter(col("customer_code") == "104823")

# COMMAND ----------

sales_df_23=sales_df_23.dropDuplicates()#.persist()

# COMMAND ----------

w_MTD= Window.partitionBy('exception_key', 'new_end_date', 'first_date_month')
sales_df_23_1 = sales_df_23.withColumn("MTD", sum(col("new_billing_value")).over(w_MTD)).withColumn("MTD_MT", sum(col("new_qty_in_kg_value")).over(w_MTD)).withColumn("MTD_CB", sum(col("new_billing_qty_value")).over(w_MTD))


# COMMAND ----------

sales_df_24 = sales_df_23_1.withColumn("MTD", round(col("MTD"),2)).withColumn("MTD_MT", round(col("MTD_MT"),2)).withColumn("MTD_CB", round(col("MTD_CB"),2)).withColumn("LY_end_date",when(date_format(col("billing_Date"),"MMM-yy")==date_format(col("current_date"),"MMM-yy"),col("current_date")).otherwise(last_day(add_months(F.col('new_end_date'),-12))))\
    .withColumn("LY_first_date_month", add_months(col('first_date_month'),-12)).withColumn("lymtd_month_year",date_format(col("LY_first_date_month"),"MMM-yy"))

# COMMAND ----------

base_data_bill_data = sales_df_24.select('exception_key', 'billing_date', 'billing_amount','qty_in_kg','billed_quantity','billing_document','billing_item','profit_center').dropDuplicates().persist()

# COMMAND ----------

# MAGIC %md # LYMTD

# COMMAND ----------

# LYMTD logic
lymtd_base = sales_df_24.select('exception_key', 'LY_end_date', 'LY_first_date_month','lymtd_month_year')
base_data_lymtd = base_data_bill_data
lymtd_base_final = lymtd_base.join(base_data_lymtd,[lymtd_base.exception_key==base_data_lymtd.exception_key,(base_data_lymtd.billing_date>=lymtd_base.LY_first_date_month)&(base_data_lymtd.billing_date<=lymtd_base.LY_end_date)],"inner").select(base_data_lymtd["*"],lymtd_base.lymtd_month_year)
lymtd_base_final = lymtd_base_final.withColumn("LYMTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key',"lymtd_month_year"))).withColumn("LYMTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key',"lymtd_month_year"))).withColumn("LYMTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key',"lymtd_month_year"))).select("exception_key","lymtd_month_year","LYMTD","LYMTD_MT","LYMTD_CB").dropDuplicates()

# COMMAND ----------

#w_lymtd= Window.partitionBy('exception_key', 'LY_end_date', 'LY_first_date_month') Running 

sales_df_25 = sales_df_24.join(lymtd_base_final,["exception_key","lymtd_month_year"],"left")
sales_df_25 = sales_df_25.fillna(0,["LYMTD","LYMTD_CB","LYMTD_MT"]).withColumn("LYMTD", round(col("LYMTD"),2))\
    .withColumn("L2MTD_end_date",when(date_format(col("billing_Date"),"MMM-yy")==date_format(col("current_date"),"MMM-yy"),col("current_date")).otherwise(last_day(add_months(F.col('new_end_date'),-2))))\
    .withColumn("L2MTD_first_date_month", add_months(col('first_date_month'),-2)).withColumn("l2mtd_month_year",date_format(col("L2MTD_first_date_month"),"MMM-yy"))\
        .withColumn("L1MTD_end_date",when(date_format(col("billing_Date"),"MMM-yy")==date_format(col("current_date"),"MMM-yy"),col("current_date")).otherwise(last_day(add_months(F.col('new_end_date'),-1))))\
    .withColumn("L1MTD_first_date_month", add_months(col('first_date_month'),-1)).withColumn("l1mtd_month_year",date_format(col("L1MTD_first_date_month"),"MMM-yy"))

    
    #.withColumn("L2MTD_end_date", add_months(col('new_end_date'),-2)).withColumn("L1MTD_end_date", add_months(col('new_end_date'),-1))

# COMMAND ----------

# MAGIC %md # L2MTD

# COMMAND ----------

# L2MTD logic
l2mtd_base = sales_df_25.select('exception_key', 'L2MTD_end_date', 'L2MTD_first_date_month','l2mtd_month_year')
base_data_l2mtd = base_data_bill_data
l2mtd_base_final = l2mtd_base.join(base_data_l2mtd,[l2mtd_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=l2mtd_base.L2MTD_first_date_month)&(base_data_l2mtd.billing_date<=l2mtd_base.L2MTD_end_date)],"inner").select(base_data_l2mtd["*"],l2mtd_base.l2mtd_month_year).dropDuplicates()
l2mtd_base_final = l2mtd_base_final.withColumn("L2MTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key',"l2mtd_month_year"))).withColumn("L2MTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key',"l2mtd_month_year"))).withColumn("L2MTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key',"l2mtd_month_year")))
l2mtd_base_final = l2mtd_base_final.select('exception_key', "l2mtd_month_year",'L2MTD',"L2MTD_MT","L2MTD_CB")

# COMMAND ----------

# L2MTD logic (Single month MTD)
l1mtd_base = sales_df_25.select('exception_key', 'L1MTD_end_date', 'L1MTD_first_date_month','l1mtd_month_year')
base_data_l1mtd = base_data_bill_data
l1mtd_base_final = l1mtd_base.join(base_data_l1mtd,[l1mtd_base.exception_key==base_data_l1mtd.exception_key,(base_data_l1mtd.billing_date>=l1mtd_base.L1MTD_first_date_month)&(base_data_l1mtd.billing_date<=l1mtd_base.L1MTD_end_date)],"inner").select(base_data_l1mtd["*"],l1mtd_base.l1mtd_month_year).dropDuplicates()
l1mtd_base_final = l1mtd_base_final.withColumn("L1MTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key',"l1mtd_month_year"))).withColumn("L1MTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key',"l1mtd_month_year"))).withColumn("L1MTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key',"l1mtd_month_year")))
l1mtd_base_final = l1mtd_base_final.select('exception_key', "l1mtd_month_year",'L1MTD','L1MTD_MT','L1MTD_CB')

# COMMAND ----------

sales_df_26 = sales_df_25.join(l2mtd_base_final,["exception_key", "l2mtd_month_year"],"left")
sales_df_26 = sales_df_26.join(l1mtd_base_final,["exception_key", "l1mtd_month_year"],"left")
sales_df_26 = sales_df_26.fillna(0,["L2MTD","L1MTD","L2MTD_MT","L1MTD_MT","L2MTD_CB","L1MTD_CB"]).withColumn("L2MMTD", round((col("L2MTD")+col("L1MTD"))/2,2)).withColumn("L2MMTD_MT", round((col("L2MTD_MT")+col("L1MTD_MT"))/2,2)).withColumn("L2MMTD_CB", round((col("L2MTD_CB")+col("L1MTD_CB"))/2,2)).withColumn("LM_end_date", add_months(F.col('last_day_of_month'),-1)).withColumn("LM_end_date", last_day(col("LM_end_date")))\
    .withColumn("LM_first_date_month", add_months(F.col('first_date_month'),-1)).withColumn("lm_month_year",date_format(col("LM_first_date_month"),"MMM-yy")).drop("L2MTD","L1MTD","L2MTD_MT","L1MTD_MT","L2MTD_CB","L1MTD_CB")

# COMMAND ----------

# MAGIC %md # LM

# COMMAND ----------

# L2MTD logic
lm_base = sales_df_26.select('exception_key', 'LM_end_date', 'LM_first_date_month','lm_month_year')
base_data_l2mtd = base_data_bill_data
lm_base_final = lm_base.join(base_data_l2mtd,[lm_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=lm_base.LM_first_date_month)&(base_data_l2mtd.billing_date<=lm_base.LM_end_date)],"inner").select(base_data_l2mtd["*"],lm_base.lm_month_year).dropDuplicates()
lm_base_final = lm_base_final.withColumn("LM",sum(col("billing_amount")).over(Window.partitionBy('exception_key', 'lm_month_year'))).withColumn("LM_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key', 'lm_month_year'))).withColumn("LM_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key', 'lm_month_year')))
lm_base_final = lm_base_final.select('exception_key', 'lm_month_year','LM','LM_CB','LM_MT').dropDuplicates()

# COMMAND ----------

sales_df_27 = sales_df_26.join(lm_base_final,["exception_key", 'lm_month_year'],"left")
sales_df_27 = sales_df_27.fillna(0,["LM","LM_MT","LM_CB"]).withColumn("LM", round(col("LM"),2)).withColumn("LM_CB", round(col("LM_CB"),2)).withColumn("LM_MT", round(col("LM_MT"),2)).withColumn("LMTD_end_date", add_months(F.col('new_end_date'),-1))\
    .withColumn("LMTD_first_date_month", add_months(F.col('first_date_month'),-1)).withColumn("lmtd_month_year",date_format(col("LMTD_first_date_month"),"MMM-yy"))

# COMMAND ----------

# MAGIC %md #LMMTD

# COMMAND ----------

# LMTD logic
lm_base = sales_df_27.select('exception_key', 'LMTD_end_date', 'LMTD_first_date_month',"lmtd_month_year")
base_data_l2mtd = base_data_bill_data
lm_base_final = lm_base.join(base_data_l2mtd,[lm_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=lm_base.LMTD_first_date_month)&(base_data_l2mtd.billing_date<=lm_base.LMTD_end_date)],"inner").select(base_data_l2mtd["*"],lm_base.lmtd_month_year).dropDuplicates()
lm_base_final = lm_base_final.withColumn("LMMTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key', 'lmtd_month_year')))\
    .withColumn("LMMTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key', 'lmtd_month_year')))\
        .withColumn("LMMTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key', 'lmtd_month_year')))
lmtd_base_final = lm_base_final.select('exception_key', 'lmtd_month_year','LMMTD','LMMTD_MT','LMMTD_CB').dropDuplicates()

# COMMAND ----------

sales_df_28 = sales_df_27.join(lmtd_base_final,["exception_key", "lmtd_month_year"],"left")
sales_df_28 = sales_df_28.fillna(0,["LMMTD","LMMTD_MT","LMMTD_CB"]).withColumn("LMMTD", round(col("LMMTD"),2)).withColumn("LMMTD_MT", round(col("LMMTD_MT"),2)).withColumn("LMMTD_CB", round(col("LMMTD_CB"),2)).withColumn("L2M_AVG_end_date", add_months(F.col('last_day_of_month'),-1)).withColumn("L2M_AVG_end_date", last_day(col("L2M_AVG_end_date")))\
    .withColumn("L2M_AVG_first_date_month", add_months(F.col('first_date_month'),-2)).withColumn("l2m_avg_month_year",date_format(col("L2M_AVG_first_date_month"),"MMM-yy"))

# COMMAND ----------

# MAGIC %md #L2M

# COMMAND ----------

# LMTD logic
lm_base = sales_df_28.select('exception_key', 'L2M_AVG_end_date', 'L2M_AVG_first_date_month','l2m_avg_month_year')
base_data_l2mtd = base_data_bill_data
lm_base_final = lm_base.join(base_data_l2mtd,[lm_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=lm_base.L2M_AVG_first_date_month)&(base_data_l2mtd.billing_date<=lm_base.L2M_AVG_end_date)],"inner").select(base_data_l2mtd["*"],lm_base.l2m_avg_month_year).dropDuplicates()
lm_base_final = lm_base_final.withColumn("L2M",(sum(col("billing_amount")).over(Window.partitionBy('exception_key', "l2m_avg_month_year")))/2)\
    .withColumn("L2M_MT",(sum(col("qty_in_kg")).over(Window.partitionBy('exception_key', "l2m_avg_month_year")))/2)\
        .withColumn("L2M_CB",(sum(col("billed_quantity")).over(Window.partitionBy('exception_key', "l2m_avg_month_year")))/2)
l2m_base_final = lm_base_final.select('exception_key', "l2m_avg_month_year",'L2M',"L2M_CB","L2M_MT").dropDuplicates()

# COMMAND ----------

sales_df_29 = sales_df_28.join(l2m_base_final,["exception_key", 'l2m_avg_month_year'],"left")
sales_df_29 = sales_df_29.fillna(0,["L2M","L2M_MT","L2M_CB"]).withColumn("L2M", round(col("L2M"),2))\
    .withColumn("L2M_MT", round(col("L2M_MT"),2))\
        .withColumn("L2M_CB", round(col("L2M_CB"),2)).dropDuplicates()

# COMMAND ----------

# Current Fiscal Year start and end date
sales_df_30 = sales_df_29.withColumn("fiscal_year", when(month(col("billing_date")) >= 4, year(col("billing_date"))).otherwise(year(col("billing_date")) - 1))
sales_df_30 = sales_df_30.withColumn("current_fy_start",to_date(concat(col("fiscal_year"), lit("-04-01")),"yyyy-MM-dd"))
sales_df_30 = sales_df_30.withColumn("current_fy_end",when(date_format(col("billing_Date"),"MMM-yy")==date_format(col("current_date"),"MMM-yy"),col("current_date")).otherwise(last_day(col("billing_date")))).withColumn("cy_month_year",date_format(col("current_fy_start"),"MMM-yy")).withColumn("last_fy_start", add_months(col('current_fy_start'),-12)).withColumn("last_fy_end", add_months(col('current_fy_end'),-12)).withColumn("ly_month_year",date_format(col("last_fy_start"),"MMM-yy"))

# # Year to Date (YTD)
# df = df.withColumn("YTD_value",when(col("billing_date") <= col("current_fy_end"), col("billing_amount"))
#                    .otherwise(0))

# # Last Year to Date (LYTD)
# df = df.withColumn("last_year_start", 
#                    (concat(col("fiscal_year") - 1, lit("-04-01"))))
# df = df.withColumn("last_year_end", 
#                    (concat(col("fiscal_year"), lit("-03-31"))))
# df = df.withColumn("LYTD", 
#                    when((col("billing_date") >= col("last_year_start")) & 
#                         (col("billing_date") <= col("last_year_end")), col('billing_amount'))
#                    .otherwise(0))


# COMMAND ----------

# MAGIC %md #YTD

# COMMAND ----------

# YTD logic
ytd_base = sales_df_30.select('exception_key', 'current_fy_end', 'current_fy_start',"cy_month_year")
base_data_l2mtd = base_data_bill_data
lm_base_final = ytd_base.join(base_data_l2mtd,[ytd_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=ytd_base.current_fy_start)&(base_data_l2mtd.billing_date<=ytd_base.current_fy_end)],"inner").select(base_data_l2mtd["*"],ytd_base.cy_month_year).dropDuplicates()
cy_base_final = lm_base_final.withColumn("YTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key', 'cy_month_year')))\
    .withColumn("YTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key', 'cy_month_year')))\
        .withColumn("YTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key', 'cy_month_year')))
cy_base_final = cy_base_final.select('exception_key', 'cy_month_year','YTD',"YTD_MT","YTD_CB").dropDuplicates()

# COMMAND ----------

sales_df_31 = sales_df_30.join(cy_base_final,["exception_key", 'cy_month_year'],"left")
sales_df_31 = sales_df_31.fillna(0,["YTD","YTD_MT","YTD_CB"]).withColumn("YTD", round(col("YTD"),2))\
    .withColumn("YTD_MT", round(col("YTD_MT"),2))\
        .withColumn("YTD_CB", round(col("YTD_CB"),2)).dropDuplicates()

# COMMAND ----------

# LYTD logic
lytd_base = sales_df_31.select('exception_key', 'last_fy_end', 'last_fy_start',"ly_month_year")
base_data_l2mtd = base_data_bill_data
lm_base_final = lytd_base.join(base_data_l2mtd,[lytd_base.exception_key==base_data_l2mtd.exception_key,(base_data_l2mtd.billing_date>=lytd_base.last_fy_start)&(base_data_l2mtd.billing_date<=lytd_base.last_fy_end)],"inner").select(base_data_l2mtd["*"],lytd_base.ly_month_year).dropDuplicates()
ly_base_final = lm_base_final.withColumn("LYTD",sum(col("billing_amount")).over(Window.partitionBy('exception_key', 'ly_month_year')))\
    .withColumn("LYTD_MT",sum(col("qty_in_kg")).over(Window.partitionBy('exception_key', 'ly_month_year')))\
        .withColumn("LYTD_CB",sum(col("billed_quantity")).over(Window.partitionBy('exception_key', 'ly_month_year')))
ly_base_final = ly_base_final.select('exception_key', 'ly_month_year','LYTD','LYTD_MT','LYTD_CB').dropDuplicates()

# COMMAND ----------

base_data_bill_data.unpersist()

# COMMAND ----------

sales_df_32 = sales_df_31.join(ly_base_final,["exception_key", 'ly_month_year'],"left")
sales_df_32 = sales_df_32.fillna(0,["LYTD","LYTD_CB","LYTD_MT"]).withColumn("LYTD", round(col("LYTD"),2))\
    .withColumn("LYTD_MT", round(col("LYTD_MT"),2))\
        .withColumn("LYTD_CB", round(col("LYTD_CB"),2)).dropDuplicates()

# COMMAND ----------

sales_df_33 = sales_df_32.withColumn("growth_percentage_MTD_vs_LYMTD",(col("MTD")-col("LYMTD"))/col("LYMTD"))\
    .withColumn("growth_percentage_MTD_Vs_LMMTD",(col("MTD")-col("LMMTD"))/col("LMMTD"))\
        .withColumn("growth__percentage_MTD_Vs_L2MMTD",(col("MTD")-col("L2MMTD"))/col("L2MMTD"))\
            .withColumn("growth_percentage_MTD_vs_LYMTD_MT",(col("MTD_MT")-col("LYMTD_MT"))/col("LYMTD_MT"))\
                .withColumn("growth_percentage_MTD_Vs_LMMTD_MT",(col("MTD_MT")-col("LMMTD_MT"))/col("LMMTD_MT"))\
                    .withColumn("growth__percentage_MTD_Vs_L2MMTD_MT",(col("MTD_MT")-col("L2MMTD_MT"))/col("L2MMTD_MT"))\
                        .withColumn("growth_percentage_MTD_vs_LYMTD_CB",(col("MTD_CB")-col("LYMTD_CB"))/col("LYMTD_CB"))\
                .withColumn("growth_percentage_MTD_Vs_LMMTD_CB",(col("MTD_CB")-col("LMMTD_CB"))/col("LMMTD_CB"))\
                    .withColumn("growth__percentage_MTD_Vs_L2MMTD_CB",(col("MTD_CB")-col("L2MMTD_CB"))/col("L2MMTD_CB"))\
                    .withColumn("growth_percentage_YTD_vs_LYTD",(col("YTD")-col("LYTD"))/col("LYTD"))\
                .withColumn("growth_percentage_YTD_Vs_LYTD_CB",(col("YTD_CB")-col("LYTD_CB"))/col("LYTD_CB"))\
                    .withColumn("growth__percentage_YTD_Vs_LYTD_MT",(col("MTD_MT")-col("L2MMTD_MT"))/col("L2MMTD_MT"))

# COMMAND ----------

sales_df_34=sales_df_33.fillna("None",["business_segment"]).dropDuplicates()

# COMMAND ----------

sales_df_35 = sales_df_34.withColumn("monthly_days", (date_diff(col("last_day_of_month"), col("first_date_month")))+1).withColumn("day_lapsed",when(date_format(col("billing_date"),"MMM-yy")==date_format(current_date(),"MMM-yy"),date_format(current_date(),"d")).otherwise(col("monthly_days"))).withColumn("MTD_per_day",col("MTD")/col("day_lapsed")).withColumn("MTD_per_day_MT",col("MTD_MT")/col("day_lapsed")).withColumn("MTD_per_day_CB",col("MTD_CB")/col("day_lapsed")).drop("monthly_days").withColumnRenamed("mrp/cb","mrp_cb").withColumnRenamed("no_of_bisc/pk","no_of_bisc_pk").withColumnRenamed("80/20 rule","80_20 rule").withColumnRenamed("first_date_month","billing_month")

# COMMAND ----------

columns_final = ['customer_group', 'customer_code', 'material_code','exception_key', 'item_category', 'material_description', 'UoM', 'product_category', 'product_brand', 'brand_flavour', 'mrp', 'pack_size', 'pack_format', 'mrp_cb', 'no_of_bisc_pk', 'secondary_pack_qty', 'secondary_pkt_type', 'material_group', 'business_segment' , 'customer_name', 'region', 'state_code', 'district', 'zone', 'zsm_emp_code', 'zsm', 'zsm_email', 'rsm_emp_code', 'rsm',
 'rsm_email', 'asm_emp_code', 'asm', 'asm_email', 'so_emp_code', 'so', 'so_email', 'town_villa', 'customer_group_description', 'mrp_group', 'trade', 'brand_billed', 'distributors', 'distributors_category', 'customer_budget', 'focus_brand', 'state_name', 'type', '80_20 rule', 'billing_month', 'brand_billing_amount', 'contribution', 'MTD', 'LYMTD', 'growth_percentage_MTD_vs_LYMTD', 'LM',
 'LMMTD', 'growth_percentage_MTD_Vs_LMMTD', 'L2M', 'L2MMTD', 'growth__percentage_MTD_Vs_L2MMTD', 'MTD_per_day', 'MTD_CB', 'LYMTD_CB', 'growth_percentage_MTD_vs_LYMTD_CB', 'LM_CB', 'LMMTD_CB', 'growth_percentage_MTD_Vs_LMMTD_CB', 'L2M_CB', 'L2MMTD_CB', 'growth__percentage_MTD_Vs_L2MMTD_CB', 'MTD_per_day_CB', 'MTD_MT', 'LYMTD_MT', 'growth_percentage_MTD_vs_LYMTD_MT', 'LM_MT', 'LMMTD_MT', 'growth_percentage_MTD_Vs_LMMTD_MT', 'L2M_MT',
 'L2MMTD_MT', 'growth__percentage_MTD_Vs_L2MMTD_MT', 'MTD_per_day_MT','current_date','LY_end_date', 'LY_first_date_month', 'L1MTD_first_date_month','L1MTD_end_date','L2MTD_first_date_month', 'L2MTD_end_date','LM_first_date_month' , 'LM_end_date','LMTD_first_date_month' ,'LMTD_end_date','L2M_AVG_first_date_month' ,'L2M_AVG_end_date', 'current_fy_start', 'current_fy_end', 'last_fy_start', 'last_fy_end', 'YTD', 'YTD_MT', 'YTD_CB', 'LYTD', 'LYTD_MT', 'LYTD_CB', 'day_lapsed','growth_percentage_YTD_vs_LYTD','growth_percentage_YTD_Vs_LYTD_CB','growth__percentage_YTD_Vs_LYTD_MT', 'damage_value', 'damage_doc_count', 'average_billing_amount']

# z = [item for item in x if item not in y]
# print(z)
sales_df_36 = sales_df_35.select(columns_final).dropDuplicates().persist()

# COMMAND ----------

filtered_df = sales_df_36.filter(
    to_date(col('billing_month'), 'yyyy-MM-dd').between(
        to_date(lit('2022-04-01'), 'yyyy-MM-dd'), 
        to_date(current_date(), 'yyyy-MM-dd')
    )
)


# COMMAND ----------

# filtered_df.count()

# COMMAND ----------

sales_df_22.columns

# COMMAND ----------

# sales_df_37 = sales_df_22.filter(col(""))

# COMMAND ----------

# MAGIC %md #Ingestion Sales Register

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"


filtered_df.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "primary_sales_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

customer_sales_register = sales_df_33.withColumn("billing_month_year",date_format(col("billing_date"),"MMM-yy")).withColumn("D_O_M",date_format(col("billing_date"), "d")).withColumn("1_7_billing_value", when(col("D_O_M").between(1,7), col("billing_amount")).otherwise(0)).withColumn("achievement_of_sales_till_1_7th_of_month", sum(col("1_7_billing_value")).over(Window.partitionBy("customer_code", "billing_month_year", "business_segment")))\
    .withColumn("1_14_billing_value", when(col("D_O_M").between(1,14), col("billing_amount")).otherwise(0)).withColumn("achievement_of_sales_till_1_14th_of_month", sum(col("1_14_billing_value")).over(Window.partitionBy("customer_code", "billing_month_year", "business_segment")))\
    .withColumn("1_21_billing_value", when(col("D_O_M").between(1,21), col("billing_amount")).otherwise(0)).withColumn("achievement_of_sales_till_1_21th_of_month", sum(col("1_21_billing_value")).over(Window.partitionBy("customer_code", "billing_month_year", "business_segment")))\
    .withColumn("1_EOM_billing_value", when(col("D_O_M").between(1,31), col("billing_amount")).otherwise(0)).withColumn("achievement_of_sales_till_1_EOM_of_month", sum(col("1_EOM_billing_value")).over(Window.partitionBy("customer_code", "billing_month_year", "business_segment"))).withColumn("montly_days", (date_diff(col("last_day_of_month"), col("first_date_month")))+1)

# COMMAND ----------

sales_df_36.unpersist()

# COMMAND ----------

customer_sales_register = customer_sales_register.select("customer_code", "billing_month_year", "achievement_of_sales_till_1_7th_of_month", "achievement_of_sales_till_1_14th_of_month", "achievement_of_sales_till_1_21th_of_month", "achievement_of_sales_till_1_EOM_of_month", "montly_days", "business_segment").dropDuplicates()

# COMMAND ----------

customer_sales_register = customer_sales_register.withColumn("achievement_of_sales_till_1_7th_of_month", round(col("achievement_of_sales_till_1_7th_of_month"), 2))\
    .withColumn("achievement_of_sales_till_1_14th_of_month", round(col("achievement_of_sales_till_1_14th_of_month"), 2))\
        .withColumn("achievement_of_sales_till_1_21th_of_month", round(col("achievement_of_sales_till_1_21th_of_month"), 2))\
            .withColumn("achievement_of_sales_till_1_EOM_of_month", round(col("achievement_of_sales_till_1_EOM_of_month"), 2))

# COMMAND ----------

customer_budget_1 = customer_budget_1.withColumn("billing_month_year", date_format(col("Month_formatted"),"MMM-yy")).fillna("None",["segment"]).dropDuplicates()

# COMMAND ----------

# display(customer_budget_1.select("customer_code", "billing_month_year", "segment").distinct())

# COMMAND ----------

# MAGIC %md ### Joinning sale register and customer budget

# COMMAND ----------

customer_budget_joined_sp_df = customer_budget_1.join(customer_sales_register, (customer_sales_register.customer_code == customer_budget_1.customer_code) & (customer_sales_register.business_segment == customer_budget_1.segment) & (customer_sales_register.billing_month_year == customer_budget_1.billing_month_year), "left").select(customer_budget_1["*"], customer_sales_register["achievement_of_sales_till_1_7th_of_month"], customer_sales_register["achievement_of_sales_till_1_14th_of_month"], customer_sales_register["achievement_of_sales_till_1_21th_of_month"], customer_sales_register["achievement_of_sales_till_1_EOM_of_month"],  customer_sales_register["montly_days"])

# COMMAND ----------

customer_budget_joined_sp_df = customer_budget_joined_sp_df.withColumn("budget_per_day", col("Monthly Budget")/col("montly_days") )\
    .withColumn("current_day",when(date_format(col("billing_month_year"),"MMM-yy")==date_format(current_date(),"MMM-yy"),date_format(current_date(),"d")).otherwise(1))\
        .withColumn("remaining_days", when(col("current_day") != 1, col("montly_days") - col("current_day")).otherwise(1))

# COMMAND ----------

customer_budget_joined_sp_df = customer_budget_joined_sp_df.withColumn("achievement_percentage_of_sales_till_1_7th_of_month", (col("achievement_of_sales_till_1_7th_of_month")/100000)/(col("Monthly Budget")) )\
    .withColumn("achievement_percentage_of_sales_till_1_14th_of_month", ((col("achievement_of_sales_till_1_14th_of_month")/100000)/(col("Monthly Budget"))) )\
        .withColumn("achievement_percentage_of_sales_till_1_21th_of_month", ((col("achievement_of_sales_till_1_21th_of_month")/100000)/(col("Monthly Budget"))) )\
            .withColumn("achievement_percentage_of_sales_till_1_EOM_of_month", ((col("achievement_of_sales_till_1_EOM_of_month")/100000)/(col("Monthly Budget")))  )\
                .withColumn("btd_per_day", (col("Monthly Budget") - (col("achievement_of_sales_till_1_EOM_of_month") / 100000))/ col("remaining_days") )\
                    .withColumn("btd", (col("Monthly Budget") - (col("achievement_of_sales_till_1_EOM_of_month") / 100000)) )

# COMMAND ----------

customer_budget_joined_sp_df = customer_budget_joined_sp_df.persist()

# COMMAND ----------

#customer_budget_joined_sp_df.count()

# COMMAND ----------

# MAGIC %md ### Ingestion of Customer Budget

# COMMAND ----------

driver = "org.mariadb.jdbc.Driver"
database_host = "anmolmysqldb-flex.mysql.database.azure.com"
database_port = "3306"
database_name = "anmol_dev"

url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"

customer_budget_joined_sp_df.write \
  .format("jdbc") \
  .option("driver", driver) \
  .option("url", url) \
  .option("dbtable", "customer_budget_prod") \
  .option("user", "dbadmin") \
  .option("password", "Welcome2tata#") \
  .option("sslmode", "require") \
  .mode("overwrite") \
  .save()

# COMMAND ----------

customer_budget_joined_sp_df.unpersist()

# COMMAND ----------

#sales_df_33 = sales_df_33.dropDuplicates().cache()

# COMMAND ----------

# MAGIC %md ### Ingestion of Sales Register

# COMMAND ----------

# driver = "org.mariadb.jdbc.Driver"
# database_host = "anmolmysqldb-flex.mysql.database.azure.com"
# database_port = "3306"
# database_name = "anmol_dev"

# url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"


# sales_df_32.write \
#   .format("jdbc") \
#   .option("driver", driver) \
#   .option("url", url) \
#   .option("dbtable", "primary_sales_prod") \
#   .option("user", "dbadmin") \
#   .option("password", "Welcome2tata#") \
#   .option("sslmode", "require") \
#   .mode("overwrite") \
#   .save()

# COMMAND ----------

spark.catalog.clearCache()

# COMMAND ----------

# df_ly_mtd = df.withColumn("LYMTD", F.sum("value").over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)))

# COMMAND ----------

# w_LM= Window.partitionBy(['customer_code','material_code']).orderBy('year', 'month')

# sales_df_25 = sales_df_24.withColumn("LM", lag(col("MTD")).over(w_LM))

# COMMAND ----------

# sales_df_26 = sales_df_25.withColumn("growth_percentage_MTD_vs_LYMTD",((col("MTD")-col("LYMTD"))/col("LYMTD"))*100)

# COMMAND ----------

#display(sales_df_26.filter(col("customer_code").rlike("103976")))

# COMMAND ----------

# month_mapping = {
#             4: 1,  # April
#             5: 2,  # May
#             6: 3,  # June
#             7: 4,  # July
#             8: 5,  # August
#             9: 6,  # September
#             10: 7,  # October
#             11: 8,  # November
#             12: 9,  # December
#             1: 10,  # January
#             2: 11,  # February
#             3: 12,  # March
#         }
 
 
        # Create a new column for the custom month number
# sales_df_24 = sales_df_23.withColumn("month", when(sales_df_23["month"] >= 4, sales_df_23["month"] - 3).otherwise(sales_df_23["month"] + 9))

# COMMAND ----------

# window_spec = Window.partitionBy('exception_key').orderBy("year", "month")
# sales_df_25 = sales_df_24.withColumn("prev_month_metric", lag(col("billing_amount")).over(window_spec))

# COMMAND ----------

# display(sales_df_1.withColumn("Count", count(col("X")).over(Window.partitionBy("business_segment"))).select("business_segment", "Count").distinct())

# COMMAND ----------

# display(sales_df_1)

# COMMAND ----------

# 'billing_date',
#  'billing_type','profit_center',
#  'plant','secondary_pack_qty',
#  'secondary_pkt_type','brand_billed',
#  'distributors',
#  'distributors_category',
#  'customer_budget',

# COMMAND ----------

# w= Window.partitionBy(['key',
#  'customer_group',
#  'customer_code',
#  'material_code',
#  'ref_invoice_number',
#  'item_category',
#  'sales_org',
#  'tax_amount',
#  'billing_item',
#  'material_description',
#  'quantity',
#  'UoM',
#  'billed_quantity',
#  'net_value',
#  'qty_in_kg',
#  'billing_amount',
#  'product_category',
#  'product_brand',
#  'brand_flavour',
#  'mrp',
#  'pack_size',
#  'pack_format',
#  'mrp/cb',
#  'no_of_bisc/pk',  
#  'material_group',
#  'business_segment',
#  'customer_name',
#  'region',
#  'state_code',
#  'district',
#  'zone',
#  'zsm_emp_code',
#  'zsm',
#  'zsm_email',
#  'rsm_emp_code',
#  'rsm',
#  'rsm_email',
#  'asm_emp_code',
#  'asm',
#  'asm_email',
#  'so_emp_code',
#  'so',
#  'so_email',
#  'town_villa',
#  'customer_group_description',
#  'mrp_group',
#  'trade',
#  'focus_brand',
#  'state_name',
#  'type',
#  '80/20 rule',
#  "billing_month"])

# COMMAND ----------

# base_sales_data_aggregated = base_sales_data_3.withColumn("billing_month", trunc(to_date(col("billing_date"),"yyyyMMdd"), "month")).withColumn("tax_amount",sum(col("tax_amount")).over(w)).withColumn("quantity",sum(col("quantity")).over(w)).withColumn("billed_quantity",sum(col("billed_quantity")).over(w)).withColumn("net_value",sum(col("net_value")).over(w)).withColumn("qty_in_kg",sum(col("qty_in_kg")).over(w)).drop('billing_date',
#  'billing_type','profit_center',
#  'plant',
#  ).dropDuplicates()

# COMMAND ----------

# display(base_sales_data_3.select("billing_month","billing_date").distinct())

# COMMAND ----------

# base_sales_data_3 = sales_df_1.persist()

# COMMAND ----------

# driver = "org.mariadb.jdbc.Driver"
# database_host = "anmolmysqldb-flex.mysql.database.azure.com"
# database_port = "3306"
# database_name = "anmol_dev"

# url = f"jdbc:mysql://{database_host}:{database_port}/{database_name}?useSSL=true&connectTimeout=5000"


# base_sales_data_3.write \
#   .format("jdbc") \
#   .option("driver", driver) \
#   .option("url", url) \
#   .option("dbtable", "primary_sales_prod") \
#   .option("user", "dbadmin") \
#   .option("password", "Welcome2tata#") \
#   .option("sslmode", "require") \
#   .mode("overwrite") \
#   .save()

# COMMAND ----------

# base_sales_data_3.unpersist()
