from pyspark.sql import SparkSession,SQLContext
from pyspark.sql.functions import *
import configparser

# Crawler for json files
def crawl_json(inp_path,table_name):

    df = spark.read.json(inp_path)
    # Check if there are any array values  (1:many)
    if "array" in str(df):
        col_list = ""
        # Resolving 1:many into 1:1 types
        for i in df.dtypes:
            if "array<string>" in i:
                col_list = col_list+"explode("+i[0]+") as "+i[0]+","
            else:
                col_list = col_list+i[0]+","
        # Formatting the columns in select clause to create new df
        sqlQuery = "select "+col_list[:-1]+" from temp"
        print("Column list is {0} \n type is {1}".format(col_list[:-1],type(col_list)))
        print("SQL Query after rebuilding : \n {0}".format(sqlQuery))
        df.registerTempTable("Temp")
        df = spark.sql(sqlQuery)
        
    else:
        pass
    
    df.registerTempTable(table_name)
    return table_name

# Crawler for parquet files    
def crawl_parquet(inp_path,table_name):
    df = spark.read.parquet(inp_path)
    df.registerTempTable(table_name)
    return table_name
    
# Extract function that generates the final output file. 
# This function can be even written using spark native methods instead of spark sql    
def generate_extract(sql,out_path):
    
    print(sql)
    df = spark.sql(sql)
    df.coalesce(1).write.partitionBy("dt").format("csv").mode("overwrite").option("header","true").save(out_path)

#  Execution starts here. Guard
if __name__ == "__main__":
    
    # Reading the values from config file
    config = configparser.ConfigParser()
    config.read("C:\git\SparkPost\config\config.cfg")
    
    dt = config['DEFAULT']['dt']
    cnt = config['DEFAULT']['threshold']
    group = config['DEFAULT']['group']
    
    inp_customers = config['DEFAULT']['inp_customers']
    inp_events = config['DEFAULT']['inp_events']
    inp_mailboxPrvdr = config['DEFAULT']['inp_mailboxPrvdr']

    out_extract = config['DEFAULT']['out_extract']

    # Initializing Spark Session 
    spark = SparkSession.builder.master("local[1]").appName("SparkPost").getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    
    # Crawl through the input files and create a table on files
    crawl_json(inp_customers,"customers")
    crawl_json(inp_mailboxPrvdr,"mailbox_provider")
    crawl_parquet(inp_events,"events")
    
    # SQL to generate the extract. This can be fed from the Param file as well
    extract_sql = """ SELECT *,
	(injection_count/sum(injection_count) over (partition by customer_id))*100 as inj_traffic_pct FROM
    (SELECT c.customer_id, domains, dt, count(type) as injection_count
    FROM customers c
    JOIN events e ON c.customer_id = e.customer_id
    JOIN mailbox_provider m ON e.routing_domain = m.domains
    WHERE is_active = 'true'
    AND dt = {0}
    AND ('*' = {2} OR group = {2})
    GROUP BY 1,2,3)
    WHERE injection_count > {1}""".format(dt,cnt,group)
    
    # Function that generates the final extract
    generate_extract(extract_sql,out_extract)