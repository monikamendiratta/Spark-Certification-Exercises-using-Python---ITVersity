pyspark --master yarn --conf spark.ui.port=12643

orders = sc.textFile("/user/pkandi/data-master/retail_db/orders")
customers = sc.textFile("/user/pkandi/data-master/retail_db/customers")

from pyspark.sql import Row

ordersDF = orders.map(lambda orders: Row(orders_customer_id = int(orders.split(",")[2]))).toDF()
customersDF = customers.map(lambda customers: Row(customer_id = int(customers.split(",")[0]), lname =(customers.split(",")[2]), fname = (customers.split(",")[1]))).toDF()

ordersDF.registerTempTable("orders_df")
customersDF.registerTempTable("customers_df")

sqlContext.setConf("spark.sql.shuffle.partitions", "1")

ordersJoinCustomers = sqlContext.sql("select lname,fname from customers_df left outer join orders_df on customer_id = orders_customer_id where orders_customer_id is null order by lname,fname")

inactiveCustomerNames = ordersJoinCustomers.rdd.map(lambda row: row[0] + ", " + row[1])

inactiveCustomerNames..saveAsTextFile("/user/pkandi/InactiveCustomers")





