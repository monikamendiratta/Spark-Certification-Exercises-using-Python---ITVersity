pyspark --master yarn --conf spark.ui.port=12643

orders = sc.textFile("/user/pkandi/data-master/retail_db/orders")
customers = sc.textFile("/user/pkandi/data-master/retail_db/customers")

ordersMap = orders.map(lambda order: (int(order.split(",")[2]),1))
customersMap = customers.map(lambda c: (int(c.split(",")[0]),(c.split(",")[2],c.split(",")[1])))

customersLeftOuterJoinOrders = customersMap.leftOuterJoin(ordersMap)

inactiveCustomers = customersLeftOuterJoinOrders.filter(lambda cust: cust[1][1] == None)
for i in inactiveCustomers.take(10)

nameOfCustomers = inactiveCustomers.map(lambda name: name[1][0][0] + "," + name[1][0][1]) 

nameOfCustomers.saveAsTextFile("Location") 

