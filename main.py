# from neo4j import __version__ as neo4j_version
# print(neo4j_version)
from neo4j import GraphDatabase
import pandas as pd


class Neo4jConnection:

    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


# CREATE BD
conn = Neo4jConnection(uri="bolt://localhost:7687", user="Mario", pwd="123123")
conn.query("CREATE OR REPLACE DATABASE coradb")



# PRODUCT

#  unitPrice,productID,productName
#  18.0,1,Chai
#  19.0,2,Chang
#  10.0,3,Aniseed Syrup
#  22.0,4,Chef Anton’s Cajun Seasoning
#  21.35,5,Chef Anton’s Gumbo Mix
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Product.csv'
AS line FIELDTERMINATOR ','
MERGE (product:Product {productID: line.ProductID})
  ON CREATE SET product.productName = line.ProductName, product.UnitPrice = toFloat(line.UnitPrice);
'''
conn.query(query_string, db='coradb')
# ORDER

# shipName,orderID
# Vins et alcools Chevalier,10248
# Toms Spezialitäten,10249
# Victuailles en stock,10250
# Suprêmes délices,10251
# Hanari Carnes,10252
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv'
AS row FIELDTERMINATOR ','
MERGE (order:Order {orderID: row.OrderID})
ON CREATE SET order.ShipName = row.ShipName
'''
conn.query(query_string, db='coradb')
# SUPPLIER

#  supplierID,companyName
#  1,Exotic Liquids
#  2,New Orleans Cajun Delights
#  3,Grandma Kelly’s Homestead
#  4,Tokyo Traders
#  5,Cooperativa de Quesos 'Las Cabras
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Supplier.csv'
AS line FIELDTERMINATOR ','
MERGE (supplier:Supplier {supplierID: line.SupplierID})
  ON CREATE SET supplier.companyName = line.CompanyName;
'''
conn.query(query_string, db='coradb')

# CATEGORY

#  description,categoryName,categoryID
#  "Soft drinks, coffees, teas, beers, and ales",Beverages,1
#  "Sweet and savory sauces, relishes, spreads, and seasonings",Condiments,2
#  "Desserts, candies, and sweet breads",Confections,3
#  Cheeses,Dairy Products,4
#  "Breads, crackers, pasta, and cereal",Grains/Cereals,5
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Category.csv'
AS line FIELDTERMINATOR ','
MERGE (c:Category {categoryID: line.CategoryID})
  ON CREATE SET c.categoryName = line.CategoryName, c.description = line.Description;
'''
conn.query(query_string, db='coradb')

# EMPLOYEE

#  lastName,firstName,employeeID,title
#  Davolio,Nancy,1,Sales Representative
#  Fuller,Andrew,2,"Vice President, Sales"
#  Leverling,Janet,3,Sales Representative
#  Peacock,Margaret,4,Sales Representative
#  Buchanan,Steven,5,Sales Manager
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Employee.csv'
AS line FIELDTERMINATOR ','
MERGE (e:Employee {employeeID:line.EmployeeID})
  ON CREATE SET e.firstName = line.FirstName, e.lastName = line.LastName, e.title = line.Title;
'''
conn.query(query_string, db='coradb')



# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv'
# AS line FIELDTERMINATOR ','
# MATCH (order:Order {orderID: line.orderID})
# MATCH (employee:Employee {employeeID: line.employeeID})
# MERGE (employee)-[:SOLD]->(order)
# '''
# conn.query(query_string, db='coradb')

# MATCH (age_id:Paper {name: line.name}),(name_id:fullName {name: line.name})
# LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv' AS row
# MATCH (order:Order {orderID: row.orderID})
# MATCH (employee:Employee {employeeID: row.employeeID})
# CREATE (employee)-[:SOLD]->(order);


# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/bd_1.csv'
# AS line FIELDTERMINATOR ','
# MATCH (age_id:Paper {name: line.name}),(name_id:fullName {name: line.name})
# MERGE (age_id)-[:AGE_IS]->(name_id)
# '''
# conn.query(query_string, db='coradb')



# query_string = '''
# // Create relationships between orders and products
# LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv'
# AS line
# MATCH (order:Order {orderID: line.orderID})
# MATCH (product:Product {productID: line.productID})
# MERGE (order)-[op:CONTAINS]->(product)
# ON CREATE SET op.unitPrice = toFloat(line.unitPrice), op.quantity = toFloat(line.Quantity);
# '''
# conn.query(query_string, db='coradb')

# # DATASET
# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/bd_1.csv'
# AS line FIELDTERMINATOR ','
# CREATE (:Paper {id: line.age, name: line.name, age: line.age})
# '''
# conn.query(query_string, db='coradb')
#
# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/bd.csv'
# AS line FIELDTERMINATOR ','
# CREATE (:fullName {id: line.name, name: line.name, fio: line.fio})
# '''
# conn.query(query_string, db='coradb')
#
# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/bd_1.csv'
# AS line FIELDTERMINATOR ','
# MATCH (age_id:Paper {name: line.name}),(name_id:fullName {name: line.name})
# MERGE (age_id)-[:AGE_IS]->(name_id)
# '''
# conn.query(query_string, db='coradb')


# query_string = '''
# CALL gds.graph.create(
#  'coraGraph',
#  'Paper',
#  'AGE_IS'
# )
# '''
# conn.query(query_string, db='coradb')


# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/bd_1.csv'
# AS line FIELDTERMINATOR ','
# MATCH (citing_paper:Paper {name: line.name}),(cited_paper:Paper {age: line.name})
# MERGE (citing_paper)-[:AGE_IS]->(cited_paper)
# '''
# conn.query(query_string, db='coradb')
#
# query_string = '''
# CALL gds.graph.create(
#  'coraGraph',
#  'Paper',
#  'AGE_IS'
# )
# '''
# conn.query(query_string, db='coradb')


# query_string = ''' USING PERIODIC COMMIT
# LOAD CSV FROM 'file:///C:/Users/yanp-/OneDrive/Рабочий стол/Neo4j_articles/bd.csv' AS row FIELDTERMINATOR ','
# CREATE (:Person {name: row[1], age: row[2]})
# '''
# query_string ='''WITH "file:///bd.csv" AS uri
# LOAD CSV WITH HEADERS FROM uri AS row
# MERGE (c:Character {name:row[1]})   'https://raw.githubusercontent.com/ngshya/datasets/master/cora/cora_content.csv'
# https://docs.google.com/spreadsheets/d/1l56Ey_1zAl2kW-Tkp7ZTSYmlwINsAt-vyVHxEJKmAJo/edit?usp=sharing
# '''


# query_string = '''
# USING PERIODIC COMMIT 500
# LOAD CSV WITH HEADERS FROM
# 'https://raw.githubusercontent.com/ngshya/datasets/master/cora/cora_cites.csv'
# AS line FIELDTERMINATOR ','
# MATCH (citing_paper:Paper {id: line.citing_paper_id}),(cited_paper:Paper {id: line.cited_paper_id})
# CREATE (citing_paper)-[:CITES]->(cited_paper)
# '''
# conn.query(query_string, db='coradb')
# print(1)

# query_string = '''
# MATCH ()-->(p:Person)
# RETURN id(p), count(*) as indegree
# ORDER BY indegree DESC LIMIT 10
# '''
# conn.query(query_string, db='coradb')
#
# query_string = '''
# MATCH (p:Paper)
# RETURN DISTINCT p.class
# ORDER BY p.class
# '''
# conn.query(query_string, db='coradb')
# print(2)
#
# query_string = '''
# CALL gds.graph.create(
#  'coraGraph',
#  'Paper',
#  'CITES'
# )
# '''
# conn.query(query_string, db='coradb')
#
#
# query_string = '''
# CALL gds.graph.create(
#  'coraGraph',
#  'Paper',
#  'CITES'
# )
# '''
# conn.query(query_string, db='coradb')
# print(3)
#
#
# query_string = '''
# CALL gds.pageRank.write('coraGraph', {
#   writeProperty: 'pagerank'
# })
# YIELD nodePropertiesWritten, ranIterations
# '''
# conn.query(query_string, db='coradb')
#
# query_string = '''
# CALL gds.betweenness.write('coraGraph', {
#   writeProperty: 'betweenness' })
# YIELD minimumScore, maximumScore, scoreSum, nodePropertiesWritten
# '''
# conn.query(query_string, db='coradb')
#
