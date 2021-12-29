from neo4j import GraphDatabase
from pandas import DataFrame

class Neo4jConnection:
    # Creating a database
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        if self.driver is not None:
            self.driver.close()
    # The method that passes the request to the database
    def query(self, query, db=None):
        assert self.driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.driver.session(database=db) if db is not None else self.driver.session()
            response = list(session.run(query))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


conn = Neo4jConnection(uri="bolt://localhost:7687", user="Mario", password="123123")
conn.query("CREATE OR REPLACE DATABASE graphDb")

# LOAD PRODUCT
query_string = '''
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Product.csv'
AS line FIELDTERMINATOR ','
MERGE (product:Product {productID: line.ProductID})
  ON CREATE SET product.productName = line.ProductName, product.UnitPrice = toFloat(line.UnitPrice);
'''
conn.query(query_string, db='graphDb')

# LOAD ORDER
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv'
AS row FIELDTERMINATOR ','
MERGE (order:Order {orderID: row.OrderID})
ON CREATE SET order.ShipName = row.ShipName, order.Quantity = row.Quantity
'''
conn.query(query_string, db='graphDb')

# LOAD SUPPLIER
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Supplier.csv'
AS line FIELDTERMINATOR ','
MERGE (supplier:Supplier {supplierID: line.SupplierID})
  ON CREATE SET supplier.companyName = line.CompanyName;
'''
conn.query(query_string, db='graphDb')

# CATEGORY
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Category.csv'
AS line FIELDTERMINATOR ','
MERGE (c:Category {categoryID: line.CategoryID})
  ON CREATE SET c.categoryName = line.CategoryName, c.description = line.Description;
'''
conn.query(query_string, db='graphDb')

# LOAD EMPLOYEE
query_string = '''
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM
'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Employee.csv'
AS row FIELDTERMINATOR ','
MERGE (e:Employee {employeeID:row.EmployeeID})
  ON CREATE SET e.firstName = row.FirstName, e.lastName = row.LastName, e.post = row.Post;
'''
conn.query(query_string, db='graphDb')

# LINK NODES: ORDER and EMPLOYEES
query_string ='''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv' AS line
MATCH (order:Order {orderID: line.OrderID})
MATCH (employee:Employee {employeeID: line.EmployeeID})
CREATE (employee)-[:SOLD]->(order);
'''
conn.query(query_string, db='graphDb')

# LINK NODES: ORDER and PRODUCT
query_string ='''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv' AS line
MATCH (order:Order {orderID: line.OrderID})
MATCH (product:Product {productID: line.ProductID})
MERGE (order)-[op:PURCHASED]->(product)
  ON CREATE SET op.quantity = toFloat(line.Quantity);
'''
conn.query(query_string, db='graphDb')

# LINK NODES: ORDER and EMPLOYEES
query_string ='''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Order.csv' AS line
MATCH (order:Order {orderID: line.OrderID})
MATCH (employee:Employee {employeeID: line.EmployeeID})
CREATE (employee)-[:SOLD]->(order);
'''
conn.query(query_string, db='graphDb')

# LINK NODES: PRODUCT and SUPPLIER
query_string ='''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Product.csv
' AS line
MATCH (product:Product {productID: line.ProductID})
MATCH (supplier:Supplier {supplierID: line.SupplierID})
MERGE (supplier)-[:SUPPLIES]->(product);
'''
conn.query(query_string, db='graphDb')

# LINK NODES: PRODUCT and CATEGORY
query_string ='''
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/Mario-cartoon/bd/main/Product.csv' AS line
MATCH (product:Product {productID: line.ProductID})
MATCH (category:Category {categoryID: line.CategoryID})
MERGE (product)-[:IS_CATEGORY]->(category);
'''
conn.query(query_string, db='graphDb')


# UPLOAD DATA FROM EMPLOYEE
query_string = '''
MATCH (em:Employee)
RETURN em.firstName, em.lastName, em.post
'''
dtf_data = DataFrame([dict(_) for _ in conn.query(query_string, db='graphDb')])
print(dtf_data)

