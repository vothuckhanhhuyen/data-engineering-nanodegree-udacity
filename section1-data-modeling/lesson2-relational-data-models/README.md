## Key Points

#### In this lesson, you'll learn the fundamentals of how to do relational data modeling by focusing on:
- Normalization
- Denormalization
- Fact/dimension tables
- Different schema models

#### Databases
- Database: A set of related data and the way it is organized
- Database Management System: Computer software that allows users to interact with the database and provides access to all of the data.
- The term database is often used to refer to both the database and the DBMS used.
- Rule 1: The information rule: 
    - All information in a relational database is represented explicitly at the logical level and in exactly one way – by values in tables.

#### Importance of Relational Databases:
- Standardization of data model: Once your data is transformed into the rows and columns format, your data is standardized and you can query it with SQL
- Flexibility in adding and altering tables: Relational databases gives you flexibility to add tables, alter tables, add and remove data.
- Data Integrity: Data Integrity is the backbone of using a relational database.
- Structured Query Language (SQL): A standard language can be used to access the data with a predefined language.
- Simplicity : Data is systematically stored and modeled in tabular format.
- Intuitive Organization: The spreadsheet format is intuitive but intuitive to data modeling in relational databases.

#### OLAP vs OLTP
- Online Analytical Processing (OLAP): Databases optimized for these workloads allow for complex analytical and ad hoc queries, including aggregations. These type of databases are optimized for reads.
- Online Transactional Processing (OLTP): Databases optimized for these workloads allow for less complex queries in large volume. The types of queries for these databases are read, insert, update, and delete.
- The key to remember the difference between OLAP and OLTP is analytics (A) vs transactions (T). If you want to get the price of a shoe then you are using OLTP (this has very little or no aggregations). If you want to know the total stock of shoes a particular store sold, then this requires using OLAP (since this will require aggregations).

#### Normalization and Denormalization
- Normalization will feel like a natural process, you will reduce the number of copies of the data and increase the likelihood that your data is correct in all locations.
    - Normalization organizes the columns and tables in a database to ensure that their dependencies are properly enforced by database integrity constraints.
    - We don’t want or need extra copies of our data, this is data redundancy. We want to be able to update data in one place and have that be the source of truth, that is data integrity.
- Denormalization will not feel as natural, as you will have duplicate copies of data, and tables will be more focused on the queries that will be run.

#### Objectives of Normal Form:
- To free the database from unwanted insertions, updates, & deletion dependencies
- To reduce the need for refactoring the database as new types of data are introduced
- To make the relational model more informative to users
- To make the database neutral to the query statistics

#### Normal Forms
- How to reach First Normal Form (1NF):
    - Atomic values: each cell contains unique and single values
    - Be able to add data without altering tables
    - Separate different relations into different tables
    - Keep relationships between tables together with foreign keys
- Second Normal Form (2NF):
    - Have reached 1NF
    - All columns in the table must rely on the Primary Key
- Third Normal Form (3NF):
    - Must be in 2nd Normal Form
    - No transitive dependencies
    - Remember, transitive dependencies you are trying to maintain is that to get from A-> C, you want to avoid going through B.
- When to use 3NF: When you want to update data, we want to be able to do in just 1 place. We want to avoid updating the table in the Customers Detail table (in the example in the lecture slide).

#### Denormalization:
- JOINS on the database allow for outstanding flexibility but are extremely slow. If you are dealing with heavy reads on your database, you may want to think about denormalizing your tables. You get your data into normalized form, and then you proceed with denormalization. So, denormalization comes after normalization.

#### Normalization vs Denormalization
- Normalization is about trying to increase data integrity by reducing the number of copies of the data. Data that needs to be added or updated will be done in as few places as possible.
- Denormalization is trying to increase performance by reducing the number of joins between tables (as joins can be slow). Data integrity will take a bit of a potential hit, as there will be more copies of the data (to reduce JOINS).

#### Fact and Dimension Tables
- The following [image](dimension-fact-tables.png) shows the relationship between the fact and dimension tables for the example shown in the video. As you can see in the image, the unique primary key for each Dimension table is included in the Fact table.
- In this example, it helps to think about the Dimension tables providing the following information:
    - Where the product was bought? (Dim_Store table)
    - When the product was bought? (Dim_Date table)
    - What product was bought? (Dim_Product table)
- The Fact table provides the metric of the business process (here Sales).
    - How many units of products were bought? (Fact_Sales table)

#### Star Schema
- Star Schema is the simplest style of data mart schema. The star schema consists of one of more fact tables referencing any number of dimension tables.
    - Gets its name from the physical model resembling a star shape
    - A fact table is at its center
    - Dimension table surrounds the fact table representing the star’s points.

#### Benefits of Star Schema
- Getting a table into 3NF is a lot of hard work, JOINs can be complex even on simple data
- Star schema allows for the relaxation of these rules and makes queries easier with simple JOINS
- Aggregations perform calculations and clustering of our data so that we do not have to do that work in our application. Examples : COUNT, GROUP BY etc

#### Snowflake Schema
- Star Schema is a special, simplified case of the snowflake schema.
- Star schema does not allow for one to many relationships while the snowflake schema does.
- Snowflake schema is more normalized than Star schema but only in 1NF or 2NF

#### Data Definition and Constraints
- NOT NULL
- UNIQUE
- PRIMARY KEY

#### Upsert
- In RDBMS language, the term upsert refers to the idea of inserting a new row in an existing table, or updating the row if it already exists in the table. The action of updating or inserting has been described as "upsert".
- The way this is handled in PostgreSQL is by using the INSERT statement in combination with the ON CONFLICT clause.
- The INSERT statement adds in new rows within the table. The values associated with specific target columns can be added in any order.
- Let's look at a simple example. We will use a customer address table as an example, which is defined with the following CREATE statement:
    ```
    CREATE TABLE IF NOT EXISTS customer_address (
        customer_id int PRIMARY KEY, 
        customer_street varchar NOT NULL,
        customer_city text NOT NULL,
        customer_state text NOT NULL
    );
    ```
- Let's try to insert data into it by adding a new row:
    ```
    INSERT into customer_address (
    VALUES
        (432, '758 Main Street', 'Chicago', 'IL'
    );
    ```
- Now let's assume that the customer moved and we need to update the customer's address. However we do not want to add a new customer id. In other words, if there is any conflict on the customer_id, we do not want that to change.
    This would be a good candidate for using the ON CONFLICT DO NOTHING clause.
    ```
    INSERT INTO customer_address (customer_id, customer_street, customer_city, customer_state)
    VALUES
    (
    432, '923 Knox Street', 'Albany', 'NY'
    ) 
    ON CONFLICT (customer_id) 
    DO NOTHING;
    ```
- Now, let's imagine we want to add more details in the existing address for an existing customer. This would be a good candidate for using the ON CONFLICT DO UPDATE clause.
    ```
    INSERT INTO customer_address (customer_id, customer_street)
    VALUES
        (
        432, '923 Knox Street, Suite 1' 
    ) 
    ON CONFLICT (customer_id) 
    DO UPDATE
        SET customer_street  = EXCLUDED.customer_street;
    ```

## Key Terms
- What makes a database a relational database and Codd’s 12 rules of relational database design
- The difference between different types of workloads for databases OLAP and OLTP
- The process of database normalization and the normal forms.
- Denormalization and when it should be used.
- Fact vs dimension tables as a concept and how to apply that to our data modeling
- How the star and snowflake schemas use the concepts of fact and dimension tables to make getting value out of the data easier.