# DataIntoResults Data Brewery # 

DataIntoResults [Data Brewery](https://databrewery.co/) is an ETL (Extract-Transform-Load) program that connect to many data sources (cloud services, databases, ...) and manage data warehouse workflow.


What is it for
---------------

Data Brewery is an ETL tool for data warehouse building meaning :

* extracting data from many sources ranging from web services (e.g. Google Analytics), web scrapping, files (e.g. local, over SFTP, Google sheets) and databases (e.g. MySQL, PostgreSQL, MongoDB).
* designing and modeling your data warehouses using a module approach in a ELT way (Extract-Load-Transform). Most of the transformation are in SQL and leverage you data warehouse computing capability.

![DataIntoResults Manifesto](https://dataintoresults.com/docs/_images/mdw-white.png)

You can find more documentation on the [documentation website](https://databrewery.co/docs/).


Simple example
---------------

The example of an definition file (XML based) show you how to defined 2 data sources (for a prestashop ERP and Google Analytics) as well as 3 modules (2 for replicating the distant data, and one for moduling business queries).

You can find a more comprehensive example at the [Getting Started](https://databrewery.co/docs/getting_started/installation.html) section of the documentation.

```xml
<dataWarehouse>
    <!-- Define where the data warehouse is located. -->
    <datastore name="dw" type="postgresql" host="dw.theowner.com" database="datawarehouse"
        user="john" password="Doe" sshUser="john">
    </datastore>

    <!-- Define where the ERP (prestashop) is located. -->
    <datastore name="prestashop" type="mysql" host="theowner.com" database="prestashop"
        user="john" password="Doe" sshUser="john">
        <!-- With autodiscover, DataFactory will analyse the prestashop schema of the database to get tables.  -->
        <autodiscovery schema="prestashop"/>
    </datastore>

    <!-- The Google Analytics data store.  -->
    <!-- Don't forget to allow DataFactory to access to your Google Analytics view.  -->
    <datastore name="ga" type="googleAnalytics" viewId="123456789">
        <table name="sessions" startDate="365daysago" endDate="yesterday">
            <!-- For the googleAnalytics datastore, notice that we use gaName and gaType.
                gaName is the reference for the column in Google Analytics.
                gaType is dimension or measure depending on the Google type.
            -->
            <column name="date" type="varchar" gaName="ga:date" gaType="dimension"/>
            <column name="medium" type="varchar" gaName="ga:medium" gaType="dimension"/>
            <column name="source" type="varchar" gaName="ga:source" gaType="dimension"/>
            <column name="campaign" type="varchar" gaName="ga:campaign" gaType="dimension"/>
            <column name="country" type="varchar" gaName="ga:country" gaType="dimension"/>
            <column name="city" type="varchar" gaName="ga:city" gaType="dimension"/>
            <column name="sessions" type="bigint" gaName="ga:sessions" gaType="measure"/>
            <column name="bounces" type="bigint" gaName="ga:bounces" gaType="measure"/>
            <column name="pageviews" type="bigint" gaName="ga:pageviews" gaType="measure"/>
        </table>
    </datastore>

    <!-- Integration layer module 1 : prestashop  -->
    <!-- In the database it will be stored under the prestashop schema  -->
    <module name="prestashop" datastore="dw">
        <!-- We replicate all the prestashop datastore-->
        <replicate datastore="prestashop"/>
    </module>

    <!-- Integration layer module 2 : Google Analytics  -->
    <!-- In the database it will be stored under the google_analytics schema  -->
    <module name="google_analytics" datastore="dw">
        <!-- We replicate all the ga (Google Analytics) datastore-->
        <replicate datastore="ga"/>
    </module>

    <!-- Refine/Presentation layer module : Queries for business analysts  -->
    <!-- In the database it will be store under the reporting schema  -->
    <module name="reporting" datastore="dw">
        <!-- We define a table -->
        <table name="customer">
            <!-- Notice we skip the columns definition. We take what the query will give us -->
            <!-- The query is plain SQL and tell DataFactory how to fill the table -->
            <source type="query">
                <![CDATA[
select id_customer, email, count(1) as nb_orders, min(newsletter_date_add) as newsletter_add, min(invoice_date) as first_invoice_date,
    max(invoice_date) as last_invoice_date, sum(total_paid) as total_revenues, min(total_paid) as smaller_invoice, max(total_paid) as larger_invoice, min(birthday) as birthday
from prestashop.ps_orders
inner join prestashop.ps_customer using (id_customer)
where email not in ('pub@prestashop.com', 'bob@theowner.com', 'qa@theowner.com' )
    and payment <> 'Commande gratuite'
group by 1, 2
order by 2 desc]]>
            </source>
        </table>
        <table name="invoice">
            <source type="query">
            <!-- As you can see we can reference of this module, but only if there are defined above
            the table in the specification -->
                <![CDATA[
select 
	id_order, id_customer, invoice_date, 
	case when invoice_date = first_invoice_date then 1 else 0 end as new_customer, 
	1 as nb_transactions,	total_paid as revenues
from prestashop.ps_orders o
inner join prestashop.ps_customer using (id_customer)
inner join reporting.customer c using( id_customer)
where payment <> 'Free';]]>
            </source>
        </table>
    </module>
</dataWarehouse>
```

Supported data sources
-----------------------

Databases :

* PostgreSQL
* MySQL
* Microsoft SQL Server
* MongoDB

Web services :

* Google Analytics
* Google Search Console
* Hubspot
* Odoo

Other : 

* Flat files (read and write)
* Excel files (read and write)
* Google Sheets (read and write)
* SFTP (read and write)

How to build
--------------

Data Brewery is a Scala projet that use SBT as a build tool. You can download SBT [here ](https://www.scala-sbt.org/download.html).

The following command will produce a .zip in the baton/target/universal directory named baton-xxx.zip where xxx will be the version built.

```
sbt ipa/dist
```

You can decompress this archive and the executable is under the bin directory (baton for Linux/MacOS or baton.bat for Windows).

