/*************************************************************************************************************
Script:             Provider sample data 1
Create Date:        2023-11-12
Author:             B. Klein
Description:        Sets up customer sample data and collection metadata
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-11-12          B. Klein                            Initial Creation
2023-11-15          B. Klein                            Incorporated target data from A. Kommini
2024-01-18          B. Klein                            Renaming to Dynamic Data Model Mapper
2024-05-01          B. Barker                           Updated Sample Data
*************************************************************************************************************/

/* cleanup */
use role accountadmin;
drop database if exists dmm_collection_metadata_db;
drop database if exists dmm_customer_sample_db;


/* set up roles */
use role accountadmin;
call system$wait(5);


/* create role and add permissions required by role for installation of framework */
create role if not exists dmm_data_mapper_role;


/* perform grants */
set myusername = current_user();
grant role dmm_data_mapper_role to user identifier($myusername);
grant role dmm_data_mapper_role to role accountadmin;
/* with grant option needed to grant permissions to app */
grant create database on account to role dmm_data_mapper_role with grant option;
grant execute task on account to role dmm_data_mapper_role with grant option;

/* for adding the shares from the consumers */
grant import share on account to role dmm_data_mapper_role;

/* for creating the application package and listing */
grant create application package on account to role dmm_data_mapper_role;
grant create application on account to role dmm_data_mapper_role;
grant create data exchange listing on account to role dmm_data_mapper_role;

/* for reading account metadata */
grant imported privileges on database snowflake to role dmm_data_mapper_role;

grant role dmm_data_mapper_role to role sysadmin;
grant create warehouse on account to role dmm_data_mapper_role with grant option;

/* set role and warehouse */
use role dmm_data_mapper_role;
call system$wait(5);


/* create warehouse */
create warehouse if not exists dmm_data_mapper_wh
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* database and schema for target collection metadata - data that is used to create target collections */
create or replace database dmm_collection_metadata_db;
create or replace schema product;

create or replace TABLE dmm_collection_metadata_db.product.SITE (SITE_NAME VARCHAR(80), SITE_ID VARCHAR(80), SITE_CITY VARCHAR(16777216),  SITE_STATE VARCHAR(50));

create or replace TABLE dmm_collection_metadata_db.product.PRODUCT (PRODUCT_NAME VARCHAR(200), PRODUCT_ID NUMBER(38,0), COST_PER_ITEM FLOAT, PRODUCT_DESCRIPTION VARCHAR(16777216), PRODUCT_PRICE FLOAT, PRODUCT_COST FLOAT);

create or replace TABLE dmm_collection_metadata_db.product.INVENTORY (SITE_ID VARCHAR(80), PRODUCT_ID NUMBER(38,0), AMOUNT FLOAT, LAST_COUNTED_DATE TIMESTAMP_NTZ(9));


--------------------------------------------------------------------------------------------------------------------------------------------------
-- Load target collection metadat to the attribute table
-- Stored procedure to update the entity tables
---------------------------------------------------------------------------------------------------------------------------------------------------
create or replace procedure dmm_collection_metadata_db.product.generate_attributes(attr_table varchar, target_collection_name varchar, target_entity_name varchar, version varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_attributes'
as
$$
import snowflake.snowpark
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def generate_attributes(session:snowflake.snowpark.Session, attr_table:str, target_collection_name:str, target_entity_name:str, version:str):
    try:

        # generates show columns statement for source table
        show_col_sql_text = f"show columns in table {attr_table}"

        session.sql(show_col_sql_text).collect()

        # use the last_query_id to get results of the show col statement
        last_query_results_sql_text = f"""
            select
                    '{target_collection_name}' as target_collection_name
                ,   '{target_entity_name}' as target_entity_name
                ,   "column_name" as target_entity_attribute_name
                ,   object_construct(
                        'data_type',parse_json("data_type"):type::varchar,
                        'is_nullable',parse_json("data_type"):nullable::boolean,
                        'precision',parse_json("data_type"):precision::number,
                        'scale',parse_json("data_type"):scale::number,
                        'length',parse_json("data_type"):length::number,
                        'byte_length',parse_json("data_type"):byteLength::number,
                        'description',null
                    ) as target_attribute_properties

            from table(RESULT_SCAN(LAST_QUERY_ID()))
        """

        source_df = session.sql(last_query_results_sql_text).with_column("LAST_UPDATED_TIMESTAMP", current_timestamp())

        target_df = session.table('dmm_model_mapper_app_package.ADMIN.TARGET_ENTITY_ATTRIBUTE')
        target_df_collection = session.table('dmm_model_mapper_app_package.ADMIN.TARGET_COLLECTION')
        target_df_entity = session.table('dmm_model_mapper_app_package.ADMIN.TARGET_ENTITY')

        # merge with table on pk cols
        target_df.merge(
            source_df,
            (
                (target_df["TARGET_COLLECTION_NAME"] == source_df["TARGET_COLLECTION_NAME"]) &
                (target_df["TARGET_ENTITY_NAME"] == source_df["TARGET_ENTITY_NAME"]) &
                (target_df["TARGET_ENTITY_ATTRIBUTE_NAME"] == source_df["TARGET_ENTITY_ATTRIBUTE_NAME"])
            )
            ,
            [
                when_matched().update(
                    {
                        "TARGET_ATTRIBUTE_PROPERTIES": source_df["TARGET_ATTRIBUTE_PROPERTIES"],
                        "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"],
                        "VERSION": version           }
                ),
                when_not_matched().insert(
                    {
                        "TARGET_COLLECTION_NAME": source_df["TARGET_COLLECTION_NAME"],
                        "TARGET_ENTITY_NAME": source_df["TARGET_ENTITY_NAME"],
                        "TARGET_ENTITY_ATTRIBUTE_NAME": source_df["TARGET_ENTITY_ATTRIBUTE_NAME"],
                        "TARGET_ATTRIBUTE_PROPERTIES": source_df["TARGET_ATTRIBUTE_PROPERTIES"],
                        "VERSION": version
                    }
                )
            ]
        )

        source_df_text = f"""
            select
                    '{target_collection_name}' as target_collection_name
                ,   '{target_entity_name}' as target_entity_name
        """
        source_df_collect = session.sql(source_df_text).with_column("LAST_UPDATED_TIMESTAMP", current_timestamp())

        target_df_entity.merge(
            source_df_collect,
            (
                (target_df_entity["TARGET_COLLECTION_NAME"] == source_df_collect["TARGET_COLLECTION_NAME"]) &
                (target_df_entity["TARGET_ENTITY_NAME"] == source_df_collect["TARGET_ENTITY_NAME"])
            )
            ,
            [
                when_matched().update(
                    {
                        "LAST_UPDATED_TIMESTAMP": source_df_collect["LAST_UPDATED_TIMESTAMP"],
                        "VERSION": version             }
                ),
                when_not_matched().insert(
                    {
                        "TARGET_COLLECTION_NAME": source_df_collect["TARGET_COLLECTION_NAME"],
                        "TARGET_ENTITY_NAME": source_df_collect["TARGET_ENTITY_NAME"],
                        "VERSION": version
                    }
                )
            ]
        )

        target_df_collection.merge(
            source_df_collect,
            (
                (target_df_entity["TARGET_COLLECTION_NAME"] == source_df_collect["TARGET_COLLECTION_NAME"])
            )
            ,
            [
                when_matched().update(
                    {
                        "LAST_UPDATED_TIMESTAMP": source_df_collect["LAST_UPDATED_TIMESTAMP"],
                        "VERSION": version             }
                ),
                when_not_matched().insert(
                    {
                        "TARGET_COLLECTION_NAME": source_df_collect["TARGET_COLLECTION_NAME"],
                        "VERSION": version
                    }
                )
            ]
        )

        return "Operation Successful"
    except:
        return "Operation Failed"
$$
;

-- Load the metadata from tables to target collection tables
insert into dmm_model_mapper_app_package.admin.customer(customer_name, customer_snowflake_organization_name) values ('CustomerABC','CustomerABCOrgName');
call dmm_collection_metadata_db.product.generate_attributes('dmm_collection_metadata_db.product.SITE','Product','SITE', 'v1');
call dmm_collection_metadata_db.product.generate_attributes('dmm_collection_metadata_db.product.PRODUCT','Product','PRODUCT', 'v1');
call dmm_collection_metadata_db.product.generate_attributes('dmm_collection_metadata_db.product.INVENTORY','Product','INVENTORY', 'v1');

insert into dmm_model_mapper_app_package.admin.subscription(customer_name, target_collection_name, version, expiration_date) values ('CustomerABC','Product','v1','2026-01-01');


/* create sample customer data - this will be the raw data we map to our targets */
create or replace database dmm_customer_sample_db;
create or replace schema dmm_customer_sample_db.sample_data;
drop schema if exists dmm_customer_sample_db.public;


select 'Run code in provider_sample_data_2.sql on the provider account' as DO_THIS_NEXT;