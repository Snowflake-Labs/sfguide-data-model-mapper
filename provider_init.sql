/*************************************************************************************************************
Script:             Provider initialization
Create Date:        2023-08-09
Author:             B. Klein
Description:        Initializes provider back-end for Dynamic Data Model Mapper
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-08-09          B. Klein                            Initial Creation
2023-10-24          B. Klein                            Packaging into a native app
2024-01-18          B. Klein                            Renaming to Dynamic Data Model Mapper
*************************************************************************************************************/

/* cleanup */
use role accountadmin;
drop share if exists dmm_data_mapper_share;
drop application if exists dmm_model_mapper_app cascade;
alter listing if exists dmm_model_mapper_app set state = unpublished;
drop listing if exists dmm_model_mapper_app;
drop database if exists dmm_data_mapper_code;
drop database if exists dmm_model_mapper_app_package;


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
grant create share on account to role dmm_data_mapper_role;

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


/* let's create the app package - this defines the data content and application logic for an app, it is a provider-side object, and it not an installed instance of the actual app */
drop database if exists dmm_model_mapper_app_package;
create application package dmm_model_mapper_app_package
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';

/* admin holds all base tables, managed by administrator */
create or replace schema dmm_model_mapper_app_package.admin 
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';
drop schema if exists dmm_model_mapper_app_package.public;

/* customer list with Snowflake Org name */
create or replace table dmm_model_mapper_app_package.admin.customer (
    customer_name varchar,
    customer_snowflake_account_identifier varchar,
    customer_snowflake_organization_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (customer_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* target collection - group of entities that define a data model, can be analogous to "product" */
create or replace table dmm_model_mapper_app_package.admin.target_collection (
    target_collection_name varchar,
    version varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version) not enforced
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* customer target collection subscriptions - one customer to many target collections */
create or replace table dmm_model_mapper_app_package.admin.subscription (
    customer_name varchar,
    target_collection_name varchar,
    version varchar,
    expiration_date date,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (customer_name, target_collection_name) not enforced,
    constraint fkey_1 foreign key (customer_name) 
        references dmm_model_mapper_app_package.admin.customer (customer_name) not enforced,
    constraint fkey_2 foreign key (target_collection_name, version) 
        references dmm_model_mapper_app_package.admin.target_collection (target_collection_name, version) match partial not enforced
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* target entity - one target collection to many entities, can be analogous to "table" */
create or replace table dmm_model_mapper_app_package.admin.target_entity (
    target_collection_name varchar,
    version varchar,
    target_entity_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version, target_entity_name) not enforced,
    constraint fkey_1 foreign key (target_collection_name, version) 
        references dmm_model_mapper_app_package.admin.target_collection (target_collection_name, version) not enforced
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';


/* target entity attribute - one entity to many attributes, can be analogous to "column" */
create or replace table dmm_model_mapper_app_package.admin.target_entity_attribute (
    target_collection_name varchar,
    version varchar,
    target_entity_name varchar,
    target_entity_attribute_name varchar,
    target_attribute_properties object,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version, target_entity_name, target_entity_attribute_name) not enforced,
    constraint fkey_1 foreign key (target_collection_name, version, target_entity_name)
        references dmm_model_mapper_app_package.admin.target_entity (target_collection_name, version, target_entity_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';

/* grant read access to admin tables to the application package's built-in share */
grant usage on schema dmm_model_mapper_app_package.admin to share in application package dmm_model_mapper_app_package;
grant select on table dmm_model_mapper_app_package.admin.customer to share in application package dmm_model_mapper_app_package;
grant select on table dmm_model_mapper_app_package.admin.subscription to share in application package dmm_model_mapper_app_package;
grant select on table dmm_model_mapper_app_package.admin.target_collection to share in application package dmm_model_mapper_app_package;
grant select on table dmm_model_mapper_app_package.admin.target_entity to share in application package dmm_model_mapper_app_package;
grant select on table dmm_model_mapper_app_package.admin.target_entity_attribute to share in application package dmm_model_mapper_app_package;


/* Now that we have an application package, let's add some helper objects for deploying/redeploying the native app */
/* Staging the native app's files for versions/patches can be done through the UI, but this will let us perform it entirely through code */
/* note, this database should not be seen as a replacement for proper CI/CD processes */
create or replace database dmm_data_mapper_code comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';
create or replace schema dmm_data_mapper_code.content comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';
drop schema if exists dmm_data_mapper_code.public;
create or replace table dmm_data_mapper_code.content.file (
        NAME varchar
    ,   CONTENT varchar(16777216)
) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';
create or replace stage dmm_data_mapper_code.content.files_v1_0 comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';

/* constructs and puts files into a stage */
CREATE OR REPLACE PROCEDURE dmm_data_mapper_code.content.PUT_TO_STAGE(STAGE VARCHAR,FILENAME VARCHAR, CONTENT VARCHAR)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
PACKAGES=('snowflake-snowpark-python')
HANDLER='put_to_stage'
COMMENT='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
AS $$
import io
import os

def put_to_stage(session, stage, filename, content):
    local_path = '/tmp'
    local_file = os.path.join(local_path, filename)
    f = open(local_file, "w", encoding='utf-8')
    f.write(content)
    f.close()
    session.file.put(local_file, '@'+stage, auto_compress=False, overwrite=True)
    return "saved file "+filename+" in stage "+stage
$$;

insert into dmm_data_mapper_code.content.file (NAME, CONTENT)
values ('MANIFEST',
$$
manifest_version: 1

version:
  name: Data Mapper
  label: “Data Mapper v1.0”
  comment: “Helps model and share data with the provider for collaboration purposes”

artifacts:
  readme: README.md
  setup_script: setup_script.sql
  default_streamlit: user_interface.data_modeler_streamlit
  extension_code: true

privileges:
- CREATE DATABASE:
    description: "To create the shareback database"
- CREATE WAREHOUSE:
    description: "To create the warehouse used for orchestration"
- EXECUTE TASK:
    description: "To manage data orchestration tasks"
$$
);

insert into dmm_data_mapper_code.content.file SELECT 'SETUP' AS NAME, REGEXP_REPLACE($$
/*************************************************************************************************************
Script:             dmm Data Mapper - Native App - Setup Script v1
Create Date:        2023-10-25
Author:             B. Klein
Description:        Dynamic Data Model Mapper Native App -- Setup script that contains the objects that
                    the application will use when implemented on the consumer account.
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-25          B. Klein                            Initial Creation
*************************************************************************************************************/

/*** make sure this file is ready, but do not run in a worksheet ***/

/* now, we need to build out what the customer interacts with to map their data to the target collection */

/* create application role for consumer access - we will use this later but want it created early */
create or replace application role dmm_consumer_app_role;

/* modeling holds all tables associated with defining a data model */
create or alter versioned schema modeling comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';

/* python procedure to initialize the shared database, based on application_name */
create or replace procedure modeling.initialize_application()
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='initialize_application'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
import snowflake.snowpark
from snowflake.snowpark.functions import sproc

def initialize_application(session):
    # get current database (app name)
    application_name = session.sql("""
        select current_database()
    """).collect()[0][0]
    
    # create warehouse
    session.sql("""
        create warehouse if not exists dmm_model_mapper_app_wh comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # create database
    session.sql("""
        create or replace database dmm_model_mapper_share_db comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # create modeled schema for sharing mapped data
    session.sql("""
        create or replace schema dmm_model_mapper_share_db.modeled comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # create mapped schema for sharing mapped data
    session.sql("""
        create or replace schema dmm_model_mapper_share_db.mapped comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # create configuration schema for sharing app configuration
    session.sql("""
        create or replace schema dmm_model_mapper_share_db.configuration comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # create utility schema for creating helper stored procedures
    session.sql("""
        create or replace schema dmm_model_mapper_share_db.utility comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # drop unnecessary public schema
    session.sql("""
        drop schema if exists dmm_model_mapper_share_db.public
    """).collect()

    # create configuration tables

    # source collection - group of entities that define a source data model
    # target_collection_name refers to the intended target collection for mapping
    # target_entity_name refers to the intended target entity for mapping
    # refresh frequency will be used the the orchestration feature later
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_collection (
        source_collection_name varchar,
        target_collection_name varchar,
        version varchar,
        target_entity_name varchar,
        custom_sql varchar,
        use_custom_sql boolean,
        generated_mapping_table varchar,
        refresh_frequency varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # source entity with fully qualified origin - can be analogous to "source table"
    # note - only one source entity can be a base entity within a collection - all other entities are aggregated - it determines the cardinality of the collection
    # is_base_entity indicates that a source entity is the source entity to be joined on by other entities
    # join_from_source_entity_name specifies what existing source entity to join to
    # join_type should be typical SQL types - inner, left, right, etc.
    # join from is the left and join to is the right side of a join condition
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_entity (
        source_collection_name varchar,
        source_entity_name varchar,
        entity_fully_qualified_source varchar,
        is_base_entity boolean,
        join_from_source_entity_name varchar,
        join_type varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name, source_entity_name) not enforced,
        constraint fkey_1 foreign key (source_collection_name) 
            references dmm_model_mapper_share_db.configuration.source_collection (source_collection_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # multiple join condition support, e.g. a.col1=b.col1 and a.col2=b.col2
    # join_from_entity_attribute_name - an attribute/column for the "from" side, "left" side of the join condition
    # operator - =, <, >, <=, >=
    # join_to_entity_attribute_name - an attribute/column for the "to" side, "right" side fo the join condition
    # note - multiple join conditions are combined with an AND
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_entity_join_condition (
        source_collection_name varchar,
        source_entity_name varchar,
        join_from_source_entity_name varchar,
        join_from_entity_attribute_name varchar,
        operator varchar,
        join_to_entity_attribute_name varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name, source_entity_name, join_from_source_entity_name, operator, join_to_entity_attribute_name) not enforced,
        constraint fkey_1 foreign key (source_collection_name, source_entity_name) 
            references dmm_model_mapper_share_db.configuration.source_entity (source_collection_name, source_entity_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # source entity attribute - one entity to many attributes, can be analogous to "column"
    # note - aggregation will only be applied to non-base entity attributes
    # include_in_entity determines if a column is surfaced in the final table or not - allows for hiding irrelevant cols
    # derived_expression should be a scalar expression using attributes of the same entity, and null if not a derived col
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_entity_attribute (
        source_collection_name varchar,
        source_entity_name varchar,
        source_entity_attribute_name varchar,
        source_attribute_properties object,
        include_in_entity boolean,
        derived_expression varchar,
        aggregation_function varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name, source_entity_name, source_entity_attribute_name) not enforced,
        constraint fkey_1 foreign key (source_collection_name, source_entity_name) 
            references dmm_model_mapper_share_db.configuration.source_entity (source_collection_name, source_entity_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # multiple filter condition support, e.g. col1=2000 and col2='ABC'
    # left_filter_expression - an expression for the "left" side of the filter condition
    # operator - =, <, >, <=, >=
    # right_filter_expression - an expression for the "right" side of the filter condition
    # note - multiple filter conditions are combined with an AND
    # applied AFTER the joins are performed
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_collection_filter_condition (
        source_collection_name varchar,
        left_filter_expression varchar,
        operator varchar,
        right_filter_expression varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name, left_filter_expression, operator, right_filter_expression) not enforced,
        constraint fkey_1 foreign key (source_collection_name) 
            references dmm_model_mapper_share_db.configuration.source_collection (source_collection_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # defines the column mappings between source collection and target entity, populated by streamlit
    session.sql("""
    create table if not exists dmm_model_mapper_share_db.configuration.source_to_target_mapping (
        source_collection_name varchar,
        generated_mapping_table_column_name varchar,
        target_attribute_name varchar,
        last_updated_timestamp timestamp default current_timestamp(),
        constraint pkey primary key (source_collection_name) not enforced,
        constraint fkey_1 foreign key (source_collection_name) 
            references dmm_model_mapper_share_db.configuration.source_collection (source_collection_name) not enforced
        ) comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
    """).collect()

    # grant application role permissions to created objects
    session.sql("""
        grant monitor on warehouse dmm_model_mapper_app_wh to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant usage on database dmm_model_mapper_share_db to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant usage on schema dmm_model_mapper_share_db.modeled to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant usage on schema dmm_model_mapper_share_db.mapped to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant usage on schema dmm_model_mapper_share_db.configuration to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant usage on schema dmm_model_mapper_share_db.utility to application role dmm_consumer_app_role
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_collection to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_collection_filter_condition to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_entity to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_entity_attribute to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_entity_join_condition to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        grant select on table dmm_model_mapper_share_db.configuration.source_to_target_mapping to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    session.sql("""
        create or replace stage dmm_model_mapper_share_db.utility.python_stg
    """.format(application_name=application_name)).collect()

    # create procedure for sharing views
    @sproc(name="dmm_model_mapper_share_db.utility.share_views", is_permanent=True, stage_location="@dmm_model_mapper_share_db.utility.python_stg", replace=True, packages=["snowflake-snowpark-python"])
    def share_views(session: snowflake.snowpark.Session) -> str:
        # get list of existing views
        session.sql("show views in dmm_model_mapper_share_db.mapped").collect()
        view_df = session.sql('select "name" from table(result_scan(last_query_id()))')

        for view_row in view_df.to_local_iterator():
            share_sql_string = 'grant select on view dmm_model_mapper_share_db.mapped."{view_name}" to share dmm_data_mapper_share'.format(view_name=view_row["name"])
            session.sql(share_sql_string).collect()

        return "Views shared successfully"

    # grant usage to application role
    session.sql("""
        grant usage on procedure dmm_model_mapper_share_db.utility.share_views() to application role dmm_consumer_app_role with grant option
    """.format(application_name=application_name)).collect()

    return 'Shared database initialized successfully'
:::
;



/* python procedure to initially populate entity attributes */
create or replace procedure modeling.generate_attributes(source_collection_name varchar, source_entity_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_attributes'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def generate_attributes(session, source_collection_name, source_entity_name):
    try:
        
        # entity dataframe
        entity_dataframe = session.table('dmm_model_mapper_share_db.configuration.source_entity') \
            .filter((col('source_collection_name') == source_collection_name) & (col('source_entity_name') == source_entity_name))

        # gets the fully qualified source table
        source_table = str(entity_dataframe \
            .select(col('entity_fully_qualified_source')).distinct().collect()[0][0])

        # gets whether or not the entity is a base entity
        join_from_source_entity_name = str(entity_dataframe \
            .select(col('join_from_source_entity_name')).distinct().collect()[0][0])

        # generates show columns statement for source table
        show_col_sql_text = "show columns in table identifier('{source_table}')".format(source_table=source_table)

        session.sql(show_col_sql_text).collect()

        # use the last_query_id to get results of the show col statement
        last_query_results_sql_text = """
            select
                    '{source_collection_name}' as source_collection_name
                ,   '{source_entity_name}' as source_entity_name
                ,   "column_name" as source_entity_attribute_name
                ,   object_construct(
                        'data_type',parse_json("data_type"):type::varchar,
                        'is_nullable',parse_json("data_type"):nullable::boolean,
                        'precision',parse_json("data_type"):precision::number,
                        'scale',parse_json("data_type"):scale::number,
                        'length',parse_json("data_type"):length::number,
                        'byte_length',parse_json("data_type"):byteLength::number,
                        'description',null
                    ) as source_attribute_properties
                ,   TRUE as include_in_entity
                ,   NULL as derived_expression
                ,   case
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'TEXT') then 'LISTAGG'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'FIXED') then 'SUM'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'BOOLEAN') then 'LISTAGG'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'DATE') then 'LISTAGG'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'TIMESTAMP_NTZ') then 'MAX'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'TIMESTAMP_LTZ') then 'MAX'
                        when ('{join_from_source_entity_name}' <> 'None' and parse_json("data_type"):type::varchar = 'TIMESTAMP_TZ') then 'MAX'
                        when ('{join_from_source_entity_name}' <> 'None') then 'MAX'
                        else null
                    end as aggregation_function
            from table(RESULT_SCAN(LAST_QUERY_ID()))
        """.format(source_collection_name=source_collection_name, source_entity_name=source_entity_name, join_from_source_entity_name=join_from_source_entity_name)

        source_df = session.sql(last_query_results_sql_text).with_column("LAST_UPDATED_TIMESTAMP", current_timestamp())

        target_df = session.table('dmm_model_mapper_share_db.configuration.SOURCE_ENTITY_ATTRIBUTE')

        # merge with table on pk cols
        target_df.merge(
            source_df,
            (
                (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["SOURCE_ENTITY_NAME"] == source_df["SOURCE_ENTITY_NAME"]) &
                (target_df["SOURCE_ENTITY_ATTRIBUTE_NAME"] == source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"])
            )
            ,
            [
                when_matched().update(
                    {
                        "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                        "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                        "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                        "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                        "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"]
                    }
                ),
                when_not_matched().insert(
                    {
                        "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                        "SOURCE_ENTITY_NAME": source_df["SOURCE_ENTITY_NAME"],
                        "SOURCE_ENTITY_ATTRIBUTE_NAME": source_df["SOURCE_ENTITY_ATTRIBUTE_NAME"],
                        "SOURCE_ATTRIBUTE_PROPERTIES": source_df["SOURCE_ATTRIBUTE_PROPERTIES"],
                        "INCLUDE_IN_ENTITY": source_df["INCLUDE_IN_ENTITY"],
                        "DERIVED_EXPRESSION": source_df["DERIVED_EXPRESSION"],
                        "AGGREGATION_FUNCTION": source_df["AGGREGATION_FUNCTION"],
                        "LAST_UPDATED_TIMESTAMP": source_df["LAST_UPDATED_TIMESTAMP"]
                    }
                )
            ]
        )

        return "Operation Successful"
    except:
        return "Operation Failed"
:::
;

/* mapping holds the dynamic tables used prior to mapping to target entites */
create or alter versioned schema mapping comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}';

/* python procedure to generate dynamic sql */
create or replace procedure modeling.generate_collection_model(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_collection_model'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched, lit, \
any_value as any_value_, avg as avg_, corr as corr_, count as count_, covar_pop as covar_pop_, covar_samp as covar_samp_, \
listagg as listagg_, max as max_, median as median_, min as min_, mode as mode_, percentile_cont as percentile_cont_, \
stddev as stddev_, stddev_pop as stddev_pop_, stddev_samp as stddev_samp_, sum as sum_, \
var_pop as var_pop_, var_samp as var_samp_, variance as variance_
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField
import re

# function to enrich dynamic_df
def enrich_dataframe(session, source_entity_df, base_entity_name, dynamic_df):
    source_collection_name = source_entity_df \
        .select(col("SOURCE_COLLECTION_NAME")).collect()[0][0]
    
    # construct group by
    base_col_list = dynamic_df.columns

    group_dmm_string = ""

    for item in base_col_list:
        if group_dmm_string == "":
            group_dmm_string = 'col("{item}")'.format(item=item)
        else:
            group_dmm_string += ', col("{item}")'.format(item=item)

    col_aggregation_dict = {}

    # recursive CTE to get all related offspring, ordered by depth, then entity name
    offspring_sql = '''
    with recursive offspring
(
        source_collection_name
    ,   source_entity_name
    ,   entity_fully_qualified_source
    ,   is_base_entity
    ,   join_from_source_entity_name
    ,   join_type
    ,   depth
)
as
(
    select
            se.source_collection_name
        ,   se.source_entity_name
        ,   se.entity_fully_qualified_source
        ,   se.is_base_entity
        ,   se.join_from_source_entity_name
        ,   se.join_type
        ,   '*' as depth
    from dmm_model_mapper_share_db.configuration.source_entity se
    where is_base_entity = true and source_collection_name = '{source_collection_name}'

    union all

    select
            se.source_collection_name
        ,   se.source_entity_name
        ,   se.entity_fully_qualified_source
        ,   se.is_base_entity
        ,   se.join_from_source_entity_name
        ,   se.join_type
        ,   o.depth || '*'
    from dmm_model_mapper_share_db.configuration.source_entity se
    inner join offspring o on se.join_from_source_entity_name = o.source_entity_name
)

select
        se.source_collection_name
    ,   se.source_entity_name
    ,   se.entity_fully_qualified_source
    ,   se.is_base_entity
    ,   se.join_from_source_entity_name
    ,   se.join_type
from offspring se
where se.is_base_entity = false
order by depth asc, source_entity_name asc;
    '''.format(source_collection_name=source_collection_name)

    child_entity_list = session.sql(offspring_sql).collect()
    
    # if child entities exist, start joining
    for child_entity_row in child_entity_list:
        join_type = child_entity_row["JOIN_TYPE"]
        
        # get child details
        child_entity_name = child_entity_row["SOURCE_ENTITY_NAME"]
        child_entity_fully_qualified = child_entity_row["ENTITY_FULLY_QUALIFIED_SOURCE"]

        # create dataframe to join
        to_join_df = session.table(child_entity_fully_qualified)

        ########## ATTRIBUTES ##########
        # get columns in to_join_df
        to_join_df = get_attributes(session, source_entity_df, child_entity_name, to_join_df)
        to_join_col_list = to_join_df.columns

        # get list of aggregations
        to_join_agg_list = get_aggregations(session, source_entity_df, child_entity_name, to_join_df)

        # string for column selection and aliasing statement
        column_select_and_alias_string = ""
        
        # get current dynamic_df columns
        current_col_list = dynamic_df.columns

        # add all current dynamic_df cols to the string
        for item in current_col_list:
            if column_select_and_alias_string == "":
                column_select_and_alias_string = 'col("{item}")'.format(item=item)
            else:
                column_select_and_alias_string += ', col("{item}")'.format(item=item)

        # find matching columns
        matched_col_list = list(set(current_col_list).intersection(to_join_col_list))

        # string for column preparation
        to_join_column_prep_string = ""
        
        # build up col select/alias statement
        for idx, item in enumerate(to_join_col_list):
            matching_item_found = False   

            for matched_item in matched_col_list:
                if matched_item == item:
                    matching_item_found = True

                    # alias duplicate cols by prepending <child_entity_name>__
                    if to_join_column_prep_string == "":
                        to_join_column_prep_string = 'col("{item}").alias("{child_entity_name}__{item}")'.format(child_entity_name=child_entity_name,item=item)
                    else:
                        to_join_column_prep_string += ', col("{item}").alias("{child_entity_name}__{item}")'.format(child_entity_name=child_entity_name,item=item)

                    if to_join_agg_list[idx]:
                        col_aggregation_dict[child_entity_name.upper() + "__" + item.upper()] = to_join_agg_list[idx]
            
            # add to string if not a duplicate
            if not matching_item_found:
                if to_join_column_prep_string == "":
                    to_join_column_prep_string = 'col("{item}")'.format(item=item)
                else:
                    to_join_column_prep_string += ', col("{item}")'.format(item=item)

                if to_join_agg_list[idx]:
                        col_aggregation_dict[item.upper()] = to_join_agg_list[idx]

        to_join_column_prep_statement = "to_join_df.select({to_join_column_prep_string})".format(to_join_column_prep_string=to_join_column_prep_string)

        # updates column names to prevent duplicates
        to_join_df = eval(to_join_column_prep_statement)

        ########## JOIN CONDITIONS ##########
        # get join conditions
        join_conditions_list = session.table("dmm_model_mapper_share_db.configuration.source_entity_join_condition") \
            .filter((col("SOURCE_COLLECTION_NAME") == source_collection_name) & \
                (col("SOURCE_ENTITY_NAME") == child_entity_name) & \
                (col("JOIN_FROM_SOURCE_ENTITY_NAME") == base_entity_name)).collect()

        # string for creating join condition statement
        join_condition_string = ""

        # iterate through join conditions
        for join_condition_row in join_conditions_list:
            join_from_attribute_name = join_condition_row["JOIN_FROM_ENTITY_ATTRIBUTE_NAME"].upper()
            sql_operator = join_condition_row["OPERATOR"]
            join_to_attribute_name = join_condition_row["JOIN_TO_ENTITY_ATTRIBUTE_NAME"].upper()

            # check if join_to attribute is a duplicate, if so, update to prepend <child_entity_name>__
            for matched_item in matched_col_list:
                if join_to_attribute_name == matched_item:
                    join_to_attribute_name = child_entity_name + "__" + join_to_attribute_name

            # convert sql comparison operator to python equivalent
            if sql_operator == "=":
                sp_operator = "=="
            elif sql_operator == ">=":
                sp_operator = ">="
            elif sql_operator == "<=":
                sp_operator = "<="
            elif sql_operator == ">":
                sp_operator = ">"
            elif sql_operator == "<":
                sp_operator = "<"
            elif sql_operator == "<>":
                sp_operator = "!="
            else:
                return "Invalid operator found"

            # assumes & operator only
            if join_condition_string == "":
                join_condition_string = "(dynamic_df.{join_from_attribute_name} {sp_operator} to_join_df.{join_to_attribute_name})" \
                    .format(join_from_attribute_name=str(join_from_attribute_name), sp_operator=sp_operator, join_to_attribute_name=str(join_to_attribute_name))
            else:
                join_condition_string += " & (dynamic_df.{join_from_attribute_name} {sp_operator} to_join_df.{join_to_attribute_name})" \
                    .format(join_from_attribute_name=str(join_from_attribute_name), sp_operator=sp_operator, join_to_attribute_name=str(join_to_attribute_name))

        # join dynamic_df and new to_join_df based on one-to-many join conditions
        join_statement = "dynamic_df.join(to_join_df, {join_condition_string})".format(join_condition_string=str(join_condition_string))

        # updates dynamic_df by running join statement
        dynamic_df = eval(join_statement)

    column_list = dynamic_df.columns

    # create group by expr statement and eval
    group_dmm_exp_statement = "dynamic_df.groupBy({group_dmm_string})".format(group_dmm_string=group_dmm_string)
    dynamic_df = eval(group_dmm_exp_statement)

    aggregation_string = ""

    # build aggregation string
    for column in column_list:
        if column in col_aggregation_dict:
            if aggregation_string == "":
                aggregation_string = '{aggregation}_("{column}").alias("{column}")'.format(column=column,aggregation=col_aggregation_dict[column].lower())
            else:
                aggregation_string += ', {aggregation}_("{column}").alias("{column}")'.format(column=column,aggregation=col_aggregation_dict[column].lower())

    agg_exp_statement = "dynamic_df.agg({aggregation_string})".format(aggregation_string=aggregation_string)
    dynamic_df = eval(agg_exp_statement)

    return dynamic_df


# function to get aggregations
def get_aggregations(session, source_entity_df, source_entity_name, source_df): 
    # get source collection name
    source_collection_name = source_entity_df \
        .select(col("SOURCE_COLLECTION_NAME")).collect()[0][0]

    col_list = source_df.columns

    aggregation_list = []

    for col_name in col_list:
        # remove source entity name from a detected duplicate
        col_name = re.sub(source_entity_name + "__", "", col_name, 1)

        # get attribute
        aggregation_function = session.table('dmm_model_mapper_share_db.configuration.source_entity_attribute') \
            .filter( \
                (col("SOURCE_COLLECTION_NAME") == source_collection_name) &  \
                (col("SOURCE_ENTITY_NAME") == source_entity_name) &  \
                (col("SOURCE_ENTITY_ATTRIBUTE_NAME") == col_name)) \
            .select(col("AGGREGATION_FUNCTION")).collect()[0][0]

        aggregation_list.append(aggregation_function)

    return aggregation_list


# function to get attributes
def get_attributes(session, source_entity_df, source_entity_name, source_df):
    # store dataframe we will dynamically update
    altered_source_df = source_df
    
    # get source collection name
    source_collection_name = source_entity_df \
        .select(col("SOURCE_COLLECTION_NAME")).collect()[0][0]

    # get attributes
    all_attributes_df = session.table('dmm_model_mapper_share_db.configuration.source_entity_attribute') \
        .filter( \
            (col("SOURCE_COLLECTION_NAME") == source_collection_name) &  \
            (col("SOURCE_ENTITY_NAME") == source_entity_name))

    # if no attributes, generate them
    if all_attributes_df.count() == 0:
        procedure_call_statement = "call modeling.generate_attributes('{source_collection_name}', '{source_entity_name}')".format(source_collection_name=source_collection_name, source_entity_name = source_entity_name)
        session.sql(procedure_call_statement).collect()

        all_attributes_df = session.table('dmm_model_mapper_share_db.configuration.source_entity_attribute') \
            .filter( \
                (col("SOURCE_COLLECTION_NAME") == source_collection_name) &  \
                (col("SOURCE_ENTITY_NAME") == source_entity_name))

    # get attribute df, filtering by collection, entity, and if included
    attributes_df = all_attributes_df.filter(col("INCLUDE_IN_ENTITY") == True)

    # get non-derived attribute names
    non_derived_attribute_names_df = attributes_df.filter(col("DERIVED_EXPRESSION").isNull()) \
        .select(col("SOURCE_ENTITY_ATTRIBUTE_NAME"))

    # get derived attribute names
    derived_attribute_names_df = attributes_df.filter(col("DERIVED_EXPRESSION").isNotNull()) \
        .select(col("SOURCE_ENTITY_ATTRIBUTE_NAME"), col("DERIVED_EXPRESSION"))

    # string to select included columns, will eventually use select_expr
    col_select_string = ""

    # get attributes
    for row in non_derived_attribute_names_df.to_local_iterator():
        if col_select_string == "":
            col_select_string += '"{col}"'.format(col=row[0])
        else:
            col_select_string += ', "{col}"'.format(col=row[0])

    # index 1 is the derived expression, we'll update the col name later
    for row in derived_attribute_names_df.to_local_iterator():
        if col_select_string == "":
            col_select_string += '"{expr}"'.format(expr=row[1]) 
        else:
            col_select_string += ', "{expr}"'.format(expr=row[1])

    # create select expr statement and eval
    select_exp_statement = "altered_source_df.select_expr({col_select_string})".format(col_select_string=col_select_string)
    altered_source_df = eval(select_exp_statement)

    # update col name for derived cols
    for row in derived_attribute_names_df.to_local_iterator():
        with_col_rename_statement = 'altered_source_df.with_column_renamed("{existing_col}", "{new_col}")'.format(existing_col=row[1].upper(), new_col=row[0])
        altered_source_df = eval(with_col_rename_statement)

    return altered_source_df


# filter dataframe
def filter_dataframe(session, source_collection_name, dynamic_df):
    filter_conditions_df = session.table('dmm_model_mapper_share_db.configuration.source_collection_filter_condition') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name)

    # return same dataframe if no filter conditions
    if filter_conditions_df.count() == 0:
        return dynamic_df
    else:
        filter_statement = ""
        
        for row in filter_conditions_df.to_local_iterator():
            left_expr = row["LEFT_FILTER_EXPRESSION"]
            sql_operator = row["OPERATOR"]
            right_expr = row["RIGHT_FILTER_EXPRESSION"]
            
            # only support ANDs to prevent overly complicating UI
            if filter_statement == "":
                filter_statement = '{left_expr} {sql_operator} {right_expr}'.format(left_expr=left_expr,sql_operator=sql_operator,right_expr=right_expr)
            else:
                filter_statement += 'AND {left_expr} {sql_operator} {right_expr}'.format(left_expr=left_expr,sql_operator=sql_operator,right_expr=right_expr)
        
        # create filter expr statement and eval
        filter_exp_statement = 'dynamic_df.filter("{filter_statement}")'.format(filter_statement=filter_statement)
        dynamic_df = eval(filter_exp_statement)

        return dynamic_df


# entry point function
def generate_collection_model(session, source_collection_name):
    # get source entity df for collection
    source_entity_df = session.table('dmm_model_mapper_share_db.configuration.source_entity') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name)

    # check if custom sql
    use_custom_sql = session.table('dmm_model_mapper_share_db.configuration.source_collection') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name) \
        .select(col("USE_CUSTOM_SQL")) \
        .collect()[0][0]

    if use_custom_sql:
        custom_sql_string = session.table('dmm_model_mapper_share_db.configuration.source_collection') \
            .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name) \
            .select(col("CUSTOM_SQL")) \
            .collect()[0][0]

        dynamic_df = session.sql(custom_sql_string)
    else:
        # get df with no join froms (should only be one per collection)
        base_entity_df = source_entity_df \
            .filter(col("IS_BASE_ENTITY") == True)

        # get the name for the source entity
        base_entity_name = str(base_entity_df \
            .select(col("SOURCE_ENTITY_NAME")) \
            .collect()[0][0])

        # get the fully qualified source for the source entity
        base_entity_fully_qualified = str(base_entity_df \
            .select(col("ENTITY_FULLY_QUALIFIED_SOURCE")) \
            .collect()[0][0])

        # update dynamic df - we will use this to collect our transformations as we go
        dynamic_df = session.table(base_entity_fully_qualified)

        # get attributes
        dynamic_df = get_attributes(session, source_entity_df, base_entity_name, dynamic_df)

        # method to add relevant, recursive joins and transformations to dynamic_df
        dynamic_df = enrich_dataframe(session, source_entity_df, base_entity_name, dynamic_df)

        # add filtering
        dynamic_df = filter_dataframe(session, source_collection_name, dynamic_df)

    # ensure that the collection name meets normal table naming rules
    generated_mapping_table = "dmm_model_mapper_share_db.modeled." + source_collection_name.replace(" ","_")
 
    # save table name to source collection
    session.sql("""
        update dmm_model_mapper_share_db.configuration.source_collection
        set generated_mapping_table = '{generated_mapping_table}'
        where source_collection_name = '{source_collection_name}'
    """.format(generated_mapping_table=generated_mapping_table, source_collection_name=source_collection_name)).collect()

    # create a dynamic table in mapping schema
    dynamic_df.create_or_replace_dynamic_table(name=generated_mapping_table, warehouse="dmm_model_mapper_app_wh", lag="DOWNSTREAM")

    session.sql("""
        grant select on table {generated_mapping_table} to application role dmm_consumer_app_role with grant option
    """.format(generated_mapping_table=generated_mapping_table)).collect()

    return "Collection dynamic table created"
:::
;

/* python procedure get columns from the source collection dynamic tables to map-from, used by Streamlit to grab columns */
create or replace procedure mapping.get_mapfrom_columns(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='get_mapfrom_columns'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def get_mapfrom_columns(session, source_collection_name):
    # source collection
    source_collection_df = session.table('dmm_model_mapper_share_db.configuration.source_collection') \
        .filter(col('source_collection_name') == source_collection_name)

    # generated table
    generated_mapping_table = source_collection_df.select(col('generated_mapping_table')).collect()[0][0]

    # table df
    generated_mapping_table_df = session.table(generated_mapping_table)

    return generated_mapping_table_df.columns
:::
;

/* python procedure get attributes for target entities to map-to, used by Streamlit to grab columns */
create or replace procedure mapping.get_mapto_columns(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='get_mapto_columns'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def get_mapto_columns(session, source_collection_name):
    # source collection
    source_collection_df = session.table('dmm_model_mapper_share_db.configuration.source_collection') \
        .filter(col('source_collection_name') == source_collection_name)

    # target collection
    target_collection_name = source_collection_df.select(col('target_collection_name')).collect()[0][0]

    # custom SQL

    # version
    version = source_collection_df.select(col('version')).collect()[0][0]

    # target entity
    target_entity_name = source_collection_df.select(col('target_entity_name')).collect()[0][0]

    # target entity attribute df
    target_entity_attribute_df = session.table('admin.target_entity_attribute') \
        .filter( \
            (col('target_collection_name') == target_collection_name) & \
            (col('version') == version) & \
            (col('target_entity_name') == target_entity_name))

    return target_entity_attribute_df.columns
:::
;

/* python procedure to generate the secure view in the share database */
create or replace procedure mapping.generate_view(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_view'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def generate_view(session, source_collection_name):
    # get current database (app name)
    application_name = session.sql("""
        select current_database()
    """).collect()[0][0]
    
    # source collection
    source_collection_df = session.table('dmm_model_mapper_share_db.configuration.source_collection'.format(application_name=application_name)) \
        .filter(col('source_collection_name') == source_collection_name)

    # generated table
    generated_mapping_table = source_collection_df.select(col('generated_mapping_table')).collect()[0][0]

    dynamic_df = session.table(generated_mapping_table)

    # source to target mapping
    source_to_target_mapping_df = session.table('dmm_model_mapper_share_db.configuration.source_to_target_mapping'.format(application_name=application_name)) \
        .filter(col('source_collection_name') == source_collection_name)

    # string to select included columns, will eventually use select_expr
    col_select_string = ""

    # get attributes
    for row in source_to_target_mapping_df.to_local_iterator():
        if col_select_string == "":
            col_select_string += '"{col}"'.format(col=row[1])
        else:
            col_select_string += ', "{col}"'.format(col=row[1])

    # create select expr statement and eval
    select_exp_statement = "dynamic_df.select_expr({col_select_string})".format(col_select_string=col_select_string)
    dynamic_df = eval(select_exp_statement)

    # update col name for derived cols
    for row in source_to_target_mapping_df.to_local_iterator():
        with_col_rename_statement = """dynamic_df.with_column_renamed("{existing_col}", "'{new_col}'")""".format(existing_col=row[1].upper(), new_col=row[2])
        dynamic_df = eval(with_col_rename_statement)

    # target collection
    target_collection_name = source_collection_df.select(col('target_collection_name')).collect()[0][0]

    # version
    version = source_collection_df.select(col('version')).collect()[0][0]

    # target entity
    target_entity_name = source_collection_df.select(col('target_entity_name')).collect()[0][0]

    view_name = '"DMM_MODEL_MAPPER_SHARE_DB"."MAPPED"."' + target_collection_name + '__' + version + '__' + target_entity_name + '"'
    
    dynamic_df.create_or_replace_view(view_name)

    session.sql('alter view ' + view_name + ' set secure').collect()

    session.sql("""
        grant select on view {view_name} to application role dmm_consumer_app_role with grant option
    """.format(view_name=view_name)).collect()

    return 'Secure view generated successfully'
:::
;

/* create user_interface schema */
create or alter versioned schema user_interface;

/* create reference procedure */
/* this callback is used by the UI to ultimately bind a reference that expects one value */
create or replace procedure user_interface.register_single_callback(ref_name string, operation string, ref_or_alias string)
returns string
language sql
as :::
    begin
        case (operation)
            when 'ADD' then
                select system$set_reference(:ref_name, :ref_or_alias);
            when 'REMOVE' then
                select system$remove_reference(:ref_name);
            when 'CLEAR' then
                select system$remove_reference(:ref_name);
            else
                return 'Unknown operation: ' || operation;
        end case;
        return 'Operation ' || operation || ' succeeded';
    end;
:::
;

/* create Streamlits */
create or replace streamlit user_interface.data_modeler_streamlit
from '/streamlit'
main_file = '/data_modeler.py'
;

grant usage on schema user_interface to application role dmm_consumer_app_role;
grant usage on streamlit user_interface.data_modeler_streamlit to application role dmm_consumer_app_role;

grant usage on schema modeling to application role dmm_consumer_app_role;
grant usage on procedure modeling.initialize_application() to application role dmm_consumer_app_role;

grant usage on schema modeling to application role dmm_consumer_app_role;
grant select on all tables in schema modeling to application role dmm_consumer_app_role;
grant usage on procedure modeling.generate_attributes(varchar, varchar) to application role dmm_consumer_app_role;
grant usage on procedure modeling.generate_collection_model(varchar) to application role dmm_consumer_app_role;


grant usage on schema mapping to application role dmm_consumer_app_role;
grant select on all tables in schema mapping to application role dmm_consumer_app_role;
grant usage on procedure mapping.get_mapfrom_columns(varchar) to application role dmm_consumer_app_role;
grant usage on procedure mapping.get_mapto_columns(varchar) to application role dmm_consumer_app_role;
grant usage on procedure mapping.generate_view(varchar) to application role dmm_consumer_app_role;


/************ DEMO MODE ************/
/************ Should be removed before production ************/
/* python procedure to add demo data */
create or replace procedure modeling.deploy_demo()
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='deploy_demo'
comment='{"origin":"sf_ps_wls","name":"dmm","version":{"major":1, "minor":0},"attributes":{"component":"dmm"}}'
as
:::
def deploy_demo(session):
    # get current database (app name)
    application_name = session.sql("""
        select current_database()
    """).collect()[0][0]
    
    # being inserting demo data
    session.sql("""
        insert overwrite into dmm_model_mapper_share_db.configuration.source_collection(source_collection_name, target_collection_name, version, target_entity_name, refresh_frequency) values ('RetailSource', 'Retail', 'v1', 'STG_ITEM', null)
    """.format(application_name=application_name)).collect()

    # items & item locations
    session.sql("""
        insert overwrite into dmm_model_mapper_share_db.configuration.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
        select
                'RetailSource'
            ,   'Items'
            ,   'dmm_customer_sample_db.sample_data.items'
            ,   TRUE
            ,   null
            ,   null
        union
        select
                'RetailSource'
            ,   'ItemLocations'
            ,   'dmm_customer_sample_db.sample_data.item_locations'
            ,   FALSE
            ,   'Items'
            ,   'LEFT'
    """.format(application_name=application_name)).collect()

    # insert derived columns
    session.sql("""
        insert overwrite into dmm_model_mapper_share_db.configuration.source_entity_attribute (source_collection_name, source_entity_name, source_entity_attribute_name, source_attribute_properties, include_in_entity, derived_expression, aggregation_function)
        select
                'RetailSource'
            ,   'Items'
            ,   'itemid_col_upper_i'
            ,   NULL
            ,   TRUE
            ,   'upper(itemid)'
            ,   NULL
        union
        select
                'RetailSource'
            ,   'ItemLocations'
            ,   'INCRSTOCKOUTCOST_col_upper_il'
            ,   NULL
            ,   FALSE
            ,   'upper(INCRSTOCKOUTCOST)'
            ,   'LISTAGG'
    """.format(application_name=application_name)).collect()

    # generate attributes for entity sample data
    session.sql("""
        call {application_name}.modeling.generate_attributes('RetailSource', 'Items')
    """.format(application_name=application_name)).collect()

    session.sql("""
        call {application_name}.modeling.generate_attributes('RetailSource', 'ItemLocations')
    """.format(application_name=application_name)).collect()

    # insert join condition
    session.sql("""
        insert overwrite into dmm_model_mapper_share_db.configuration.source_entity_join_condition(source_collection_name, source_entity_name, join_from_source_entity_name, join_from_entity_attribute_name, operator, join_to_entity_attribute_name)
        select
                'RetailSource'
            ,   'ItemLocations'
            ,   'Items'
            ,   'ITEMID'
            ,   '='
            ,   'ItemLocations__ITEMID'
    """.format(application_name=application_name)).collect()

    # add example filter conditions
    session.sql("""
        insert into dmm_model_mapper_share_db.configuration.source_collection_filter_condition(source_collection_name, left_filter_expression, operator, right_filter_expression)
        select
                'RetailSource'
            ,   '1'
            ,   '='
            ,   'left(1,1)'
    """.format(application_name=application_name)).collect()

    session.sql("""
        call {application_name}.modeling.generate_collection_model('RetailSource')
    """.format(application_name=application_name)).collect()

    # demonstration, Streamlit will call these when facilitating mapping
    session.sql("""
        call {application_name}.mapping.get_mapfrom_columns('RetailSource')
    """.format(application_name=application_name)).collect()

    session.sql("""
        call {application_name}.mapping.get_mapto_columns('RetailSource')
    """.format(application_name=application_name)).collect()

    # example data
    session.sql("""
        insert into dmm_model_mapper_share_db.configuration.source_to_target_mapping (source_collection_name, generated_mapping_table_column_name, target_attribute_name)
        select
                'RetailSource'
            ,   'ITEMID'
            ,   'ITEM_ID'
    """.format(application_name=application_name)).collect()

    session.sql("""
        call {application_name}.mapping.generate_view('RetailSource')
    """.format(application_name=application_name)).collect()

    return 'Demo data inserted successfully'
:::
;

/* create application role for demo data access - not for normal usage */
create or replace application role dmm_demo_app_role;

/* grant access to procedure */
grant usage on procedure modeling.deploy_demo() to application role dmm_demo_app_role;

$$,':::','$$') as CONTENT;

insert into dmm_data_mapper_code.content.file (NAME , CONTENT)
values ( 'README',$$
# Data Mappper
      
The Data Mappper faciliates modeling, mapping, and sharing consumer data with the provider in a standard format.

$$);

/* put files into stage */
CALL dmm_data_mapper_code.content.PUT_TO_STAGE('files_v1_0','manifest.yml',(SELECT CONTENT FROM dmm_data_mapper_code.content.file WHERE NAME = 'MANIFEST'));
CALL dmm_data_mapper_code.content.PUT_TO_STAGE('files_v1_0','setup_script.sql', (SELECT CONTENT FROM dmm_data_mapper_code.content.file WHERE NAME = 'SETUP'));
CALL dmm_data_mapper_code.content.PUT_TO_STAGE('files_v1_0','README.md', (SELECT CONTENT FROM dmm_data_mapper_code.content.file WHERE NAME = 'README'));

select 'Stage files in the toStage folder (one directory at a time, skipping .streamlit) to dmm_data_mapper_code.content.files_v1_0 using Snowsight, and then run code in provider_sample_data_1.sql on the provider account' as DO_THIS_NEXT;