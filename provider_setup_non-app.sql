/*************************************************************************************************************
Script:             Provider setup non-app snapshot
Create Date:        2023-10-24
Author:             B. Klein
Description:        Non-app snapshot of Doctor Bernard Data Mapper - will deviate from app over time
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-24          B. Klein                            Initial Creation
***************************************************************************************************************************************************/

/* cleanup */
/*
use role accountadmin;
alter listing if exists drb_data_mapper_app_share set state = unpublished;
drop listing if exists drb_data_mapper_app_share;
drop share if exists drb_data_mapper_share;
drop database if exists drb_data_mapper_share_db;
drop database if exists drb_modeler_provider_db;
drop database if exists drb_customer_sample_db;
*/

/* set up roles */
use role accountadmin;
call system$wait(5);


/* create warehouse */
create warehouse if not exists drb_data_mapper_wh 
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* create role and add permissions required by role for installation of framework */
create role if not exists drb_data_mapper_role;


/* perform grants */
set myusername = current_user();
grant role drb_data_mapper_role to user identifier($myusername);
grant role drb_data_mapper_role to role accountadmin;
grant create database on account to role drb_data_mapper_role;
grant execute task on account to role drb_data_mapper_role;
grant role drb_data_mapper_role to role sysadmin;
grant usage, operate on warehouse drb_data_mapper_wh to role drb_data_mapper_role;
grant imported privileges on database snowflake to role drb_data_mapper_role;

/* needed for shareback, not needed by app */
grant create share on account to role drb_data_mapper_role;


/* set role and warehouse */
use role drb_data_mapper_role;
call system$wait(5);
use warehouse drb_data_mapper_wh;


/* create sample customer data - this will be the raw data we map to our targets */
create or replace database drb_customer_sample_db;
create or replace schema drb_customer_sample_db.sample_data;
drop schema if exists drb_customer_sample_db.public;
create or replace table drb_customer_sample_db.sample_data.sample_base_table (
    base_table_record_col varchar,
    base_table_varchar_col varchar,
    base_table_number_col number,
    base_table_bool_col boolean,
    base_table_date_col date,
    base_table_timestamp_col timestamp,
    constraint pkey primary key (base_table_record_col) not enforced
) CHANGE_TRACKING = TRUE;
create or replace table drb_customer_sample_db.sample_data.sample_agg_table (
    base_table_record_col varchar,
    agg_table_record_col varchar,
    agg_table_varchar_col varchar,
    agg_table_number_col number,
    agg_table_bool_col boolean,
    agg_table_date_col date,
    agg_table_timestamp_col timestamp,
    constraint pkey primary key (base_table_record_col, agg_table_record_col) not enforced,
    constraint fkey_1 foreign key (base_table_record_col) references drb_customer_sample_db.sample_data.sample_base_table (base_table_record_col) not enforced
) CHANGE_TRACKING = TRUE;
insert into drb_customer_sample_db.sample_data.sample_base_table values (
    'my_record',
    'some_varchar',
    7,
    true,
    '2023-01-01',
    current_timestamp()
);
insert into drb_customer_sample_db.sample_data.sample_agg_table values (
    'my_record',
    'my_agg_record',
    'agg_me',
    99.9,
    false,
    '2022-06-01',
    current_timestamp()
);
insert into drb_customer_sample_db.sample_data.sample_agg_table values (
    'my_record',
    'my_agg_record_2',
    'agg_me',
    0.1,
    false,
    '2022-06-01',
    current_timestamp()
);


/* we now have a customer database with some customer source data available.. no app yet */

/* make the app database */
create or replace database drb_data_mapper_db 
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
/* admin holds all base tables, managed by administrator */
create or replace schema drb_data_mapper_db.admin 
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
drop schema if exists drb_data_mapper_db.public;

/* customer list with Snowflake Org name */
create or replace table drb_data_mapper_db.admin.customer (
    customer_name varchar,
    customer_snowflake_organization_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (customer_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* target collection - group of entities that define a data model, can be analogous to "product" */
create or replace table drb_data_mapper_db.admin.target_collection (
    target_collection_name varchar,
    version varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* customer target collection subscriptions - one customer to many target collections */
create or replace table drb_data_mapper_db.admin.subscription (
    customer_name varchar,
    target_collection_name varchar,
    version varchar,
    expiration_date date,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (customer_name, target_collection_name) not enforced,
    constraint fkey_1 foreign key (customer_name) 
        references drb_data_mapper_db.admin.customer (customer_name) not enforced,
    constraint fkey_2 foreign key (target_collection_name, version) 
        references drb_data_mapper_db.admin.target_collection (target_collection_name, version) match partial not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* target entity - one target collection to many entities, can be analogous to "table" */
create or replace table drb_data_mapper_db.admin.target_entity (
    target_collection_name varchar,
    version varchar,
    target_entity_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version, target_entity_name) not enforced,
    constraint fkey_1 foreign key (target_collection_name, version) 
        references drb_data_mapper_db.admin.target_collection (target_collection_name, version) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* target entity attribute - one entity to many attributes, can be analogous to "column" */
create or replace table drb_data_mapper_db.admin.target_entity_attribute (
    target_collection_name varchar,
    version varchar,
    target_entity_name varchar,
    target_entity_attribute_name varchar,
    target_attribute_properties object,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (target_collection_name, version, target_entity_name, target_entity_attribute_name) not enforced,
    constraint fkey_1 foreign key (target_collection_name, version, target_entity_name)
        references drb_data_mapper_db.admin.target_entity (target_collection_name, version, target_entity_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* admin sample data */
insert into drb_data_mapper_db.admin.customer(customer_name, customer_snowflake_organization_name) values ('CustomerABC','CustomerABCOrgName');
insert into drb_data_mapper_db.admin.target_collection(target_collection_name, version) values ('MyFirstTargetCollection','v1');
insert into drb_data_mapper_db.admin.target_collection(target_collection_name, version) values ('TheNextTargetCollection','v1');
insert into drb_data_mapper_db.admin.subscription(customer_name, target_collection_name, version, expiration_date) values ('CustomerABC','MyFirstTargetCollection','v1','2024-01-01');
insert into drb_data_mapper_db.admin.subscription(customer_name, target_collection_name, version, expiration_date) values ('CustomerABC','TheNextTargetCollection','v1','2024-01-01');
insert into drb_data_mapper_db.admin.target_entity(target_collection_name, version, target_entity_name) values ('MyFirstTargetCollection','v1','FirstTable');
insert into drb_data_mapper_db.admin.target_entity(target_collection_name, version, target_entity_name) values ('MyFirstTargetCollection','v1','SecondTable');
insert into drb_data_mapper_db.admin.target_entity(target_collection_name, version, target_entity_name) values ('TheNextTargetCollection','v1','NextTable');
insert into drb_data_mapper_db.admin.target_entity_attribute(target_collection_name, version, target_entity_name, target_entity_attribute_name, target_attribute_properties)
select
        'MyFirstTargetCollection'
    ,   'v1'
    ,   'FirstTable'
    ,   'ExampleVarcharColumn'
    ,   object_construct(
            'data_type','VARCHAR',
            'is_nullable',false,
            'is_required',true,
            'description','The first column'
        )
;
insert into drb_data_mapper_db.admin.target_entity_attribute(target_collection_name, version, target_entity_name, target_entity_attribute_name, target_attribute_properties)
select
        'MyFirstTargetCollection'
    ,   'v1'
    ,   'FirstTable'
    ,   'ExampleBoolColumn'
    ,   object_construct(
            'data_type','boolean',
            'is_nullable',false,
            'is_required',false,
            'description','The second column'
        )
;
insert into drb_data_mapper_db.admin.target_entity_attribute(target_collection_name, version, target_entity_name, target_entity_attribute_name, target_attribute_properties)
select
        'MyFirstTargetCollection'
    ,   'v1'
    ,   'SecondTable'
    ,   'ExampleVarcharColumn'
    ,   object_construct(
            'data_type','VARCHAR',
            'is_nullable',false,
            'is_required',true,
            'description','The first column'
        )
;
insert into drb_data_mapper_db.admin.target_entity_attribute(target_collection_name, version, target_entity_name, target_entity_attribute_name, target_attribute_properties)
select
        'TheNextTargetCollection'
    ,   'v1'
    ,   'NextTable'
    ,   'ExampleDateColumn'
    ,   object_construct(
            'data_type','DATE',
            'is_nullable',false,
            'is_required',false,
            'description','The first column'
        )
;


/* we now have an app and can tell the customer what target collections are relevant, which entities are required, and the attributes required for each entity */
/* now, we need to build out what the customer interacts with to map their data to the target collection */

/* modeling holds all tables associated with defining a data model */
create or replace schema drb_data_mapper_db.modeling comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* source collection - group of entities that define a source data model */
/* target_collection_name refers to the intended target collection for mapping */
/* target_entity_name refers to the intended target entity for mapping */
/* refresh frequency will be used the the orchestration feature later */
create or replace table drb_data_mapper_db.modeling.source_collection (
    source_collection_name varchar,
    target_collection_name varchar,
    version varchar,
    target_entity_name varchar,
    custom_sql varchar,
    use_custom_sql boolean,
    generated_mapping_table varchar,
    refresh_frequency varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (source_collection_name) not enforced,
    constraint fkey_1 foreign key (target_collection_name, version, target_entity_name) 
        references drb_data_mapper_db.admin.target_entity (target_collection_name, version, target_entity_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* source entity with fully qualified origin - can be analogous to "source table" */
/* note - only one source entity can be a base entity within a collection - all other entities are aggregated - it determines the cardinality of the collection  */
/* is_base_entity indicates that a source entity is the source entity to be joined on by other entities */
/* join_from_source_entity_name specifies what existing source entity to join to */
/* join_type should be typical SQL types - inner, left, right, etc. */
/* join from is the left and join to is the right side of a join condition */
create or replace table drb_data_mapper_db.modeling.source_entity (
    source_collection_name varchar,
    source_entity_name varchar,
    entity_fully_qualified_source varchar,
    is_base_entity boolean,
    join_from_source_entity_name varchar,
    join_type varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (source_collection_name, source_entity_name) not enforced,
    constraint fkey_1 foreign key (source_collection_name) 
        references drb_data_mapper_db.modeling.source_collection (source_collection_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* multiple join condition support, e.g. a.col1=b.col1 and a.col2=b.col2 */
/*
join_from_entity_attribute_name - an attribute/column for the "from" side, "left" side of the join condition
operator - =, <, >, <=, >=
join_to_entity_attribute_name - an attribute/column for the "to" side, "right" side fo the join condition
*/
/* note - multiple join conditions are combined with an AND */
create or replace table drb_data_mapper_db.modeling.source_entity_join_condition (
    source_collection_name varchar,
    source_entity_name varchar,
    join_from_source_entity_name varchar,
    join_from_entity_attribute_name varchar,
    operator varchar,
    join_to_entity_attribute_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (source_collection_name, source_entity_name, join_from_source_entity_name) not enforced,
    constraint fkey_1 foreign key (source_collection_name, source_entity_name) 
        references drb_data_mapper_db.modeling.source_entity (source_collection_name, source_entity_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* source entity attribute - one entity to many attributes, can be analogous to "column" */
/* note - aggregation will only be applied to non-base entity attributes */
/* include_in_entity determines if a column is surfaced in the final table or not - allows for hiding irrelevant cols */
/* derived_expression should be a scalar expression using attributes of the same entity, and null if not a derived col */
create or replace table drb_data_mapper_db.modeling.source_entity_attribute (
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
        references drb_data_mapper_db.modeling.source_entity (source_collection_name, source_entity_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* multiple filter condition support, e.g. col1=2000 and col2='ABC' */
/*
left_filter_expression - an expression for the "left" side of the filter condition
operator - =, <, >, <=, >=
right_filter_expression - an expression for the "right" side of the filter condition
*/
/* note - multiple filter conditions are combined with an AND */
/* applied AFTER the joins are performed */
create or replace table drb_data_mapper_db.modeling.source_collection_filter_condition (
    source_collection_name varchar,
    left_filter_expression varchar,
    operator varchar,
    right_filter_expression varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (source_collection_name, left_filter_expression) not enforced,
    constraint fkey_1 foreign key (source_collection_name) 
        references drb_data_mapper_db.modeling.source_collection (source_collection_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* python procedure to initially populate entity attributes */
create or replace procedure drb_data_mapper_db.modeling.generate_attributes(source_collection_name varchar, source_entity_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_attributes'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def generate_attributes(session, source_collection_name, source_entity_name):
    try:
        
        # entity dataframe
        entity_dataframe = session.table('drb_data_mapper_db.modeling.source_entity') \
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

        target_df = session.table('DRB_DATA_MAPPER_DB.MODELING.SOURCE_ENTITY_ATTRIBUTE')

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
$$
;

/* for testing customer SQL option */
--update drb_data_mapper_db.modeling.source_collection
--set custom_sql = $$select BASE_TABLE_VARCHAR_COL from DRB_CUSTOMER_SAMPLE_DB.SAMPLE_DATA.SAMPLE_BASE_TABLE$$, use_custom_sql=TRUE;


/* modeler sample data, based on customer sample db */
insert into drb_data_mapper_db.modeling.source_collection(source_collection_name, target_collection_name, version, target_entity_name, refresh_frequency) values ('MySourceCollection', 'MyFirstTargetCollection', 'v1', 'FirstTable', null);
insert into drb_data_mapper_db.modeling.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
select
        'MySourceCollection'
    ,   'SampleBaseTable'
    ,   'drb_customer_sample_db.sample_data.sample_base_table'
    ,   TRUE
    ,   null
    ,   null
;

insert into drb_data_mapper_db.modeling.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
select
        'MySourceCollection'
    ,   'SampleAggTable'
    ,   'drb_customer_sample_db.sample_data.sample_agg_table'
    ,   FALSE
    ,   'SampleBaseTable'
    ,   'LEFT'
;
insert into drb_data_mapper_db.modeling.source_entity_join_condition(source_collection_name, source_entity_name, join_from_source_entity_name, join_from_entity_attribute_name, operator, join_to_entity_attribute_name)
select
        'MySourceCollection'
    ,   'SampleAggTable'
    ,   'SampleBaseTable'
    ,   'base_table_record_col'
    ,   '='
    ,   'base_table_record_col'
;

/* join to same table again */
insert into drb_data_mapper_db.modeling.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
select
        'MySourceCollection'
    ,   'SampleAggTable2'
    ,   'drb_customer_sample_db.sample_data.sample_agg_table'
    ,   FALSE
    ,   'SampleBaseTable'
    ,   'LEFT'
;
insert into drb_data_mapper_db.modeling.source_entity_join_condition(source_collection_name, source_entity_name, join_from_source_entity_name, join_from_entity_attribute_name, operator, join_to_entity_attribute_name)
select
        'MySourceCollection'
    ,   'SampleAggTable2'
    ,   'SampleBaseTable'
    ,   'base_table_record_col'
    ,   '='
    ,   'base_table_record_col'
;

/* grandchild table */
insert into drb_data_mapper_db.modeling.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
select
        'MySourceCollection'
    ,   'SampleAggGrandchildTable'
    ,   'drb_customer_sample_db.sample_data.sample_agg_table'
    ,   FALSE
    ,   'SampleAggTable'
    ,   'LEFT'
;
insert into drb_data_mapper_db.modeling.source_entity_join_condition(source_collection_name, source_entity_name, join_from_source_entity_name, join_from_entity_attribute_name, operator, join_to_entity_attribute_name)
select
        'MySourceCollection'
    ,   'SampleAggGrandchildTable'
    ,   'SampleAggTable'
    ,   'agg_table_record_col'
    ,   '='
    ,   'agg_table_record_col'
;


/* generate attributes for entity sample data */
call drb_data_mapper_db.modeling.generate_attributes('MySourceCollection', 'SampleBaseTable');
call drb_data_mapper_db.modeling.generate_attributes('MySourceCollection', 'SampleAggTable');
call drb_data_mapper_db.modeling.generate_attributes('MySourceCollection', 'SampleAggTable2');
call drb_data_mapper_db.modeling.generate_attributes('MySourceCollection', 'SampleAggGrandchildTable');

/* insert derived column */
insert into drb_data_mapper_db.modeling.source_entity_attribute (source_collection_name, source_entity_name, source_entity_attribute_name, source_attribute_properties, include_in_entity, derived_expression, aggregation_function)
select
        'MySourceCollection'
    ,   'SampleBaseTable'
    ,   'base_table_varchar_col_left_4'
    ,   parse_json('{"byte_length":16777216,"data_type":"TEXT","is_nullable":true,"length":16777216}')
    ,   TRUE
    ,   'left(base_table_varchar_col, 4)'
    ,   NULL
;

/* add example filter conditions */
insert into drb_data_mapper_db.modeling.source_collection_filter_condition(source_collection_name, left_filter_expression, operator, right_filter_expression)
select
        'MySourceCollection'
    ,   '1'
    ,   '='
    ,   'left(1,1)'
;

insert into drb_data_mapper_db.modeling.source_collection_filter_condition(source_collection_name, left_filter_expression, operator, right_filter_expression)
select
        'MySourceCollection'
    ,   '2'
    ,   '='
    ,   'right(2,1)'
;

/* mapping holds the dynamic tables used prior to mapping to target entites */
create or replace schema drb_data_mapper_db.mapping comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* python procedure to generate dynamic sql */
create or replace procedure drb_data_mapper_db.modeling.generate_collection_model(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_collection_model'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched, lit, \
any_value as any_value_, avg as avg_, corr as corr_, count as count_, covar_pop as covar_pop_, covar_samp as covar_samp_, \
listagg as listagg_, max as max_, median as median_, min as min_, mode as mode_, percentile_cont as percentile_cont_, \
stddev as stddev_, stddev_pop as stddev_pop_, stddev_samp as stddev_samp_, sum as sum_, \
var_pop as var_pop_, var_samp as var_samp_, variance as variance_
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

# function to enrich dynamic_df
def enrich_dataframe(session, source_entity_df, base_entity_name, dynamic_df):
    source_collection_name = source_entity_df \
        .select(col("SOURCE_COLLECTION_NAME")).collect()[0][0]
    
    # construct group by
    base_col_list = dynamic_df.columns

    group_by_string = ""

    for item in base_col_list:
        if group_by_string == "":
            group_by_string = 'col("{item}")'.format(item=item)
        else:
            group_by_string += ', col("{item}")'.format(item=item)

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
    from drb_data_mapper_db.modeling.source_entity se
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
    from drb_data_mapper_db.modeling.source_entity se
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
        join_conditions_list = session.table("drb_data_mapper_db.modeling.source_entity_join_condition") \
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
    group_by_exp_statement = "dynamic_df.groupBy({group_by_string})".format(group_by_string=group_by_string)
    dynamic_df = eval(group_by_exp_statement)

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
        # get attribute
        aggregation_function = session.table('drb_data_mapper_db.modeling.source_entity_attribute') \
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
    all_attributes_df = session.table('drb_data_mapper_db.modeling.source_entity_attribute') \
        .filter( \
            (col("SOURCE_COLLECTION_NAME") == source_collection_name) &  \
            (col("SOURCE_ENTITY_NAME") == source_entity_name))

    # if no attributes, generate them
    if all_attributes_df.count() == 0:
        procedure_call_statement = "call drb_data_mapper_db.modeling.generate_attributes('{source_collection_name}', '{source_entity_name}')".format(source_collection_name=source_collection_name, source_entity_name = source_entity_name)
        session.sql(procedure_call_statement).collect()

        all_attributes_df = session.table('drb_data_mapper_db.modeling.source_entity_attribute') \
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
    filter_conditions_df = session.table('drb_data_mapper_db.modeling.source_collection_filter_condition') \
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
    source_entity_df = session.table('drb_data_mapper_db.modeling.source_entity') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name)

    # check if custom sql
    use_custom_sql = session.table('drb_data_mapper_db.modeling.source_collection') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name) \
        .select(col("USE_CUSTOM_SQL")) \
        .collect()[0][0]

    if use_custom_sql:
        custom_sql_string = session.table('drb_data_mapper_db.modeling.source_collection') \
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
    generated_mapping_table = "mapping." + source_collection_name.replace(" ","_")
 
    # save table name to source collection
    session.sql("""
        update modeling.source_collection
        set generated_mapping_table = '{generated_mapping_table}'
        where source_collection_name = '{source_collection_name}'
    """.format(generated_mapping_table=generated_mapping_table, source_collection_name=source_collection_name)).collect()

    # create a dynamic table in mapping schema
    dynamic_df.create_or_replace_dynamic_table(name=generated_mapping_table, warehouse="drb_data_mapper_wh", lag="DOWNSTREAM")

    return "Collection dynamic table created"
$$
;

call drb_data_mapper_db.modeling.generate_collection_model('MySourceCollection');


/* python procedure get columns from the source collection dynamic tables to map-from, used by Streamlit to grab columns */
create or replace procedure drb_data_mapper_db.mapping.get_mapfrom_columns(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='get_mapfrom_columns'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def get_mapfrom_columns(session, source_collection_name):
    # source collection
    source_collection_df = session.table('drb_data_mapper_db.modeling.source_collection') \
        .filter(col('source_collection_name') == source_collection_name)

    # generated table
    generated_mapping_table = source_collection_df.select(col('generated_mapping_table')).collect()[0][0]

    # table df
    generated_mapping_table_df = session.table(generated_mapping_table)

    return generated_mapping_table_df.columns
$$
;

/* python procedure get attributes for target entities to map-to, used by Streamlit to grab columns */
create or replace procedure drb_data_mapper_db.mapping.get_mapto_columns(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='get_mapto_columns'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def get_mapto_columns(session, source_collection_name):
    # source collection
    source_collection_df = session.table('drb_data_mapper_db.modeling.source_collection') \
        .filter(col('source_collection_name') == source_collection_name)

    # target collection
    target_collection_name = source_collection_df.select(col('target_collection_name')).collect()[0][0]

    # custom SQL

    # version
    version = source_collection_df.select(col('version')).collect()[0][0]

    # target entity
    target_entity_name = source_collection_df.select(col('target_entity_name')).collect()[0][0]

    # target entity attribute df
    target_entity_attribute_df = session.table('drb_data_mapper_db.admin.target_entity_attribute') \
        .filter( \
            (col('target_collection_name') == target_collection_name) & \
            (col('version') == version) & \
            (col('target_entity_name') == target_entity_name))

    return target_entity_attribute_df.columns
$$
;

/* demonstration, Streamlit will call these when facilitating mapping */
call drb_data_mapper_db.mapping.get_mapfrom_columns('MySourceCollection');
call drb_data_mapper_db.mapping.get_mapto_columns('MySourceCollection');


/* defines the column mappings between source collection and target entity, populated by streamlit  */
create or replace table drb_data_mapper_db.mapping.source_to_target_mapping (
    source_collection_name varchar,
    generated_mapping_table_column_name varchar,
    target_attribute_name varchar,
    last_updated_timestamp timestamp default current_timestamp(),
    constraint pkey primary key (source_collection_name) not enforced,
    constraint fkey_1 foreign key (source_collection_name) 
        references drb_data_mapper_db.modeling.source_collection (source_collection_name) not enforced
) comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';


/* example data */
insert into drb_data_mapper_db.mapping.source_to_target_mapping (source_collection_name, generated_mapping_table_column_name, target_attribute_name)
select
        'MySourceCollection'
    ,   'BASE_TABLE_VARCHAR_COL'
    ,   'ExampleVarcharColumn'
;


/* database that will be created by the app - must be separate from app to be included in a share */
/* dropping listing and share if exists, as database 'replace' will fail if the db is included in the share*/
alter listing if exists drb_data_mapper_app_share set state = unpublished;
drop listing if exists drb_data_mapper_app_share;
drop share if exists drb_data_mapper_share;
create or replace database drb_data_mapper_share_db comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
/* schema for the share */
create or replace schema drb_data_mapper_share_db.shared comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
/* schema for the configuration */
create or replace schema drb_data_mapper_share_db.configuration comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
/* schema for utilities for managing the share */
create or replace schema drb_data_mapper_share_db.utility comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}';
drop schema if exists drb_data_mapper_share_db.public;

/* create tables to provide configuration back */
create or replace secure view drb_data_mapper_share_db.configuration.source_collection_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.modeling.source_collection;

create or replace secure view drb_data_mapper_share_db.configuration.source_collection_filter_condition_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.modeling.source_collection_filter_condition;

create or replace secure view drb_data_mapper_share_db.configuration.source_entity_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.modeling.source_entity;

create or replace secure view drb_data_mapper_share_db.configuration.source_entity_attribute_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.modeling.source_entity_attribute;

create or replace secure view drb_data_mapper_share_db.configuration.source_entity_join_condition_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.modeling.source_entity_join_condition;

create or replace secure view drb_data_mapper_share_db.configuration.source_to_target_mapping_vw comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}' as
select * from drb_data_mapper_db.mapping.source_to_target_mapping;

/* python procedure to generate the secure view in the share database */
create or replace procedure drb_data_mapper_db.mapping.generate_view(source_collection_name varchar)
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='generate_views'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def generate_views(session, source_collection_name):
    # source collection
    source_collection_df = session.table('drb_data_mapper_db.modeling.source_collection') \
        .filter(col('source_collection_name') == source_collection_name)

    # generated table
    generated_mapping_table = 'drb_data_mapper_db.' + source_collection_df.select(col('generated_mapping_table')).collect()[0][0]

    dynamic_df = session.table(generated_mapping_table)

    # source to target mapping
    source_to_target_mapping_df = session.table('drb_data_mapper_db.mapping.source_to_target_mapping') \
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

    view_name = '"DRB_DATA_MAPPER_SHARE_DB"."SHARED"."' + target_collection_name + '__' + version + '__' + target_entity_name + '"'
    
    dynamic_df.create_or_replace_view(view_name)

    session.sql('alter view ' + view_name + ' set secure').collect()

    return 'Secure view generated successfully'
$$
;

call drb_data_mapper_db.mapping.generate_view('MySourceCollection');

create or replace procedure drb_data_mapper_share_db.utility.share_views()
returns string
language python
runtime_version=3.8
packages=('snowflake-snowpark-python')
handler='share_views'
comment='{"origin":"sf_ps_wls","name":"drb","version":{"major":1, "minor":0},"attributes":{"component":"drb"}}'
as
$$
from snowflake.snowpark.functions import call_udf, col, current_timestamp, when_matched, when_not_matched
from snowflake.snowpark.types import IntegerType, StringType, StructType, StructField

def share_views(session):
    # get list of existing views
    session.sql("show views in drb_data_mapper_share_db.shared").collect()
    view_df = session.sql('select "name" from table(result_scan(last_query_id()))')

    for view_row in view_df.to_local_iterator():
        share_sql_string = 'grant select on view drb_data_mapper_share_db.shared."{view_name}" to share drb_data_mapper_share'.format(view_name=view_row["name"])
        session.sql(share_sql_string).collect()

    return "Views shared successfully"
$$
;

/* sharing testing - will be performed by the consumer through Streamlit */
/* Note - the Listing API is in private preview and only for non-production use cases */
/* The native app can help users walk through using the GUI, which is GA, and the Listing API added later */
create or replace share drb_data_mapper_share;
grant usage on database drb_data_mapper_share_db to share drb_data_mapper_share;
grant reference_usage on database drb_data_mapper_db to share drb_data_mapper_share;
grant usage on schema drb_data_mapper_share_db.configuration to share drb_data_mapper_share;
grant usage on schema drb_data_mapper_share_db.shared to share drb_data_mapper_share;

grant select on view drb_data_mapper_share_db.configuration.source_collection_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_collection_filter_condition_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_join_condition_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_entity_attribute_vw to share drb_data_mapper_share;
grant select on view drb_data_mapper_share_db.configuration.source_to_target_mapping_vw to share drb_data_mapper_share;

call drb_data_mapper_share_db.utility.share_views();



/* FOR NON-PRODUCTION - feel free to use the Listing API to automate during development, but the Listing API is still in preview */
/* 
create listing drb_data_mapper_app_share in data exchange SNOWFLAKE_DATA_MARKETPLACE
for share drb_data_mapper_share as
$$
 title: "Data Mapping App Share"
 description: "The shareback from the Data Mapper App"
 terms_of_service:
   type: "OFFLINE"
 auto_fulfillment:
   refresh_schedule: "10 MINUTE"
   refresh_type: "FULL_DATABASE"
 targets:
   accounts: ["ORG_NAME.ACCOUNT_NAME"]
$$;

alter listing  drb_data_mapper_app_share set state = published;

show listings like 'drb_data_mapper_app_share' in data exchange snowflake_data_marketplace;

select 'Find LISTING_GLOBAL_NAME in consumer_setup.sql and replace with ' || "global_name" || ' and then run code in script file consumer_setup.sql on the consumer account' as DO_THIS_NEXT from table(result_scan(last_query_id()));
*/