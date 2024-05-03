-- Disregard, only for local testing

use role accountadmin;
use database drb_data_mapper_app;
use schema user_interface;

/* python procedure to generate dynamic sql */
create or replace procedure drb_data_mapper_share_db.configuration.generate_collection_model(source_collection_name varchar)
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
import re

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
    from drb_data_mapper_share_db.configuration.source_entity se
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
    from drb_data_mapper_share_db.configuration.source_entity se
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
        join_conditions_list = session.table("drb_data_mapper_share_db.configuration.source_entity_join_condition") \
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
        # remove source entity name from a detected duplicate
        col_name = re.sub(source_entity_name + "__", "", col_name, 1)

        # get attribute
        aggregation_function = session.table('drb_data_mapper_share_db.configuration.source_entity_attribute') \
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
    all_attributes_df = session.table('drb_data_mapper_share_db.configuration.source_entity_attribute') \
        .filter( \
            (col("SOURCE_COLLECTION_NAME") == source_collection_name) &  \
            (col("SOURCE_ENTITY_NAME") == source_entity_name))

    # if no attributes, generate them
    if all_attributes_df.count() == 0:
        procedure_call_statement = "call drb_data_mapper_app.modeling.generate_attributes('{source_collection_name}', '{source_entity_name}')".format(source_collection_name=source_collection_name, source_entity_name = source_entity_name)
        session.sql(procedure_call_statement).collect()

        all_attributes_df = session.table('drb_data_mapper_share_db.configuration.source_entity_attribute') \
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
    filter_conditions_df = session.table('drb_data_mapper_share_db.configuration.source_collection_filter_condition') \
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
    source_entity_df = session.table('drb_data_mapper_share_db.configuration.source_entity') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name)

    # check if custom sql
    use_custom_sql = session.table('drb_data_mapper_share_db.configuration.source_collection') \
        .filter(col("SOURCE_COLLECTION_NAME") == source_collection_name) \
        .select(col("USE_CUSTOM_SQL")) \
        .collect()[0][0]

    if use_custom_sql:
        custom_sql_string = session.table('drb_data_mapper_share_db.configuration.source_collection') \
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
    generated_mapping_table = "drb_data_mapper_share_db.modeled." + source_collection_name.replace(" ","_")
 
    # save table name to source collection
    session.sql("""
        update drb_data_mapper_share_db.configuration.source_collection
        set generated_mapping_table = '{generated_mapping_table}'
        where source_collection_name = '{source_collection_name}'
    """.format(generated_mapping_table=generated_mapping_table, source_collection_name=source_collection_name)).collect()

    # create a dynamic table in mapping schema
    dynamic_df.create_or_replace_dynamic_table(name=generated_mapping_table, warehouse="drb_data_mapper_wh", lag="DOWNSTREAM")

    #session.sql("""
    #    grant select on table {generated_mapping_table} to application role drb_consumer_app_role with grant option
    #""".format(generated_mapping_table=generated_mapping_table)).collect()

    return "Collection dynamic table created"
$$
;


    
-- being inserting demo data
insert overwrite into drb_data_mapper_share_db.configuration.source_collection(source_collection_name, target_collection_name, version, target_entity_name, refresh_frequency) values ('RetailSource', 'Retail', 'v1', 'STG_ITEM', null)
;

-- items & item locations
insert overwrite into drb_data_mapper_share_db.configuration.source_entity(source_collection_name, source_entity_name, entity_fully_qualified_source, is_base_entity, join_from_source_entity_name, join_type)
select
        'RetailSource'
    ,   'Items'
    ,   'drb_customer_sample_db.sample_data.items'
    ,   TRUE
    ,   null
    ,   null
union
select
        'RetailSource'
    ,   'ItemLocations'
    ,   'drb_customer_sample_db.sample_data.item_locations'
    ,   FALSE
    ,   'Items'
    ,   'LEFT'
;

-- insert derived column
insert overwrite into drb_data_mapper_share_db.configuration.source_entity_attribute (source_collection_name, source_entity_name, source_entity_attribute_name, source_attribute_properties, include_in_entity, derived_expression, aggregation_function)
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
;

-- generate attributes for entity sample data
call drb_data_mapper_app.modeling.generate_attributes('RetailSource', 'Items');
call drb_data_mapper_app.modeling.generate_attributes('RetailSource', 'ItemLocations');

select * from drb_data_mapper_share_db.configuration.source_entity_attribute;-- where SOURCE_ENTITY_ATTRIBUTE_NAME = 'INCRSTOCKOUTCOST';

-- join on derived cols
insert overwrite into drb_data_mapper_share_db.configuration.source_entity_join_condition(source_collection_name, source_entity_name, join_from_source_entity_name, join_from_entity_attribute_name, operator, join_to_entity_attribute_name)
select
        'RetailSource'
    ,   'ItemLocations'
    ,   'Items'
    ,   'ITEMID'
    ,   '='
    ,   'ItemLocations__ITEMID'
;


-- add example filter conditions
insert overwrite into drb_data_mapper_share_db.configuration.source_collection_filter_condition(source_collection_name, left_filter_expression, operator, right_filter_expression)
select
        'RetailSource'
    ,   '1'
    ,   '='
    ,   'left(1,1)'
;

-- generate collection
call drb_data_mapper_share_db.configuration.generate_collection_model('RetailSource');