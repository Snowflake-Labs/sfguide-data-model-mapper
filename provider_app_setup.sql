/*************************************************************************************************************
Script:             Provider App Setup
Create Date:        2023-08-09
Author:             B. Klein
Description:        Sets up app and app package for Dynamic Data Model Mapper
Copyright © 2023 Snowflake Inc. All rights reserved
*************************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                              Comments
------------------- -------------------                 --------------------------------------------
2023-10-30          B. Klein                            Initial Creation
2024-01-18          B. Klein                            Renaming to Dynamic Data Model Mapper
*************************************************************************************************************/

/* set role and warehouse */
use role dmm_data_mapper_role;
call system$wait(5);
use warehouse dmm_data_mapper_wh;

/* create application package version using code files */
ALTER APPLICATION PACKAGE dmm_model_mapper_app_package
    ADD VERSION Version1 USING '@dmm_data_mapper_code.content.files_v1_0';

ALTER APPLICATION PACKAGE dmm_model_mapper_app_package
  SET DEFAULT RELEASE DIRECTIVE
  VERSION = Version1
  PATCH = 0;    

/* test creating the app locally */
CREATE APPLICATION dmm_model_mapper_app FROM APPLICATION PACKAGE dmm_model_mapper_app_package;

/* requires that the app was installed as dmm_model_mapper_app in the previous step */
grant application role dmm_model_mapper_app.dmm_consumer_app_role to role dmm_data_mapper_role;

/* nonprod dem application role */
grant application role dmm_model_mapper_app.dmm_demo_app_role to role dmm_data_mapper_role;

/* grant required app permissions */
grant create database on account to application dmm_model_mapper_app;
grant execute task on account to application dmm_model_mapper_app;
grant create warehouse on account to application dmm_model_mapper_app;

/* initializes the app by creating the outside database - required for operation */
call dmm_model_mapper_app.modeling.initialize_application();


/* grant app access to demo data */
grant usage on database dmm_customer_sample_db to application dmm_model_mapper_app;
grant usage on schema dmm_customer_sample_db.sample_data to application dmm_model_mapper_app;
grant select on all tables in schema dmm_customer_sample_db.sample_data to application dmm_model_mapper_app;


/* only for nonprod demo use, uncomment and run to deploy mock config */
--call dmm_model_mapper_app.modeling.deploy_demo();


/* grant permissions on app's share db to dmm_data_mapper_role */
use role accountadmin;
call system$wait(5);

/* Make required grants */
grant all privileges on database dmm_model_mapper_share_db to role dmm_data_mapper_role with grant option;
grant all privileges on schema dmm_model_mapper_share_db.configuration to role dmm_data_mapper_role with grant option;
grant all privileges on schema dmm_model_mapper_share_db.modeled to role dmm_data_mapper_role with grant option;
grant all privileges on schema dmm_model_mapper_share_db.mapped to role dmm_data_mapper_role with grant option;
grant all privileges on schema dmm_model_mapper_share_db.utility to role dmm_data_mapper_role with grant option;

grant all privileges on all tables in schema dmm_model_mapper_share_db.configuration to role dmm_data_mapper_role;
grant all privileges on future tables in schema dmm_model_mapper_share_db.configuration to role dmm_data_mapper_role;
grant all privileges on all tables in schema dmm_model_mapper_share_db.modeled to role dmm_data_mapper_role;
grant all privileges on future tables in schema dmm_model_mapper_share_db.modeled to role dmm_data_mapper_role;
grant all privileges on all views in schema dmm_model_mapper_share_db.mapped to role dmm_data_mapper_role;
grant all privileges on future views in schema dmm_model_mapper_share_db.mapped to role dmm_data_mapper_role;
grant all privileges on all functions in schema dmm_model_mapper_share_db.utility to role dmm_data_mapper_role;
grant all privileges on all procedures in schema dmm_model_mapper_share_db.utility to role dmm_data_mapper_role;
grant ownership on procedure dmm_model_mapper_share_db.utility.share_views() to role dmm_data_mapper_role revoke current grants;

use role dmm_data_mapper_role;
call system$wait(5);



/* sharing testing - will be performed by the consumer through Streamlit */
/* note - the Listing API is in private preview and only for non-production use cases */
/* the native app can help users walk through using the GUI, which is GA, and the Listing API added later */
drop share if exists dmm_data_mapper_share;
create or replace share dmm_data_mapper_share;
grant usage on database dmm_model_mapper_share_db to share dmm_data_mapper_share;
grant usage on schema dmm_model_mapper_share_db.configuration to share dmm_data_mapper_share;
grant usage on schema dmm_model_mapper_share_db.mapped to share dmm_data_mapper_share;


grant select on table dmm_model_mapper_share_db.configuration.source_collection to share dmm_data_mapper_share;
grant select on table dmm_model_mapper_share_db.configuration.source_collection_filter_condition to share dmm_data_mapper_share;
grant select on table dmm_model_mapper_share_db.configuration.source_entity to share dmm_data_mapper_share;
grant select on table dmm_model_mapper_share_db.configuration.source_entity_join_condition to share dmm_data_mapper_share;
grant select on table dmm_model_mapper_share_db.configuration.source_entity_attribute to share dmm_data_mapper_share;
grant select on table dmm_model_mapper_share_db.configuration.source_to_target_mapping to share dmm_data_mapper_share;


call dmm_model_mapper_share_db.utility.share_views();

 
/* FOR NON-PRODUCTION - feel free to use the Listing API to automate during development, but the Listing API is still in preview */
/* 
create listing dmm_model_mapper_app_share in data exchange SNOWFLAKE_DATA_MARKETPLACE
for share dmm_data_mapper_share as
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

alter listing  dmm_model_mapper_app_share set state = published;

show listings like 'dmm_model_mapper_app_share' in data exchange snowflake_data_marketplace;

select 'Find LISTING_GLOBAL_NAME in consumer_setup.sql and replace with ' || "global_name" || ' and then run code in script file consumer_setup.sql on the consumer account' as DO_THIS_NEXT from table(result_scan(last_query_id()));
*/


select 'App installed!' as STATUS;