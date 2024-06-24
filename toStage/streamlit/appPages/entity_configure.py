import streamlit as st
import pandas as pd
from appPages.page import BasePage, col, set_page


def get_collection_name():
    session = st.session_state.session
    if st.session_state["streamlit_mode"] == "NativeApp":
        collection_name = (
            session.table(st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION")
            .select(col("SOURCE_COLLECTION_NAME"))
            .filter(col("TARGET_COLLECTION_NAME") == st.session_state.selected_target_collection)
            .filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name).distinct()
        )
    else:
        collection_name = (
            session.table("MODELING.SOURCE_COLLECTION")
            .select(col("SOURCE_COLLECTION_NAME"))
            .filter(col("TARGET_COLLECTION_NAME") == st.session_state.selected_target_collection)
            .filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name).distinct()
        )

    collection_name_pd = collection_name.to_pandas()

    if len(collection_name_pd) > 0:
        st.session_state.collection_name = collection_name_pd.loc[0, "SOURCE_COLLECTION_NAME"]
        st.session_state.delete_enable = False
    else:
        st.session_state.delete_enable = True
        st.session_state.collection_name = "N/A"


def drop_view_and_dynamic():
    session = st.session_state.session

    # get current database (app name)

    if st.session_state["streamlit_mode"] == "NativeApp":
        application_name = session.sql("""select current_database()""").collect()[0][0]

        source_collection_pd = (
            session.table("dmm_model_mapper_share_db.configuration.source_collection")
            .filter(col("source_collection_name") == st.session_state.collection_name)).distinct().to_pandas()

        source_collection_pd.reset_index(inplace=True, drop=True)

        if len(source_collection_pd) > 0:
            application_name = session.sql("""select current_database()""").collect()[0][0]
            # target collection
            target_collection_name = source_collection_pd.loc[0, "TARGET_COLLECTION_NAME"]

            # version
            version = source_collection_pd.loc[0, "VERSION"]

            # target entity
            target_entity_name = source_collection_pd.loc[0, "TARGET_ENTITY_NAME"]

            view_name = 'DMM_MODEL_MAPPER_SHARE_DB.MAPPED.' + target_collection_name + '__' + version + '__' + target_entity_name

            # Drop View Name
            drop_view_sql = f"""DROP VIEW IF EXISTS {view_name} """
            # st.write("drop view script: " + drop_view_sql)
            try:
                session.sql(drop_view_sql).collect()
            except Exception as e:
                st.info(e)
            # Drop Dynamic Table

            dynamic_table_name = 'DMM_MODEL_MAPPER_SHARE_DB.MODELED.' + st.session_state.collection_name
            drop_dynamic_sql = f"""DROP DYNAMIC TABLE IF EXISTS {dynamic_table_name} """
            # st.write("drop dynamic script: " + drop_dynamic_sql)
            try:
                session.sql(drop_dynamic_sql).collect()
            except Exception as e:
                st.info(e)

        else:
            st.write("No Views to delete")


def remove_definitions():
    session = st.session_state.session

    # Drop views and dynamic table tied to collection
    drop_view_and_dynamic()

    if st.session_state["streamlit_mode"] == "NativeApp":
        collection_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        update_entity_sql = ("DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY  \
                                     WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' ")
        condition_delete_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        filter_condition_delete_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_join_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_attribute_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_ATTRIBUTE WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_target_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_TO_TARGET_MAPPING WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_filter_condition_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_entity_condition_sql = "DELETE FROM " + st.session_state.native_database_name + ".configuration.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
    else:
        collection_sql = "DELETE FROM MODELING.SOURCE_COLLECTION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        update_entity_sql = ("DELETE FROM MODELING.SOURCE_ENTITY  \
                                     WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "' ")
        condition_delete_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        filter_condition_delete_sql = "DELETE FROM MODELING.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_join_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        entity_attribute_sql = "DELETE FROM MODELING.SOURCE_ENTITY_ATTRIBUTE WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_target_sql = "DELETE FROM MODELING.SOURCE_TO_TARGET_MAPPING WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_filter_condition_sql = "DELETE FROM MODELING.SOURCE_COLLECTION_FILTER_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"
        source_entity_condition_sql = "DELETE FROM MODELING.SOURCE_ENTITY_JOIN_CONDITION WHERE SOURCE_COLLECTION_NAME = '" + st.session_state.collection_name + "'"

    # Add Try Catches Error Handling
    try:
        collection_run = session.sql(collection_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        update_run = session.sql(update_entity_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        condition_run = session.sql(condition_delete_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        filter_condition_run = session.sql(filter_condition_delete_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        entity_run = session.sql(entity_join_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        entity_attribute_run = session.sql(entity_attribute_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_target_run = session.sql(source_target_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_filter_condition_run = session.sql(source_filter_condition_sql).collect()
    except Exception as e:
        st.info(e)
    try:
        source_entity_condition_run = session.sql(source_entity_condition_sql).collect()
    except Exception as e:
        st.info(e)

    st.success("Collection data successfully deleted")


class EntityConfiguration(BasePage):
    def __init__(self):
        self.name = "entity_config"

    def print_page(self):
        session = st.session_state.session

        # View Text
        # sql_text = """ALTER VIEW {view_name} set comment= '{"origin":"sf_sit","name":"dmm","version":{"major":1, "minor":0},"attributes":
        #     {"component":"dmm"}}'"""
        # st.write(sql_text)

        get_collection_name()

        tab1, tab2 = st.tabs(["Delete Colleciton", "Dynamic Refresh Schedule"])

        with tab1:
            if 'collection_name' in st.session_state:
                st.header("Collection data for: " + '**' + st.session_state.collection_name + '**')

                if st.session_state.collection_name == "N/A":
                    st.write("No collection data exists for deletion")
                else:
                    st.write("(This will delete all metadata,views,dynamic tables tied to the collection)")
                    st.button(
                        "DELETE ALL",
                        key="delete",
                        type="primary",
                        disabled=st.session_state.delete_enable,
                        on_click=remove_definitions,
                    )
        with tab2:
            col1, col2, col3 = st.columns((6, .25, 1))
            with col1:
                st.write("")

                st.slider("Dynamic Table Refresh (in Minutes):", 0, 500, 60)
            with col3:
                st.header("#")
                st.header("#")
                st.button(
                    "Save",
                    key="save_dynamic",
                    help="This button planned to be enabled in future release",
                    disabled=True,
                    on_click=remove_definitions,
                )

    def print_sidebar(self):
        super().print_sidebar()
