from appPages.page import st
from appPages.overview import OverviewPage
from appPages.collection_list import CollectionList
from appPages.collection_joining import CollectionJoining
from appPages.collection_mapping import CollectionMapping
from appPages.target_administration import TargetAdministration
from appPages.entity_configure import EntityConfiguration
from snowflake.snowpark.context import get_active_session


# Check snowflake connection type


def set_session():
    try:
        import snowflake.permissions as permissions

        session = get_active_session()

        st.session_state["streamlit_mode"] = "NativeApp"
    except:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account_1")

            st.session_state["streamlit_mode"] = "OSS"

    return session


# Set user organization name
# if "organization_name" not in st.session_state:
#     st.session_state.organization_name = session.sql(
#         "SELECT CURRENT_ORGANIZATION_NAME()"
#     ).collect()[0]


# Set starting page
if "page" not in st.session_state:
    st.session_state.page = "overview"

    #Set table database location session variables
    #This should happen on first load only
    st.session_state.native_database_name = "dmm_model_mapper_share_db"

pages = [
    OverviewPage(),
    CollectionList(),
    CollectionJoining(),
    CollectionMapping(),
    TargetAdministration(),
    EntityConfiguration()
]


def main():
    st.set_page_config(layout="wide")
    if 'session' not in st.session_state:
        st.session_state.session = set_session()
    for page in pages:
        if page.name == st.session_state.page:
            st.session_state.layout = "wide"
            page.print_sidebar()
            page.print_page()


main()
