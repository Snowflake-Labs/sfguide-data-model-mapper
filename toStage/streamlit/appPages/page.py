from abc import ABC, abstractmethod
import streamlit as st
import base64
import pandas as pd
import time
from PIL import Image
from snowflake.snowpark.functions import col, when_matched, when_not_matched, current_timestamp, parse_json

if 'session' in st.session_state:
    session = st.session_state.session


# Sets the page based on page name
def set_page(page: str):
    if "editor" in st.session_state:
        del st.session_state.editor

    st.session_state.page = page


class Page(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def print_page(self):
        pass

    @abstractmethod
    def print_sidebar(self):
        pass


class BasePage(Page):
    def __init__(self):
        pass

    def print_page(self):
        pass

    # Repeatable element: sidebar buttons that navigate to linked pages
    def print_sidebar(self):
        session = st.session_state.session

        if st.session_state.page == "collection_list":
            st.session_state.back_page = "overview"
            st.session_state.info = "Please select a Target Collection to Map"

        elif st.session_state.page == "collection_joining":
            st.session_state.back_page = "collection_list"
            st.session_state.info = "Please identify/join your sources"

        elif st.session_state.page == "collection_mapping":
            st.session_state.back_page = "collection_joining"
            st.session_state.info = "Please map your collection entities"

        if st.session_state.page == "overview":
            with st.sidebar:
                side_col1, side_col2 = st.columns((0.5, 2.5))
                with side_col1:
                    st.subheader("")
                    if st.session_state.streamlit_mode != "OSS":
                        image_name = "Images/snow.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=200)
                    else:
                        dataimage = Image.open("toStage/streamlit/Images/snow.png")
                        st.image(dataimage, width=200)

                with side_col2:
                    st.write('#')
                st.header("Dynamic Data Model Mapper")
                st.markdown("")
                st.markdown(
                    "This application facilitates mapping raw data to target data models and sharing back to app provider"
                )
                st.markdown("")
                st.markdown(
                    "To learn more about Snowflake's Native Apps Framework, feel free to check out the "
                    "[Documentation](https://docs.snowflake.com/en/developer-guide/native-apps/native-apps-about)"
                )
                st.markdown("")
                st.markdown("")
                st.markdown("")

        elif st.session_state.page in ("collection_joining", "collection_mapping"):
            with st.sidebar:
                side_col1, side_col2 = st.columns((0.5, 2.5))
                with side_col1:
                    if st.session_state.streamlit_mode != "OSS":
                        image_name = "Images/snow.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=200)
                    else:
                        dataimage = Image.open("toStage/streamlit/Images/snow.png")
                        st.image(dataimage, width=200)

                with side_col2:
                    st.write('#')
                st.header("Dynamic Data Model Mapper")
                help_check = st.checkbox('Enable verbose Instructions',
                                         key='help_check')
                st.subheader('')
                with side_col2:
                    st.write("#")

                st.write("Target Collection:")
                if 'collection_entity_name' in st.session_state:
                    st.write('**'+st.session_state.selected_target_collection+'**')

                st.session_state.attribute_of_entity_df = session.table(
                    "ADMIN.TARGET_ENTITY_ATTRIBUTE"
                ).filter(col("TARGET_ENTITY_NAME") == st.session_state.collection_entity_name)

                data_type_pd = st.session_state.attribute_of_entity_df.select(
                    st.session_state.attribute_of_entity_df.TARGET_ENTITY_ATTRIBUTE_NAME,
                    st.session_state.attribute_of_entity_df.TARGET_ATTRIBUTE_PROPERTIES["data_type"],
                ).to_pandas()

                # Rename Columns for better readability
                data_type_pd.columns = ["ATTRIBUTE NAME", "DATA TYPE"]
                data_type_pd['DATA TYPE'] = data_type_pd['DATA TYPE'].str.replace(r'"', '')

                st.write("Target Entity: " + '**' + st.session_state.collection_entity_name+ '**')

                st.dataframe(data_type_pd)

                st.session_state.data_type_pd = data_type_pd

                st.write("#")
                st.button(
                    label="Back",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=(st.session_state.back_page,),
                )
                st.button(
                    label="Return Home",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=("overview",),
                )
        else:
            with st.sidebar:

                side_col1, side_col2 = st.columns((0.5, 2.5))
                with side_col1:

                    if st.session_state.streamlit_mode != "OSS":
                        image_name = "Images/snow.png"
                        mime_type = image_name.split(".")[-1:][0].lower()
                        with open(image_name, "rb") as f:
                            content_bytes = f.read()
                            content_b64encoded = base64.b64encode(
                                content_bytes
                            ).decode()
                            image_name_string = (
                                f"data:image/{mime_type};base64,{content_b64encoded}"
                            )
                            st.image(image_name_string, width=200)
                    else:
                        dataimage = Image.open("toStage/streamlit/Images/snow.png")
                        st.image(dataimage, width=200)
                st.header("Dynamic Data Model Mapper")
                st.subheader("")
                help_check = st.checkbox('Enable verbose Instructions',
                                         key='help_check')
                st.subheader('')
                with side_col2:
                    st.write("#")

                if 'collection_entity_name' in st.session_state:
                    st.write(st.session_state.selected_target_collection)

                st.info(st.session_state.info)
                st.write("#")
                st.button(
                    label="Back",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=(st.session_state.back_page,),
                )
                st.button(
                    label="Return Home",
                    help="Warning: Changes will be lost!",
                    on_click=set_page,
                    args=("overview",),
                )
