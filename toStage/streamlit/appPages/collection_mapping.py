from appPages.page import (BasePage, pd, st,
                           when_matched, when_not_matched, current_timestamp, set_page, time)

if 'session' in st.session_state:
    session = st.session_state.session


def call_get_map_from_columns(state):
    session = st.session_state.session
    if state == 'initial':
        with st.spinner('Generating Attributes...'):
            from_columns = session.call("MAPPING.GET_MAPFROM_COLUMNS", st.session_state.collection_name)
        st.success('Done!')

        st.session_state.from_columns = from_columns

        st.session_state.mapping_state = 'current'


def call_get_map_to_columns():
    session = st.session_state.session

    with st.spinner('Getting to Attributes...'):
        to_columns = session.call("MAPPING.GET_MAPTO_COLUMNS", st.session_state.collection_name)
    st.success('Done!')

    st.session_state.to_columns = to_columns


def save_mapping():
    session = st.session_state.session

    initial_data = []

    for index, row in st.session_state.data_type_pd.iterrows():
        if st.session_state["map_select_" + str(index)] == '':
            continue

        initial_data.append(
            {
                "SOURCE_COLLECTION_NAME": st.session_state.collection_name,
                "GENERATED_MAPPING_TABLE_COLUMN_NAME": st.session_state["map_select_" + str(index)],
                "TARGET_ATTRIBUTE_NAME": row['ATTRIBUTE NAME']
            })

    initial_data_pd = pd.DataFrame(initial_data)
    if len(initial_data_pd) > 0:
        dupe_check = initial_data_pd['GENERATED_MAPPING_TABLE_COLUMN_NAME'].duplicated().any()
        if dupe_check:
            st.error('It seems you have mapped the same column name to mulitple target attributes, please make sure these are 1:1')
        else:
            source_df = session.create_dataframe(initial_data)
            if st.session_state["streamlit_mode"] == "NativeApp":
                target_df = session.table(st.session_state.native_database_name +".configuration.SOURCE_TO_TARGET_MAPPING")
            else:
                target_df = session.table("MAPPING.SOURCE_TO_TARGET_MAPPING")

            target_df.merge(
                source_df,
                (target_df["SOURCE_COLLECTION_NAME"] == source_df["SOURCE_COLLECTION_NAME"]) &
                (target_df["TARGET_ATTRIBUTE_NAME"] == source_df["TARGET_ATTRIBUTE_NAME"]),
                [
                    when_matched().update(
                        {
                            "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                            "GENERATED_MAPPING_TABLE_COLUMN_NAME": source_df["GENERATED_MAPPING_TABLE_COLUMN_NAME"],
                            "TARGET_ATTRIBUTE_NAME": source_df["TARGET_ATTRIBUTE_NAME"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                    when_not_matched().insert(
                        {
                            "SOURCE_COLLECTION_NAME": source_df["SOURCE_COLLECTION_NAME"],
                            "GENERATED_MAPPING_TABLE_COLUMN_NAME": source_df["GENERATED_MAPPING_TABLE_COLUMN_NAME"],
                            "TARGET_ATTRIBUTE_NAME": source_df["TARGET_ATTRIBUTE_NAME"],
                            "LAST_UPDATED_TIMESTAMP": current_timestamp(),
                        }
                    ),
                ],
            )

            session.call("MAPPING.GENERATE_VIEW", st.session_state.collection_name)

            st.info('Save Successful')
        with st.spinner('Redirecting back to Target list in 5 Seconds'):
            time.sleep(5)
        set_page('collection_list')


class CollectionMapping(BasePage):
    def __init__(self):
        self.name = "collection_mapping"

    def print_page(self):
        session = st.session_state.session
        st.header("Collection Mapping")
        st.header('')

        if st.session_state.help_check:
            st.info('''
                    Your generated collection from your defined join is below \n
                    Please use this as a guide to map your 'SOURCE COLLECTION ATTRIBUTES' to the TARGET ATTRIBUTES \n
                    Once finished, hit the 'Save and Continue' button
                      ''')
        st.write('#')

        # Grab existing target attribute session var
        data_type_pd = pd.DataFrame(st.session_state.data_type_pd)

        if 'mapping_state' not in st.session_state:
            st.session_state.mapping_state = 'initial'
            if st.session_state["streamlit_mode"] == "NativeApp":
                qualified_table_name = st.session_state.native_database_name + ".MODELED." + st.session_state.collection_name
            else:
                qualified_table_name = "MAPPING." + st.session_state.collection_name
            st.session_state.dynamic_table = session.table(qualified_table_name)

        call_get_map_from_columns(st.session_state.mapping_state)

        with st.expander("Work Area", expanded=True):
            st.dataframe(st.session_state.dynamic_table)
            st.header('####')

        if 'from_columns' in st.session_state:
            st.header('####')
            from_list = eval(st.session_state.from_columns)
            from_columns_pd = pd.DataFrame(from_list)
            add_new_row = pd.DataFrame({0: ''}, index=[0])
            from_columns_pd = from_columns_pd.sort_values(by=0, ascending=True)
            from_columns_pd = pd.concat([add_new_row, from_columns_pd]).reset_index(drop=True)

            for i in range(len(data_type_pd)):
                col1, col2, col3 = st.columns(3)
                if i == 0:
                    col1.write("**" + data_type_pd.columns[0] + "**")
                    col2.write("**" + data_type_pd.columns[1] + "**")
                    col3.write('SOURCE COLLECTION ATTRIBUTE')
                col1.write(data_type_pd.loc[i, 'ATTRIBUTE NAME'])
                col2.write(data_type_pd.loc[i, 'DATA TYPE'])
                col3.selectbox(
                        "",
                        from_columns_pd,
                        label_visibility='collapsed',
                        key="map_select_" + str(i)
                    )

            bottom_col1, bottom_col2, bottom_col3, bottom_col4 = st.columns((6, 2, 2, 2))
            st.write("#")

            with bottom_col4:
                st.header("")
                done_adding_button = st.button(
                    "Save and Continue",
                    key="done",
                    on_click=save_mapping,
                    type="primary"
                )

    def print_sidebar(self):
        super().print_sidebar()