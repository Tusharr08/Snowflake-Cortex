# Import python packages
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session


import pandas as pd
import json

#pd.set_option("max_coldwidth", None)

CORTEX_SEARCH_DATABASE="CORTEX_POC"
CORTEX_SEARCH_SCHEMA="SUMMIT"
CORTEX_SEARCH_SERVICE="TED_SS"

COLUMNS=['NAME', 'DESCRIPTION','MAIN_SPEAKER','URL','EVENT','TITLE', 'SPEAKER_OCCUPATION','VIEWS']


# Write directly to the app


# Get the current credentials
session = get_active_session()
root= Root(session)

cortex_search_service=root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]

def config_options():

    events = session.sql("Select distinct event from ted_main;").collect()

    events_list=['ALL']
    for event in events:
        events_list.append(event.EVENT)

    st.selectbox('Looking for any specific event?', events_list, key="event_value")

    st.number_input("Num Events", value=3 , key="num_events", label_visibility="collapsed")


def get_similiar_events_search_service(query):

    if st.session_state.event_value == 'ALL':
        response= cortex_search_service.search(query, COLUMNS, limit=st.session_state.num_events)
    else:
        filter_obj = {"@eq":{"event": st.session_state.event_value}}
        response = cortex_search_service.search(query, COLUMNS, filter=filter_obj, limit= st.session_state.num_events)

    return response.results

def main():

    st.title("TED TALKS Search Service:rocket:")
    st.write(
        """Welcome to the search service! 
           Search any TED Talk you want to know.
        """
    )
    st.markdown(f"Querying service: `{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_SERVICE}`".replace('"', ''))

    config_options()

    question = st.text_input("Enter question", placeholder="What do you want to know?", label_visibility="collapsed")

    if question:
        response= get_similiar_events_search_service(question)
        results_df= pd.DataFrame(response)

        st.dataframe(results_df)

if __name__ == "__main__":
    main()
# # Use an interactive slider to get user input
# hifives_val = st.slider(
#     "Number of high-fives in Q3",
#     min_value=0,
#     max_value=90,
#     value=60,
#     help="Use this to enter the number of high-fives you gave in Q3",
# )

# #  Create an example dataframe
# #  Note: this is just some dummy data, but you can easily connect to your Snowflake data
# #  It is also possible to query data using raw SQL using session.sql() e.g. session.sql("select * from table")
# created_dataframe = session.create_dataframe(
#     [[50, 25, "Q1"], [20, 35, "Q2"], [hifives_val, 30, "Q3"]],
#     schema=["HIGH_FIVES", "FIST_BUMPS", "QUARTER"],
# )

# # Execute the query and convert it into a Pandas dataframe
# queried_data = created_dataframe.to_pandas()

# # Create a simple bar chart
# # See docs.streamlit.io for more types of charts
# st.subheader("Number of high-fives")
# st.bar_chart(data=queried_data, x="QUARTER", y="HIGH_FIVES")

# st.subheader("Underlying data")
# st.dataframe(queried_data, use_container_width=True)
