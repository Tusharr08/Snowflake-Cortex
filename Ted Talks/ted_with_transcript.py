# Import python packages
import streamlit as st

from snowflake.core import Root
from snowflake.cortex import Summarize

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col


import pandas as pd
import json

#pd.set_option("max_coldwidth", None)

CORTEX_SEARCH_DATABASE="CORTEX_POC"
CORTEX_SEARCH_SCHEMA="SUMMIT"
CORTEX_SEARCH_SERVICE="TED_WITH_TRANSCRIPT_SS"

COLUMNS=['NAME', 'DESCRIPTION','MAIN_SPEAKER','URL','EVENT','TITLE', 'TRANSCRIPT']


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
    
    st.number_input("Number of talks?", value=3 , key="num_events")


def get_similiar_events_search_service(query):

    if st.session_state.event_value == 'ALL':
        response= cortex_search_service.search(query, COLUMNS, limit=st.session_state.num_events)
    else:
        filter_obj = {"@eq":{"event": st.session_state.event_value}}
        response = cortex_search_service.search(query, COLUMNS, filter=filter_obj, limit= st.session_state.num_events)

    return response.results

def main():

    st.title("TED TALKS Search Service:studio_microphone:")
    st.write(
        """Welcome to the search service! 
           Search any TED Talk you want to know.
        """
    )
    st.markdown(f"Querying service: `{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_SERVICE}`".replace('"', ''))

    config_options()

    question = st.text_input("Curious? Ask a question", placeholder="What do you want to know?")

    if question:
        response= get_similiar_events_search_service(question)
        results_df= pd.DataFrame(response)
        orderbycol={}
        orderbycol =st.multiselect('Order by?', COLUMNS)
        results_df = results_df.sort_values(by=orderbycol)
        st.text("Ordered Result")
        st.dataframe(results_df)

        selected_indices = st.selectbox("What summary do you want", results_df.index)

        selected_ted= results_df.loc[selected_indices]

        st.text("Selected TED Talk")
        st.dataframe(selected_ted)

        talk_content= session.table("ted_with_transcript").select('transcript').filter(col('URL')==selected_ted.URL).collect()
        text_to_summarize=talk_content[0].TRANSCRIPT
        text_summarise= Summarize(text_to_summarize)

        st.title("Summary")
        st.markdown(text_summarise)

if __name__ == "__main__":
    main()
