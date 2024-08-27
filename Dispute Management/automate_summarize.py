# Import python packages
import streamlit as st
from snowflake.core import Root
from snowflake.cortex import Summarize
from snowflake.snowpark.context import get_active_session
import pandas as pd
# Database and service configuration

CORTEX_SEARCH_DATABASE = "CORTEX_POC"
CORTEX_SEARCH_SCHEMA = "DISPUTE"
CORTEX_SEARCH_SERVICE = "DISPUTE_SAMPLE"
COLUMNS = [
    'DISPUTE_ID', 'CUSTOMER_ID', 'TRANSACTION_AMOUNT',
    'DISPUTE_DATE', 'PRODUCT_TYPE', 'DISPUTE_TYPE',
    'CUSTOMER_STATEMENT', 'ASSIGNED_STAFF', 'SUPPORTING_DOCUMENTS', 'STATUS'
]
# Get the current credentials
session = get_active_session()
root = Root(session)
# Access the Cortex search service
cortex_search_service = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
def config_options():
    events = session.sql("SELECT DISTINCT dispute_type FROM DISPUTE_ANALYSIS;").collect()
    
    # Create a list of dispute types to choose from
    events_list = ['ALL']
    for event in events:
        events_list.append(event.DISPUTE_TYPE)
        
    # Select box for choosing a dispute type
    selected_event = st.selectbox('Looking for any specific dispute type?', events_list, key="event_value")
    
    # Column selection for filtering
    selected_column = st.selectbox('Select a column to filter on:', ['STATUS', 'PRODUCT_TYPE']) 
    
    # Fetch unique values for the selected column
    unique_values = []
    if selected_column:  # Check to ensure a valid column is selected
        unique_values = session.sql(f"SELECT DISTINCT {selected_column} FROM DISPUTE_ANALYSIS;").collect()
    
    unique_values_list = ['ALL'] + [value[0] for value in unique_values] if unique_values else []
    selected_value = st.selectbox(f'Select a value for **{selected_column}**', unique_values_list)
    return selected_event, selected_column, selected_value
    
def get_all_similar_events_search_service(selected_event, selected_column, selected_value):
    all_records = []
    limit = 1000  # Maximum records to fetch per call
    while True:
        if selected_event == 'ALL':
            # Fetch all records for all dispute types
            response = cortex_search_service.search("*", COLUMNS, limit=limit)  # Use of limit only
        else:
            # Filter for the selected dispute type
            filter_obj = {"@eq": {"dispute_type": selected_event}}
            response = cortex_search_service.search("*", COLUMNS, filter=filter_obj, limit=limit)
            
        # Add fetched records to the list
        all_records.extend(response.results)
        
        # If fewer records are returned than requested, stop fetching
        if len(response.results) < limit:
            break  # No more records to fetch
            
    # Filter results based on the selected column and value
    filtered_records = []
    if selected_value == "ALL":
        filtered_records = all_records  # Show all records if "ALL" is selected
    else:
        # Only add records that match the selected column value
        filtered_records = [record for record in all_records if record[selected_column] == selected_value]
    return filtered_records
    
def main():
    st.title("Automated Summarization for Dispute Investigations🔍📝")
    st.write("Welcome to the search service! Search for disputes and apply filters.")
    st.markdown(f"Querying service: `{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_SERVICE}`".replace('"', ''))
    
    selected_event, selected_column, selected_value = config_options()
    
    if selected_event:
        # Fetch and display results
        response = get_all_similar_events_search_service(selected_event, selected_column, selected_value)
        results_df = pd.DataFrame(response)
        
        if results_df.empty:
            st.write("No records found.")
        else:
            # Reorder DataFrame columns
            results_df = results_df[COLUMNS]  # Ensure the DataFrame is in the preferred column order
            
            st.write("Results")
            st.dataframe(results_df)  # Display the full results in a DataFrame
            
            # Create a dropdown for dispute IDs from the result set
            dispute_ids = results_df['DISPUTE_ID'].unique().tolist()  # Get unique DISPUTE_IDs
            selected_dispute_id = st.selectbox("Select a DISPUTE_ID for more information:", options=[""] + dispute_ids, index=0)

            
            # Optionally, display additional information for the selected DISPUTE_ID
            if selected_dispute_id:
                selected_record = results_df[results_df['DISPUTE_ID'] == selected_dispute_id]
                st.markdown(f"Detailed Information for DISPUTE_ID: **{selected_dispute_id}**")
                # Transpose the DataFrame to display DISPUTE_ID as column
                transposed_record = selected_record.T
                st.dataframe(transposed_record)  # Display transposed details for the selected DISPUTE_ID
                
                # Retrieve and summarize the CUSTOMER_STATEMENT for the selected DISPUTE_ID
                if 'CUSTOMER_STATEMENT' in selected_record.columns:
                    customer_statement = selected_record['CUSTOMER_STATEMENT'].values[0]  # Assume only one row
                    
                    # Generate summary using the Summarize function
                    summary = Summarize(customer_statement)     
                    
                    # Display the summary
                    st.subheader("Summary of CUSTOMER_STATEMENT:")
                    st.write(summary)
                else:
                    st.error("Customer statement not found in the selected record.")
                
if __name__ == "__main__":
    main()