# Import python packages
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Summarize
from snowflake.snowpark.functions import col
from streamlit import (vega_lite_chart, code)
import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt
import altair as alt
import numpy as np

# Database and service configuration
CORTEX_SEARCH_DATABASE = "CORTEX_POC"
CORTEX_SEARCH_SCHEMA = "DISPUTE"
CORTEX_SEARCH_SERVICE = "DISPUTE_SAMPLE"
COLUMNS = [
    'DISPUTE_ID', 'CUSTOMER_ID', 'MERCHANT_ID', 'AMOUNT', 'PRODUCT_TYPE', 'DISPUTE_TYPE',
    'CUSTOMER_STATEMENT', 'ASSIGNED_STAFF', 'CUSTOMER_DOCUMENTATION', 'STATUS'
]

# Write directly to the app
st.title("Predicitve Analysis of Chargebacks :chart_with_downwards_trend:")
st.subheader(
    """Utilizing predictive analytics to quickly determine liability in chargeback scenarios
    Based on the validity of member documentation and dispute types.
    """
)



# Get the current credentials
session = get_active_session()
root = Root(session)
cortex_search_service = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]


model_names= session.sql('SELECT MODEL FROM MODEL_PRICING order by CREDITS_PER_MILLION_TOKENS ').to_pandas()

model = st.sidebar.selectbox('Select your model:',model_names)

#per_token =session.sql('SELECT CREDITS_PER_MILLION_TOKENS FROM MODEL_PRICING WHERE model=?',params=[model]).to_pandas()
per_token= session.table('MODEL_PRICING').select('CREDITS_PER_MILLION_TOKENS').filter(col('MODEL')==model).collect()
# st.sidebar.write(per_token)
# st.sidebar.write(per_token[0])
tokens= per_token[0].CREDITS_PER_MILLION_TOKENS

st.sidebar.write(tokens,'credits will be consumed per 1M tokens.')


# Use an interactive slider to get user input

year = st.slider(
    "Select year!",
    min_value=2020,
    max_value=2022,
    value=2021,
    help="Use this to enter the year for which you want to see chargebacks.",
)


sql= f"""
    SELECT * FROM DISPUTE_DATA WHERE EXTRACT(YEAR FROM CHARGEBACK_DATE) <= {year};
"""
data= session.sql(sql).to_pandas()
st.subheader("List of all chargebacks over the years:page_with_curl:")
st.dataframe(data)

with st.expander("Click to see insight on above data"):
    st.subheader("Amount charged on each Product Category by Status:shopping_bags:")
    st.bar_chart(data, x='PRODUCT_TYPE', y='AMOUNT', color='STATUS')



with st.expander("Click to see Yearly Trends"):
    sql2=f"""
    SELECT
    TO_CHAR(CHARGEBACK_DATE, 'YYYY') AS year,
    DISPUTE_TYPE,
    COUNT(*) AS DISPUTE_COUNT
    FROM DISPUTE_DATA WHERE EXTRACT(YEAR FROM CHARGEBACK_DATE) >=2020 and EXTRACT(YEAR FROM CHARGEBACK_DATE)<={year}
    GROUP BY 1, 2
    ORDER BY 1;
    """
    monthly_trends= session.sql(sql2).to_pandas()
    value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])

    st.subheader("Yearly Trends :calendar:")
    with value_chart_tab:
        st.line_chart(monthly_trends, x='YEAR', y='DISPUTE_COUNT', color='DISPUTE_TYPE')
    with value_dataframe_tab:
        st.dataframe(monthly_trends)
    with value_query_tab:
        code(sql2)

st.subheader("Classifying the Disputes:")
st.markdown("You can see who might be responsible for future chargbacks. We'll be predicting on 2023 data.")
sql3= f"""
        SELECT * FROM DISPUTE_OUTCOME;
    """
disputes= session.sql(sql3).to_pandas()
st.dataframe(disputes)

if st.button("Visualize the Prediction :pushpin:"):

    # Streamlit application
    st.header("Chargeback Predictive Analysis")
    # Use Case 1: Distribution of Dispute Liability
    st.subheader("1: Distribution of Dispute Liability")
    
    liability_counts = disputes['LIABLE_FOR_DISPUTE'].value_counts().reset_index()
    liability_counts.columns = ['Liable_For_Dispute', 'Count']
    # Create a vega-lite chart for the pie chart
    liability_pie_chart = {
        "data": {
            "values": liability_counts.to_dict(orient='records')
        },
        "mark": "arc",
        "encoding": {
            "theta": {"field": "Count", "type": "quantitative"},
            "color": {"field": "Liable_For_Dispute", "type": "nominal"}
        }
    }
    st.vega_lite_chart(liability_pie_chart, use_container_width=True)
    
    
    st.subheader("2: Product Type Distribution")
    # Aggregate the data to count outcomes by MEMBER_DOCUMENTATION
    outcomes_count = disputes.groupby(['PRODUCT_TYPE', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')
    # Create a stacked bar chart with Vega-Lite
    stacked_bar_chart = {
        "data": {
            "values": outcomes_count.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "PRODUCT_TYPE","title":"Product Type", "type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        },
        "title": "Proportion of Outcomes Based on Member Documentation"
    }
    # Display the stacked bar chart
    st.vega_lite_chart(stacked_bar_chart,use_container_width=True, theme=None)

    # Use Case 2: Chargeback Reasons Distribution
    st.subheader("3: Dispute Type Distribution")
    reason_counts = disputes.groupby(['DISPUTE_TYPE', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')

    reasons_bar_chart = {
        "data": {
            "values": reason_counts.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "DISPUTE_TYPE", "title":"Dispute Type","type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        }
    }
    st.vega_lite_chart(reasons_bar_chart,use_container_width=True)

st.title("Chargeback: Historical Trend Analysis")

def format_sql(sql):
    # Remove padded space so that it looks good in code and in the ui element
    return sql.replace("\n        ", "\n")


def disputes_by_member_chart():
    # SQL query to count the number of disputes each member has in each year
    sql = f"""
    SELECT
        CUSTOMER_ID,
        YEAR(date_trunc('year', chargeback_date)) AS CHARGEBACK_YEAR,
        count(dispute_id) as YEARLY_DISPUTE_COUNT,
    FROM dispute_data
    where CHARGEBACK_YEAR<>'2023'
    group by CUSTOMER_ID,CHARGEBACK_YEAR
    ORDER BY CUSTOMER_ID, CHARGEBACK_YEAR;
    ;
    """
    # Execute the SQL query and convert to a pandas DataFrame
    disputes_data = session.sql(sql).to_pandas()
    # Render the title and description in the Streamlit app
    st.subheader("**Number of Disputes per Member by Year**")
    # Set up tabs for chart, raw data, and SQL query visibility
    value_dataframe_tab,value_chart_tab, value_query_tab = st.tabs(
        [
            "Raw Data",
            "Chart",
            "SQL Query",
        ],
    )
    with value_chart_tab:
         # Prepare data for the bar chart visualizing yearly dispute counts
        status_distribution = (
            disputes_data.groupby(['CHARGEBACK_YEAR', 'CUSTOMER_ID'])['YEARLY_DISPUTE_COUNT']
            .sum()
            .reset_index()
        )
        # Create Bar Chart using Vega-Lite
        bar_chart_spec = {
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "CUSTOMER_ID",
                    "type": "ordinal",  # Treat year as ordinal
                    "title": "Members",
                },
                "y": {
                    "field": "YEARLY_DISPUTE_COUNT",
                    "type": "quantitative",
                    "title": "Total Yearly Dispute Count",
                },
                "color": {
                    "field": "CHARGEBACK_YEAR",
                    "type": "nominal",
                    "scale": {"scheme": "category10"},  # Using a categorical color scheme
                },
                "tooltip": [
                    {"field": "CUSTOMER_ID", "title": "Member ID"},
                    {"field": "CHARGEBACK_YEAR", "title": "Year"},
                    {"field": "YEARLY_DISPUTE_COUNT", "title": "Count"},
                ]
            },
            "width": 700,
            "height": 400
        }
        # Display the bar chart for yearly dispute counts
        st.markdown("**Yearly Dispute Counts by Member ID**")
        st.vega_lite_chart(data=status_distribution, spec=bar_chart_spec, use_container_width=True)
 
        
    with value_dataframe_tab:
        st.dataframe(disputes_data, use_container_width=True)
        # Display the raw dispute data in a DataFrame

    with value_query_tab:
        # Show the SQL query used to get the data
        st.code(format_sql(sql), "sql")
 
    sql = f"""
    SELECT
        CUSTOMER_ID,
        YEAR(date_trunc('year', chargeback_date)) AS CHARGEBACK_YEAR,
        COUNT(dispute_id) AS YEARLY_DISPUTE_COUNT
    FROM dispute_data
    GROUP BY CUSTOMER_ID, CHARGEBACK_YEAR
    ORDER BY YEARLY_DISPUTE_COUNT DESC
    LIMIT 25;  
"""
    # Execute the SQL query and convert to a pandas DataFrame
    disputes_data = session.sql(sql).to_pandas()
    # Render the title and description in the Streamlit app
    st.subheader("**Top 25 Customers Raising Maximum Disputes**")
    # st.dataframe(disputes_data, use_container_width=True)
    # Set up tabs for chart, raw data, and SQL query visibility
    value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs(
    [
    "Chart",
    "Raw Data",
    "SQL Query",
    ],
    )
    with value_dataframe_tab:
        st.dataframe(disputes_data, use_container_width=True) 
    with value_chart_tab:
    # Prepare data for the bar chart visualizing yearly dispute counts
        status_distribution = (
        disputes_data.groupby(['CHARGEBACK_YEAR', 'CUSTOMER_ID'])['YEARLY_DISPUTE_COUNT']
        .sum()
        .reset_index()
        )
        # Create Bar Chart using Vega-Lite
        bar_chart_spec = {
        "mark": "bar",
        "encoding": {
        "x": {
            "field": "CUSTOMER_ID",
            "type": "ordinal",  # Treat CUSTOMER_ID as ordinal
            "title": "Members",
        },
        "y": {
            "field": "YEARLY_DISPUTE_COUNT",
            "type": "quantitative",
            "title": "Total Yearly Dispute Count",
        },
        "color": {
            "field": "CHARGEBACK_YEAR",
            "type": "nominal",
            "scale": {"scheme": "category10"},  # Using a categorical color scheme
        },
        "tooltip": [
            {"field": "CUSTOMER_ID", "title": "Customer ID"},
            {"field": "CHARGEBACK_YEAR", "title": "Year"},
            {"field": "YEARLY_DISPUTE_COUNT", "title": "Count"},
        ]
        },
        "width": 700,
        "height": 400
        }
        # Display the bar chart for yearly dispute counts
        st.markdown("**Yearly Dispute Counts by Top Customer IDs**")
        st.vega_lite_chart(data=status_distribution, spec=bar_chart_spec, use_container_width=True)
    # Display the raw dispute data in a DataFrame
    with value_query_tab:
    # Show the SQL query used to get the data
        st.code(format_sql(sql), "sql")
 
 
disputes_by_member_chart()

# def display_grouped_bar_chart_all_cust():

#     sql= f"""
#     SELECT customer_id, dispute_type, chargeback_date FROM dispute_data2
#     """

#     valid_df = session.sql(sql).to_pandas()
    
#     # Group the data and create a stacked bar chart using Plotly
#     grouped_data = valid_df.groupby(['CUSTOMER_ID', 'DISPUTE_TYPE']).size().unstack(fill_value=0)
#     fig = px.bar(grouped_data, barmode='stack', labels={'index': 'CUSTOMER_ID', 'value': 'Count'},
#                  title='Count of Occurrences - CUSTOMER_ID vs DISPUTE_TYPE')
#     fig.update_layout(legend_title_text='DISPUTE_TYPE')
#     st.set_option('deprecation.showPyplotGlobalUse', False)
#     st.plotly_chart(fig)


# # Run the Streamlit app
# if __name__ == '__main__':
#     display_grouped_bar_chart_all_cust()

session = get_active_session()

sql= f"""
    select * from dispute_data where fraud_indicator='FALSE' and customer_id in (select customer_id from top25customers) order by customer_id
    """

valid_df = session.sql(sql).to_pandas()


def disputes_valid_invalid():
    # SQL query to count valid and invalid dispute data by customer
    sql = """
         WITH DisputeDetails AS (
    SELECT 
        CUSTOMER_ID,
        COUNT(DISPUTE_ID) AS Number_Of_Disputes,
        SUM(CASE 
            WHEN FRAUD_INDICATOR='TRUE' THEN 1
            ELSE 0 
        END) AS Invalid_Disputes,
        SUM(CASE 
            WHEN  FRAUD_INDICATOR='FALSE' THEN 1
            ELSE 0 
        END) AS Valid_Disputes
    FROM 
        dispute_data 
        where    
        year(chargeback_date)<>'2023' 
        and customer_id in (select customer_id from top25customers)
    GROUP BY 
        CUSTOMER_ID
),
SuspiciousCustomers AS (
    SELECT 
        CUSTOMER_ID,
        Number_Of_Disputes,
        
        Valid_Disputes,
        Invalid_Disputes,
        CASE 
            WHEN Number_Of_Disputes > 15 THEN 'High Frequency'    
            WHEN Number_Of_Disputes BETWEEN 10 AND 15 THEN 'Moderate Frequency' 
            ELSE 'Low Frequency'  
        END AS Frequency_level
    FROM 
        DisputeDetails
)
SELECT 
    CUSTOMER_ID,
    Number_Of_Disputes,
    Frequency_level,
    Valid_Disputes,
    Invalid_Disputes
   
  
FROM 
    SuspiciousCustomers
 order by customer_id;


    
    """
    
    # Execute the SQL query and convert to a pandas DataFrame
    disputes_data = session.sql(sql).to_pandas()
    # Render the title and description in the Streamlit app
    st.subheader("**Dispute Status per Member**")
    # st.dataframe(disputes_data, use_container_width=True)
    # Set up tabs for chart, raw data, and SQL query visibility
    value_chart_tab,value_dataframe_tab, value_query_tab = st.tabs(
        ["Chart","Raw Data", "SQL Query"]
    )
    with value_chart_tab:
        # Prepare data for visualization
        # Melting the DataFrame to have valid and invalid disputes in long format for better visualization
        status_distribution = disputes_data.melt(
            id_vars="CUSTOMER_ID", 
            value_vars=["VALID_DISPUTES", "INVALID_DISPUTES"], 
            var_name="DISPUTE_STATUS", 
            value_name="COUNT"
        )
        # Create Bar Chart using Altair / Vega-Lite
        bar_chart_spec = {
            "mark": "bar",
            "encoding": {
                "x": {
                    "field": "CUSTOMER_ID",
                    "type": "ordinal",  # Treat customer as ordinal
                    "title": "Customer ID",
                },
                "y": {
                    "field": "COUNT",
                    "type": "quantitative",
                    "title": "Dispute Count",
                },
                "color": {
                    "field": "DISPUTE_STATUS",
                    "type": "nominal",
                    "scale": {"scheme": "category10"},  # Categorical color scheme
                },
                "tooltip": [
                    {"field": "CUSTOMER_ID", "title": "Customer ID"},
                    {"field": "DISPUTE_STATUS", "title": "Status"},
                    {"field": "COUNT", "title": "Count"},
                ]
            },
            "width": 700,
            "height": 400
        }
        # Display the bar chart for valid and invalid disputes
        st.markdown("**Valid and Invalid Disputes by Customer ID**")
        st.vega_lite_chart(data=status_distribution, spec=bar_chart_spec, use_container_width=True)

# Exa
    with value_dataframe_tab:
        st.dataframe(disputes_data, use_container_width=True)  # Display the raw dispute data in a DataFrame
    with value_query_tab:
        st.code(sql, "sql")  # Show the SQL query used to retrieve the data

disputes_valid_invalid()

st.title("Valid Customer Chargebacks")
st.dataframe(valid_df)

def display_grouped_bar_chart():

    st.header("Valid Dispute Types raised by Top 25 Customers:")
    # Group the data and create a stacked bar chart using Plotly
    grouped_data = valid_df.groupby(['CUSTOMER_ID', 'DISPUTE_TYPE']).size().unstack(fill_value=0)
    fig = px.bar(grouped_data, barmode='stack', labels={'index': 'CUSTOMER_ID', 'value': 'Count'})
    fig.update_layout(legend_title_text='DISPUTE_TYPE')
    st.set_option('deprecation.showPyplotGlobalUse', False)
    st.plotly_chart(fig)


# Run the Streamlit app
if __name__ == '__main__':
    display_grouped_bar_chart()

sql1= f"""
    with disputes as (
    select customer_id, dispute_type, count(*) as counts
    from (select * from dispute_data where fraud_indicator='FALSE' and customer_id in (select customer_id from top25customers)) 
    group by customer_id, dispute_type
    ),
    customer_summary as (
        select 
            customer_id, 
            sum(counts) as total_chargebacks
        from disputes 
        group by customer_id
    ),
    top_customer as (
        select customer_id, total_chargebacks
        from customer_summary
        order by total_chargebacks desc
        limit 1
    ),
    most_common_dispute as (
        select dispute_type, sum(counts) as total_count
        from disputes
        group by dispute_type
        order by total_count desc
        limit 1
    ),
    summ as (
        select 
            (select customer_id from top_customer) as highest_chargeback_customer,
            (select total_chargebacks from top_customer) as highest_count,
            (select dispute_type from most_common_dispute) as most_common_dispute_type,
            (select total_count from most_common_dispute) as most_common_count
    ),
    analysis as(
    select  concat(
        'You are provided with a dataset of customer chargebacks. Your task is to analyze this dataset and generate a detailed report covering several aspects. Follow the steps below:',
        
        '1. Identify the customer who has filed the highest number of chargebacks: ',
        highest_chargeback_customer, 
        ' with a total count of chargebacks: ',
        highest_count,
        '2. Determine the most common dispute type across all records: ',
        most_common_dispute_type, 
        ' with a total count of: ',
        most_common_count,
        '3. Summarize the findings including trends or patterns.',
      
        '4. Discuss implications of chargeback behaviors on the business.',
        '5. Predict future trends regarding customers likely to file chargebacks.'
    ) as prompt
    from summ
    )
    select snowflake.cortex.complete(?,prompt) from analysis;
"""

summary_df = session.sql(sql1, params=[model]).to_pandas()

summary_text = summary_df.iloc[0][0]  

st.title("Analysis")
st.text_area("Overall analysis on disputes filled by a customer", value=summary_text, height=700)



st.title("High Rejection Rate :crossed_swords:")
st.subheader(
    """We're analyzing the reasons why disputes are often rejected. 
    """
)
st.markdown("""
            Below is our current dataset that includes detailed information about each dispute.
""")
# Get the current credentials
session = get_active_session()


sql=f"""
            SELECT 
            * from DISPUTE_OUTCOME WHERE STATUS='Rejected';
    """
data = session.sql(sql).to_pandas()
st.dataframe(data)

st.markdown("""
            By employing sentiment analysis, 
            we can examine comments left by customers regarding their disputes to find why the disputes are being rejected.
""")

sql= f"""
    select * from dispute_sentiment order by sentiment_result
"""
sentiment_data = session.sql(sql).to_pandas()
st.subheader("Data after Sentimental Analysis:")
st.write("From low sentiment to high.")
st.dataframe(sentiment_data)
st.subheader("Sentiment against Product Type")
st.line_chart(sentiment_data,x="PRODUCT_TYPE",y="SENTIMENT_RESULT")

st.subheader("Need Details?")
dispute_ids = sentiment_data['DISPUTE_ID'].unique().tolist()  # Get unique DISPUTE_IDs
dispute_ids.sort() 
selected_dispute_id = st.selectbox("Select a DISPUTE_ID for more information:", options=[""] + dispute_ids, index=0)

if selected_dispute_id:
    selected_record = sentiment_data[sentiment_data['DISPUTE_ID'] == selected_dispute_id]
    st.markdown(f"Detailed Information for DISPUTE_ID: **{selected_dispute_id}**")
                    # Transpose the DataFrame to display DISPUTE_ID as column

    transposed_record = selected_record.T
    st.dataframe(transposed_record)  # Display transposed details for the selected DISPUTE_ID
                
                # Retrieve and summarize the CUSTOMER_STATEMENT for the selected DISPUTE_ID

    customer_statement = selected_record['CUSTOMER_STATEMENT'].values[0]  # Assume only one row
                        
                        # Generate summary using the Summarize function
    summary = Summarize(customer_statement)
                        
                        # Display the summary
    st.header("Reason of Rejection")
    st.write(summary)
    
    
    def create_prompt(context):
        prompt = f"""
              'You are an expert assistant in extracting insights from the above dispute data and resolve them. 
                Answer the question based on the context and offer actionable solutions tailored for sales executives seeking to enhance customer service. Be concise and do not hallucinate. 
                If you don't have the information, simply state that. 
                Context: {context} 
                Question: 
                Generate actionable recommendations in very short for sales executives to address the issues highlighted in the context and improve customer service effectively. 
                Answer:
               """
        return prompt
    
    def display_solution(summary, model):
        prompt= create_prompt(summary)
    
        cmd = f"""
                 select SNOWFLAKE.CORTEX.COMPLETE(?,?) as response
               """
        
        df_response = session.sql(cmd, params=[model, prompt]).collect()
        
        return df_response

    
    
    if st.button("Want Recommended solution? :seedling:"):
        response=display_solution(summary, model)
        st.header("Recommended way to resolve the dispute:")
        st.markdown(response[0].RESPONSE)



def config_options():
    events = session.sql("SELECT DISTINCT dispute_type FROM DISPUTE_DATA;").collect()
    
    # Create a list of dispute types to choose from, including 'ALL'
    events_list = ['ALL'] + [event.DISPUTE_TYPE for event in events]  # Added 'ALL' option
    
    # Select box for choosing a dispute type (required)
    selected_event = st.selectbox('Looking for any specific dispute type?', events_list, key="event_value")
    
    # Multi-select for choosing columns to filter on (required)
    selected_column = st.multiselect('Select columns to filter on:', ['STATUS', 'PRODUCT_TYPE']) 
    
    # Check if selection for columns has been made
    if not selected_column:
        st.error("Please select at least one column to filter on.")
        return None, None, None, None  # Return None to indicate failure
    
    # For all selected columns, fetch unique values
    unique_values_dict = {}
    for column in selected_column:
        if column:  # Ensure a valid column is selected
            unique_values = session.sql(f"SELECT DISTINCT {column} FROM DISPUTE_DATA;").collect()
            unique_values_dict[column] = [value[0] for value in unique_values] if unique_values else []
    
    # Create a multiselect for each selected column to filter values
    selected_values = {}
    for column, values in unique_values_dict.items():
        selected_values[column] = st.selectbox(f'Select values for **{column}**', values)
        
        # If PRODUCT_TYPE is selected but no values are chosen, display an error:
        if column == 'PRODUCT_TYPE' and not selected_values[column]:
            st.error("Please select at least one value for PRODUCT_TYPE.")
    
    # Number input for limit shown only if at least one column is selected
    limit = None
    if selected_column:
        limit = st.number_input("Enter the number of records to fetch:", min_value=1000, value=1000, key="record_limit")
    
    return selected_event, selected_column, selected_values, limit
    
# def get_summary_cost():
#     # Execute the SQL query to get summary costs
#     query = """
#     SELECT 'SUMMARIZE' AS FUNCTION,
#            SUM(TOKENS) AS TOTAL_TOKENS,
#            SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
#            (SUM(TOKENS)/1000000) * 0.10 AS CREDITS_BILLED,
#            (SUM(TOKENS)/1000000) * 0.10 * 3 AS TOTAL_COST_USD
#     FROM (
#         SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
#                SNOWFLAKE.CORTEX.SUMMARIZE(CUSTOMER_STATEMENT) AS SUMMARY,
#                SNOWFLAKE.CORTEX.COUNT_TOKENS('summarize', CUSTOMER_STATEMENT) AS TOKENS
#         FROM DISPUTE_DATA
#     )
#     """
    
#     # Return the result as a DataFrame
#     result = session.sql(query).collect()
#     return result[0] if result else None
    
def get_all_similar_events_search_service(selected_event, selected_column, selected_values, limit):
    all_records = []
    
    # If selected_event is 'ALL', fetch all records for all dispute types
    if selected_event == 'ALL':
        response = cortex_search_service.search("*", COLUMNS, limit=limit)
    else:
        # Filter for the selected dispute type
        filter_obj = {"@eq": {"dispute_type": selected_event}}
        response = cortex_search_service.search("*", COLUMNS, filter=filter_obj, limit=limit)
    
    # Add fetched records to the list
    all_records.extend(response.results)
    
    # Count the available records
    available_records_count = len(all_records)
    
    # If the available records are fewer than the requested limit, adjust the limit
    if available_records_count < limit:
        limit = available_records_count  # Set limit to the number of available records
    
    # Filter results based on the selected columns and values
    filtered_records = []
    if selected_values:
        filtered_records = all_records  # Start with all records
        for column in selected_column:
            if selected_values[column]:  # Ensure at least one value is selected
                # Filter for records matching the selected values in the respective column
                filtered_records = [record for record in filtered_records if record[column] in selected_values[column]]
    else:
        filtered_records = all_records  # No filtering if no values selected
    
    # Return only the top 'limit' number of records
    return filtered_records[:limit]  # Return only the number of records specified by the limit
    
def generate_recommended_resolution(customer_statement):
    # Create a prompt for the model to generate a recommended resolution
    prompt = (
    "As a skilled dispute resolver, your task is to create a clear and structured resolution that helps the merchant/seller rectify the issue outlined in the customer statement by considering the selected PRODUCT_TYPE and STATUS. Analyze the following customer statement carefully, which expresses the customer's concerns and feelings:\n\n"
    f"{customer_statement}\n\n"
    "Consider the context of the situation and think about potential resolutions that could effectively address the customer's concerns. Your recommendations should be aimed at resolving the current dispute and providing insights to help the merchant/seller improve their operations, enhance customer satisfaction, and reduce future disputes. Please present your recommendations in a clear, point-wise format:\n"
    "1. **Suggested Resolution:** Begin with an apology for the inconvenience caused, acknowledging the customer's feelings, and outline specific resolutions to address their issues in point-wise format.\n"
    "2. **Future Improvement:** Offer actionable strategies to improve operations, enhance customer satisfaction, and prevent similar issues from occurring in the future, along with clear instructions for relevant internal departments in point-wise format.\n"
    "3. [Continue listing additional recommendations as necessary.]\n\n"
    "Ensure that your recommendations include explanations of how each action addresses the customer's concerns and insights for future improvement. Its mandatory to follow the above format with the headings (**Suggested Resolution:** and **Future Improvement:**) mentioned accurately"
   )

    
    resolution = Summarize(prompt)
    return resolution

def main():
    st.title("Automated Summarization for Dispute Investigations🔍📝")
    st.write("Welcome to the search service! Search for disputes and apply filters.")
    
    # Get summary cost information
    # summary_cost_info = get_summary_cost()
    
    # if summary_cost_info:
    #     st.markdown(
    #         f"""
            
    #         Total Cost (USD) Incurred : `${summary_cost_info.TOTAL_COST_USD:.2f}`
            # """)
    st.markdown(f"Querying service: `{CORTEX_SEARCH_DATABASE}.{CORTEX_SEARCH_SCHEMA}.{CORTEX_SEARCH_SERVICE}`".replace('"', ''))
    
    selected_event, selected_column, selected_values, limit = config_options()
    
    # Validate selections
    if selected_event is None or selected_event == '':
        return  # Exit the function if the dropdown is not valid
    if selected_column is None:  # Check if config_options returned None due to validation
        return  # Exit the function
    # Check if PRODUCT_TYPE selection is required
    if 'PRODUCT_TYPE' in selected_column and not selected_values.get('PRODUCT_TYPE'):
        return  # Exit the function if no values are selected for PRODUCT_TYPE
    
    if selected_event:
        # Fetch and display results
        response = get_all_similar_events_search_service(selected_event, selected_column, selected_values, limit)
        results_df = pd.DataFrame(response)
        
        if results_df.empty:
            st.write("No records found.")
        else:
            # Reorder DataFrame columns
            results_df = results_df[COLUMNS]  # Ensure the DataFrame is in the preferred column order
            
            st.write("Results")
            st.dataframe(results_df)  # Display the full results in a DataFrame
            
            # Generate summary for the entire CUSTOMER_STATEMENT column
            if 'CUSTOMER_STATEMENT' in results_df.columns:
                combined_statements = " ".join(results_df['CUSTOMER_STATEMENT'].astype(str).tolist())  # Concatenate all statements
                summary = Summarize(combined_statements)  # Generate a summary for concatenated customer statements
                
                # Display the summary
                st.subheader("Consolidated Summary📌")
                st.write(summary)
                
                # Button to view recommended resolution
                if st.button("Want Recommended Resolution?🗣️"):
                    resolution = generate_recommended_resolution(combined_statements)  # Generate the resolution
                    st.subheader("Recommended Resolution:")
                    st.write(resolution)
            else:
                st.error("Customer statement column not found in the results.")
            
            # Create a dropdown for dispute IDs from the result set
            dispute_ids = results_df['DISPUTE_ID'].unique().tolist()  # Get unique DISPUTE_IDs
            dispute_ids.sort()
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
                    statement_summary = Summarize(customer_statement)
                    
                    # Display the summary
                    st.subheader("Summary selected DISPUTE_ID✍🏽")
                    st.write(statement_summary)
                else:
                    st.error("Customer statement not found in the selected record.")

        cmd = f"""
        SELECT 'COMPLETE' AS FUNCTION, ? as MODEL, SUM(TOKENS) AS TOTAL_TOKENS,
               SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
               (SUM(TOKENS) / 1000000) * 0.22 AS CREDITS_BILLED,
               CREDITS_BILLED * 3 AS COST_USD,
               2 as TIMES_USED,
               2 * COST_USD AS TOTAL_COST_USD
        FROM (
            SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
                   CONCAT(
                       'You are an expert assistant in extracting insights from the above dispute data and resolve them. Answer the question based on the context and offer actionable solutions tailored for sales executives seeking to enhance customer service. Be concise and do not hallucinate. If you don''t have the information, simply state that. Question: Generate actionable recommendations for sales executives to address the issues highlighted in the context and improve customer service effectively. Context: ',
                       CUSTOMER_STATEMENT
                   ) AS prompt,
                   SNOWFLAKE.CORTEX.COUNT_TOKENS(?, prompt) AS TOKENS
            FROM DISPUTE_OUTCOME
            
        ) 
        UNION
        SELECT 'SUMMARIZE' AS FUNCTION, ? AS MODEL, SUM(TOKENS) AS TOTAL_TOKENS,
               SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
               (SUM(TOKENS) / 1000000) * 0.10 AS CREDITS_BILLED,
               CREDITS_BILLED * 3 AS COST_USD,
               4 as TIMES_USED,
               4*COST_USD AS TOTAL_COST_USD
        FROM (
            SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
                   SNOWFLAKE.CORTEX.COUNT_TOKENS('summarize', CUSTOMER_STATEMENT) AS TOKENS
            FROM DISPUTE_OUTCOME
           
        ) 
        UNION
        SELECT 'SENTIMENT' AS FUNCTION, ? AS MODEL, SUM(TOKENS) AS TOTAL_TOKENS,
               SUM(TOKENS)/1000000 AS TOTAL_TOKENS_FOR_CREDITS,
               (SUM(TOKENS) / 1000000) * 0.08 AS CREDITS_BILLED,
               CREDITS_BILLED * 3 AS COST_USD,
               1 as TIMES_USED,
               1 * COST_USD AS TOTAL_COST_USD
        FROM 
        (
            SELECT DISPUTE_ID, CUSTOMER_STATEMENT,
                   SNOWFLAKE.CORTEX.COUNT_TOKENS('sentiment', CUSTOMER_STATEMENT) AS TOKENS
            FROM DISPUTE_OUTCOME
            
        ) 
    """
    # Assuming you have already set the 'model' variable properly
    credits_spent = session.sql(cmd, params=[model, model, model, model]).to_pandas()
    st.subheader("What will be the cost incurred?")
    st.write("We've calculated estimated pricing for the application.")
    st.dataframe(credits_spent)



if __name__ == "__main__":
    main()