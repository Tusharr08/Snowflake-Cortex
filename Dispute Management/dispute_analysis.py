# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Summarize
from snowflake.snowpark.functions import col
from streamlit import vega_lite_chart

# Write directly to the app
st.title("Predicitve Analysis of Chargebacks :chart_with_downwards_trend:")
st.subheader(
    """Utilizing predictive analytics to quickly determine liability in chargeback scenarios
    Based on the validity of member documentation and dispute types.
    """
)

model = st.sidebar.selectbox('Select your model:',(
                                    'mixtral-8x7b',
                                    'snowflake-arctic',
                                    'mistral-large',
                                    'llama3-8b',
                                    'llama3-70b',
                                    'reka-flash',
                                     'mistral-7b',
                                     'llama2-70b-chat',
                                     'gemma-7b'))
# Get the current credentials
session = get_active_session()

# Use an interactive slider to get user input

year = st.slider(
    "Select year!",
    min_value=2020,
    max_value=2023,
    value=2021,
    help="Use this to enter the year for which you want to see chargebacks.",
)


sql= f"""
    SELECT * FROM DISPUTE_DATA WHERE EXTRACT(YEAR FROM CHARGEBACK_DATE) = {year};
"""
data= session.sql(sql).to_pandas()
st.subheader("List of all chargebacks over the years:page_with_curl:")
st.dataframe(data)

with st.expander("Click to see insight on above data"):
    st.subheader("Amount charged on each Product Category by Status:shopping_bags:")
    st.bar_chart(data, x='PRODUCT_TYPE', y='AMOUNT', color='STATUS')



with st.expander("Click to see Monthly Trends"):
    sql2=f"""
    SELECT
    TO_CHAR(CHARGEBACK_DATE, 'YYYY-MM') AS month_year,
    Chargeback_Reason,
    COUNT(*) AS chargeback_count
    FROM CHARGEBACKS WHERE EXTRACT(YEAR FROM CHARGEBACK_DATE) >=2020 and EXTRACT(YEAR FROM CHARGEBACK_DATE)<={year}
    GROUP BY 1, 2
    ORDER BY 1;
    """
    monthly_trends= session.sql(sql2).to_pandas()
    st.subheader("Monthly Trends :calendar:")
    st.bar_chart(monthly_trends, x='MONTH_YEAR', y='CHARGEBACK_COUNT', color='CHARGEBACK_REASON')

st.subheader("Classifying the Disputes:")
st.markdown("You can see who might be responsible for future chargbacks.")
sql3= f"""
        SELECT * FROM DISPUTE_OUTCOME;
    """
prediction= session.sql(sql3).to_pandas()
st.dataframe(prediction)
if st.button("Visualize the Prediction"):

    # Streamlit application
    st.header("Chargeback Predictive Analysis")
    # Use Case 1: Distribution of Dispute Liability
    st.subheader("1: Distribution of Dispute Liability")
    liability_counts = prediction['LIABLE_FOR_DISPUTE'].value_counts().reset_index()
    liability_counts.columns = ['Liable For Dispute', 'Count']
    # Create a vega-lite chart for the pie chart
    liability_pie_chart = {
        "data": {
            "values": liability_counts.to_dict(orient='records')
        },
        "mark": "arc",
        "encoding": {
            "theta": {"field": "Count", "type": "quantitative"},
            "color": {"field": "Liable For Dispute", "type": "nominal"}
        }
    }
    st.vega_lite_chart(liability_pie_chart, use_container_width=True)
    
    
    st.subheader("2: Product Type Distribution")
    # Aggregate the data to count outcomes by MEMBER_DOCUMENTATION
    outcomes_count = prediction.groupby(['DISPUTE_TYPE', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')
    # Create a stacked bar chart with Vega-Lite
    stacked_bar_chart = {
        "data": {
            "values": outcomes_count.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "DISPUTE_TYPE", "type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        },
        "title": "Proportion of Outcomes Based on Member Documentation"
    }
    # Display the stacked bar chart
    st.vega_lite_chart(stacked_bar_chart,use_container_width=True, theme=None)

    # Use Case 2: Chargeback Reasons Distribution
    st.subheader("3: Dispute Type Distribution")
    reason_counts = prediction.groupby(['DISPUTE_TYPE', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')

    reasons_bar_chart = {
        "data": {
            "values": reason_counts.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "DISPUTE_TYPE", "type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        }
    }
    st.vega_lite_chart(reasons_bar_chart,use_container_width=True)

    
    # Use Case 4: Trends Over Time
    st.subheader("4: Chargeback Trends Over Time")
    prediction['CHARGEBACK_DATE'] = prediction['CHARGEBACK_DATE'].dt.date
    trends = prediction.groupby(['CHARGEBACK_DATE', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')
    st.vega_lite_chart(
        trends,
        {
            "mark": "area",
            "encoding": {
                "x": {"field": "CHARGEBACK_DATE", "type": "temporal"},
                "y": {"field": "Count", "type": "quantitative"},
                "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal"},
                "stroke": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal"}
            }
        },
        use_container_width=True
    )

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
            * from DISPUTE_DATA;
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
              'You are an expert assistant in extracting insights from the provided context. 
                Answer the question based on the context and offer actionable solutions tailored for sales executives seeking to enhance customer service. Be concise and do not hallucinate. 
                If you don't have the information, simply state that. 
                Context: {context} 
                Question: 
                Generate actionable recommendations for sales executives to address the issues highlighted in the context and improve customer service effectively. 
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

    response=display_solution(summary, model)
    st.header("Recommended Solution:")
    st.markdown(response[0].RESPONSE)




