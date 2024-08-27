# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import Summarize
from snowflake.snowpark.functions import col
from streamlit import vega_lite_chart

# Write directly to the app
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
            * from DISPUTE_ANALYSIS;
    """
data = session.sql(sql).to_pandas()
st.dataframe(data)

st.subheader("STATUS OF DISPUTE FOR EACH PRODUCT_TYPE WITH AMOUNT ❄️")
st.bar_chart(data, x='PRODUCT_TYPE', y='TRANSACTION_AMOUNT', color='STATUS')

st.markdown("""
            By employing sentiment analysis, 
            we can examine comments left by members regarding their disputes to find the most common negative sentiments.
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
selected_indices = st.selectbox("Which Dispute number you want to know about?", sentiment_data.index)

selected_ted= sentiment_data.loc[selected_indices]
st.text("Selected Customer Review:")
st.dataframe(selected_ted)  

talk_content= session.table("dispute_sentiment").select('CUSTOMER_STATEMENT').filter(col('DISPUTE_ID')==selected_ted.DISPUTE_ID).collect()
text_to_summarize=talk_content[0].CUSTOMER_STATEMENT
text_summarise= Summarize(text_to_summarize)

st.header("Reason of Rejection")
st.markdown(text_summarise)
