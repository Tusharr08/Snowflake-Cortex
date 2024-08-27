# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title("Predicitve Analysis of Chargebacks :chart_with_downwards_trend:")
st.subheader(
    """Utilizing predictive analytics to quickly determine liability in chargeback scenarios
    Based on the validity of member documentation and dispute types.
    """
)

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
    SELECT * FROM CHARGEBACKS WHERE EXTRACT(YEAR FROM CHARGEBACK_DATE) = {year};
"""
data= session.sql(sql).to_pandas()
st.subheader("List of all chargebacks over the years:page_with_curl:")
st.dataframe(data)

st.subheader("Amount charged on each CHARGEBACK date:date:")
st.bar_chart(data, x='CHARGEBACK_DATE', y='AMOUNT')



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
        SELECT * FROM CHARGEBACK_PREDICTED_OUTCOME3;
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
    
    
    
    # Aggregate the data to count outcomes by MEMBER_DOCUMENTATION
    outcomes_count = prediction.groupby(['MEMBER_DOCUMENTATION', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')
    # Create a stacked bar chart with Vega-Lite
    stacked_bar_chart = {
        "data": {
            "values": outcomes_count.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "MEMBER_DOCUMENTATION", "type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        },
        "title": "Proportion of Outcomes Based on Member Documentation"
    }
    # Display the stacked bar chart
    st.vega_lite_chart(stacked_bar_chart,use_container_width=True, theme=None)

    # Use Case 2: Chargeback Reasons Distribution
    st.subheader("Chargeback Reasons Distribution")
    reason_counts = prediction.groupby(['CHARGEBACK_REASON', 'LIABLE_FOR_DISPUTE']).size().reset_index(name='Count')

    reasons_bar_chart = {
        "data": {
            "values": reason_counts.to_dict(orient='records')
        },
        "mark": "bar",
        "encoding": {
            "x": {"field": "CHARGEBACK_REASON", "type": "nominal", "axis": {"labelAngle": -45}},
            "y": {"field": "Count", "type": "quantitative"},
            "color": {"field": "LIABLE_FOR_DISPUTE", "type": "nominal", "scale": {"scheme": "category10"}}
        }
    }
    # Streamlit application
    st.subheader("3: Chargeback Reasons Distribution")
    # Display the stacked bar chart
    st.vega_lite_chart(reasons_bar_chart,use_container_width=True)

    
    # Use Case 4: Trends Over Time
    st.subheader("Chargeback Trends Over Time")
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
