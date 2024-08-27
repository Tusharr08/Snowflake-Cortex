# Import python packages
import streamlit as st 
from snowflake.snowpark.context import get_active_session
from snowflake.core import Root
import pandas as pd
import json

from streamlit import (
    header,
    code,
    dataframe,
    markdown,
    multiselect,
    select_slider,
    tabs,
    vega_lite_chart,
)

pd.set_option("max_colwidth", None)

# Service parameter
CORTEX_SEARCH_DATABASE = "CORTEX_POC"
CORTEX_SEARCH_SCHEMA = "LLM_FUNC"
CORTEX_SEARCH_SERVICE = "SUPERSTORE_SS"

# Columns to query in the service
COLUMNS = ['ORDER_ID','ORDER_DATE','SHIP_DATE','SHIP_MODE','CUSTOMER_ID','CUSTOMER_NAME','SEGMENT','COUNTRY','CITY','STATE','POSTAL_CODE','REGION','PRODUCT_ID','CATEGORY','SUB_CATEGORY','PRODUCT_NAME','SALES','QUANTITY','DISCOUNT','PROFIT']

session = get_active_session()
root = Root(session)

cortex_search_service = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
# Functions
            

def seasonal_buying_patterns(response):
    
    # Convert Pandas DataFrame to Snowpark DataFrame if needed
    if isinstance(response, pd.DataFrame):
        response = session.create_dataframe(response)
        
    # Step 1: Create a temporary table from the Snowpark DataFrame
    temp_table_name = "TEMP_selling_products"
    
    response.write.mode("overwrite").save_as_table(temp_table_name)
    
    sql = f"""
            SELECT 
            TO_CHAR(ORDER_DATE, 'YYYY-MM') AS MONTH,
            STATE,
            SUM(SALES) AS TOTAL_SALES
        FROM 
            SUPERSTORE_SALES
        GROUP BY 
            MONTH,
            STATE
        ORDER BY 
            MONTH, 
            STATE;
    """

    data = session.sql(sql).to_pandas()
    #st.write(data)
    header("**Seasonal Buying Patterns**")
    value_chart_tab, value_dataframe_tab, value_query_tab = tabs(
        [
            "Chart",
            "Raw Data",
            "SQL Query",
        ],
    )

    with value_chart_tab:
        data["TOTAL_SALES"] = pd.to_numeric(data["TOTAL_SALES"])
        vega_lite_chart(
            data=data,
            spec={  # to customize see https://vega.github.io/vega-lite/
                "autosize": {
                    "type": "fit",
                    "contains": "padding",
                    "resize": True,
                },
                "mark": {
                    "type": "line",
                    "tooltip": True,
                },
                "title": None,
                "encoding": {
                    "x": {
                        "field": "MONTH",
                        "title": "MONTH",
                        "type": "temporal",
                        #"sort": "y",
                        #"axis": {"labelLimit": 0},
                    },
                    "y": {
                        "field": "TOTAL_SALES",
                        "title": "TOTAL SALES",
                        "type": "quantitative",
                        
                    },
                    "color":{
                        "field":"STATE",
                        "type":"nominal"
                    }
                },
                "height": 500,
            },
            use_container_width=True,
            theme=None,
        )

    with value_dataframe_tab:
        dataframe(data, use_container_width=True)

    with value_query_tab:
        code(
            sql, "sql"
        )

def impact_of_shipping_mode(response):

        # Convert Pandas DataFrame to Snowpark DataFrame if needed
    if isinstance(response, pd.DataFrame):
        response = session.create_dataframe(response)
        
    # Step 1: Create a temporary table from the Snowpark DataFrame
    temp_table_name = "TEMP_TEMP_sales_per"
    
    response.write.mode("overwrite").save_as_table(temp_table_name)
    
    sql=f"""
    SELECT 
    SHIP_MODE,
    REGION,
    SUM(QUANTITY) AS TOTAL_QUANTITY
    FROM 
        SUPERSTORE_SALES
    GROUP BY 
        SHIP_MODE, 
        REGION
    ORDER BY 
        SHIP_MODE, 
        TOTAL_QUANTITY DESC;
    """

    sales= session.sql(sql).to_pandas()
    header("Impact of Shipping Mode on Quantity Purchased")

    value_chart_tab, value_dataframe_tab, value_query_tab = tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ],)

    with value_chart_tab:
        sales["TOTAL_QUANTITY"]=pd.to_numeric(sales["TOTAL_QUANTITY"])
        vega_lite_chart(
            data=sales,
            spec={
                
                  "mark": "bar",
                  "encoding": {
                    "color": {"field": "SHIP_MODE", "type":"nominal","title":"Ship Mode", "axis":{"labelAngle": 0}},
                    "y": {"field": "TOTAL_QUANTITY", "type": "quantitative","title": "Total Quantity Purchased"},
                    #"xOffset": {"field": "CUSTOMER_TYPE"},
                    "x": {"field": "REGION", "type":"nominal", "title": "Region"},
                    #"strokeDash": {"field":"CUSTOMER_TYPE", "type":"nominal"}
                  }
            },
            use_container_width=True,
            theme=None
        )

    with value_dataframe_tab:
        dataframe(sales, use_container_width=True)

    with value_query_tab:
        code(
            sql, "sql"
        )

def impact_of_discount(response):
    # Convert Pandas DataFrame to Snowpark DataFrame if needed
    if isinstance(response, pd.DataFrame):
        response = session.create_dataframe(response)
        
    # Step 1: Create a temporary table from the Snowpark DataFrame
    temp_table_name = "TEMP_TEMP_mon_sales"
    
    response.write.mode("overwrite").save_as_table(temp_table_name)
    
    sql=f"""
    SELECT 
    REGION,
    CATEGORY,
    SUM(SALES) AS TOTAL_SALES,
    AVG(DISCOUNT) AS AVG_DISCOUNT
    FROM 
        SUPERSTORE_SALES
    WHERE 
        DISCOUNT > 0  -- Only consider entries with discounts
    GROUP BY 
        REGION, 
        CATEGORY
    ORDER BY 
        REGION, 
        CATEGORY;
    """
    monthly_sales= session.sql(sql).to_pandas()
    header("Impact of Discounts on Sales by Product Category and Region")

    value_chart_tab, value_dataframe_tab, value_query_tab = tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ],)

    with value_chart_tab:
        monthly_sales["TOTAL_SALES"]=pd.to_numeric(monthly_sales["TOTAL_SALES"])
        layered_chart = {
            "encoding": {
                        "x": {
                            "field": "CATEGORY",
                            "title": "Category",
                            "type": "nominal",
                           # "scale": {"type": "utc"},
                        },
                    },
            "resolve": {"scale": {"y": "independent"}},
            "layer":[
                        {
                            "mark": {
                                "type": "bar",
                                # "point": False,
                                # "tooltip": True,
                            },
                            "encoding": {
                                "y": {
                                    "field": "TOTAL_SALES",
                                    "title": "TOTAL SALES",
                                    "type": "quantitative",
                                    
                                },
                            },
                        },
                        {
                            "mark": {
                                "stroke": "#FC2947",
                                "type": "line",
                                "point": False,
                                "tooltip": True,
                            },
                            "encoding": {
                                "y": {
                                    "field": "AVG_DISCOUNT",
                                    "title": "Average Discount",
                                    "type": "quantitative",
                                    
                                },
                            },
                        },
                    ]
    }
    # Render the layered chart in Streamlit
    st.vega_lite_chart(monthly_sales, layered_chart, use_container_width=True, theme="streamlit")
        
    with value_dataframe_tab:
        dataframe(monthly_sales, use_container_width=True)

    with value_query_tab:
        code(
            sql, "sql"
        )

def product_preferences_across_geographic_areas(response):
    # Convert Pandas DataFrame to Snowpark DataFrame if needed
    if isinstance(response, pd.DataFrame):
        response = session.create_dataframe(response)
        
    # Step 1: Create a temporary table from the Snowpark DataFrame
    temp_table_name = "TEMP_TEMP_selling_products"
    response.write.mode("overwrite").save_as_table(temp_table_name)
    
    sql = f"""
        SELECT 
        Country,
        City,
        Category,
        SUM(Sales) AS TotalSales
        FROM superstore_sales
        GROUP BY Country,City,Category
        ORDER BY Country,City,TotalSales DESC;
    """
 
    data = session.sql(sql).to_pandas()
    #st.write(data)
    header("**Product preferences across geographic areasr**")
    value_chart_tab, value_dataframe_tab, value_query_tab = tabs(
        [
            "Chart",
            "Raw Data",
            "SQL Query",
        ],
    )

 
    with value_chart_tab:
        data["TOTALSALES"] = pd.to_numeric(data["TOTALSALES"])
        st.vega_lite_chart(
                data=data,
                spec={
                    "mark": "bar",
                    "encoding": {
                     "color": { "field": "CATEGORY", "type": "nominal", "title": "Product Category" },
                     "x": { "field": "CITY", "type": "nominal", "title": "City" },
                     "y": {
                         
                         "field": "TOTALSALES",
                         "type": "quantitative",
                         "title": "Total Sales"
                         }
                      },
                     "facet": {
                         "column": { "field": "Country", "type": "nominal", "title": "Country" }
                         },
                     "config": {
                         "axisX": { "labelAngle": -45 }
                     }
 
                    },
                use_container_width=True,
                theme="streamlit"
        )

 
    with value_dataframe_tab:
        dataframe(data, use_container_width=True)
 
    with value_query_tab:
        code(
            sql, "sql"
        )

def discount_effect(response):
    
        # Convert Pandas DataFrame to Snowpark DataFrame if needed
    if isinstance(response, pd.DataFrame):
        response = session.create_dataframe(response)
        
    # Step 1: Create a temporary table from the Snowpark DataFrame
    temp_table_name = "TEMP_TEMP_avg_ratings"
    
    response.write.mode("overwrite").save_as_table(temp_table_name)
    
    sql = f"""
        SELECT 
        SUB_CATEGORY,
        DISCOUNT,
        SUM(SALES) AS TOTAL_SALES
        FROM 
            SUPERSTORE_SALES
        GROUP BY 
            SUB_CATEGORY, 
            DISCOUNT
        ORDER BY 
            SUB_CATEGORY, 
            DISCOUNT;
    """
    ratings = session.sql(sql).to_pandas()
    st.header("Discount Effect on Sales Volume By Sub Category")
    value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
    with value_chart_tab:
       
        ratings["TOTAL_SALES"] = pd.to_numeric(ratings["TOTAL_SALES"], errors='coerce')
        
        if not ratings.empty: 
            st.vega_lite_chart(
                data=ratings,
                spec={
                    "mark": "point",
                    "encoding": {
                        "x": {"field": "DISCOUNT", "type": "quantitative", "title": "Discount (%)"},
                        "y": {"field": "TOTAL_SALES", "type": "quantitative", "title": "Total Sales"},
                    "color": {"field": "SUB_CATEGORY", "type": "nominal", "title": "Sub Category"},
                    "tooltip": [
                            {"field": "PRODUCT_ID", "title": "Product ID"},
                            {"field": "DISCOUNT", "title": "Discount"},
                            {"field": "TOTAL_SALES", "title": "Total Sales"}
                        ]

                    }
                },
                use_container_width=True,
                theme=None 
            )
        else:
            st.write("No data available for the selected criteria.")
    with value_dataframe_tab:
        st.dataframe(ratings, use_container_width=True)
    with value_query_tab:
        st.code(sql,"sql")




def config_options():
    events = session.sql("SELECT DISTINCT SEGMENT FROM SUPERSTORE_SALES;").collect()
    events_list = ["ALL"]
    for event in events:
        events_list.append(event.SEGMENT)
    st.selectbox('Looking for any specific SEGMENT?', events_list, key="event_value")
    # New drop-down for ordering the results
    order_by_column = st.selectbox("Order by", COLUMNS, index=0, key="order_by")
    
    st.number_input("How many records?", value=100, key="num_events")
    return order_by_column  # Return the selected order_by column


def get_similar_events_search_service(query, order_by):
    if st.session_state.event_value == "ALL":
        response = cortex_search_service.search(query, COLUMNS, limit=st.session_state.num_events)
    else:
        filter_obj = {"@eq": {"SEGMENT": st.session_state.event_value}}
        response = cortex_search_service.search(query, COLUMNS, filter=filter_obj, limit=st.session_state.num_events)
    
    # Sort the results based on the selected order_by column after fetching
    if order_by:
        response.results.sort(key=lambda x: x[order_by])

    # Create DataFrame with a predefined column order
    preferred_order = ['ORDER_ID','ORDER_DATE','SHIP_DATE','SHIP_MODE','CUSTOMER_ID','CUSTOMER_NAME','SEGMENT','COUNTRY','CITY','STATE','POSTAL_CODE','REGION','PRODUCT_ID','CATEGORY','SUB_CATEGORY','PRODUCT_NAME','SALES','QUANTITY','DISCOUNT','PROFIT']
    result_df = pd.DataFrame(response.results, columns=preferred_order)
    return result_df
    
    #return response.results


def main():
    st.title("SuperStore Sales Search Service :department_store:")
    order_by = config_options()  # Get selected order_by column
    question = st.text_input("Search for your product!", placeholder="What you want to know?")
    if question:
        response = get_similar_events_search_service(question, order_by)  # Pass the order_by to the function
        #st.write(response)
        result_df = pd.DataFrame(response)
        st.dataframe(result_df)

        seasonal_buying_patterns(result_df)
        impact_of_shipping_mode(result_df)
        impact_of_discount(result_df)
        #product_preferences_across_geographic_areas(result_df)
        discount_effect(result_df)
        #st.line_chart(result_df, x="GENDER", y=["TOTAL","RATING"],color=["#FF0000", "#0000FF"])
if __name__ == "__main__":
    main()