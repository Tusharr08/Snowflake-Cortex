# 🔍 Cortex Search: Low-Latency Fuzzy Search for Snowflake Data

Welcome to the **Cortex Search** quickstart guide! 
Cortex Search enables low-latency, high-quality “fuzzy” search capabilities over your Snowflake data, powering a wide range of search experiences, including **Retrieval-Augmented Generation (RAG)** applications using Large Language Models (LLMs).

## 🌟 Overview
Cortex Search lets you set up a hybrid (vector and keyword) search engine on your text data in minutes. This allows you to focus less on embedding, infrastructure maintenance, and search quality parameter tuning, and more on developing high-quality chat and search experiences with your data. 

**Check out the tutorials in this folder** for step-by-step instructions on using Cortex Search to power AI chat and search applications.

## 📅 When to Use Cortex Search
Cortex Search is ideal for two primary use cases:
1. **Retrieval-Augmented Generation (RAG)**: Utilize Cortex Search as a RAG engine for chat applications, leveraging semantic search to provide customized and contextualized responses.
2. **Enterprise Search**: Implement Cortex Search as a backend for a high-quality search bar embedded within your applications.

### 🚀 Cortex Search for RAG
RAG is a technique that retrieves information from a knowledge base to enrich the generated response of a large language model. Cortex Search acts as the retrieval engine, supplying the LLM with the context necessary to provide answers grounded in your most up-to-date proprietary data.

## 🔧 Example Implementation
This example outlines the steps to create a Cortex Search Service and query it using the REST API. The example utilizes a sample customer support transcript dataset.

### Step 1: Create the Dataset
Run the following SQL commands to create the database and the sample dataset:
```sql
    CREATE OR REPLACE TABLE support_transcripts (
        transcript_text VARCHAR,
        region VARCHAR,
        agent_id VARCHAR
    );
    INSERT INTO support_transcripts VALUES
        ('My internet has been down since yesterday, can you help?', 'North America', 'AG1001'),
        ('I was overcharged for my last bill, need an explanation.', 'Europe', 'AG1002'),
        ('How do I reset my password? The email link is not working.', 'Asia', 'AG1003'),
        ('I received a faulty router, can I get it replaced?', 'North America', 'AG1004');
```
### Step 2: Create the Search Service
Create a Cortex Search Service using the following SQL command:

```sql
    CREATE OR REPLACE CORTEX SEARCH SERVICE transcript_search_service
    ON transcript_text
    ATTRIBUTES region
    WAREHOUSE = mywh
    TARGET_LAG = '1 day'
    AS (
        SELECT
            transcript_text,
            region,
            agent_id
        FROM support_transcripts
    );
```

Explanation:
This command initializes the search service for your data.
Queries will target the transcript_text column.
The TARGET_LAG parameter indicates the service will check the support_transcripts table for updates approximately once per day.
The region and agent_id columns will be indexed for returning results and filtering.

### 📞 Support & Contributions
We appreciate your contributions and feedback! If you have any questions or suggestions, please feel free to open an issue in this repository.
Leverage the full power of your Snowflake data with Cortex Search to create seamless, intelligent search experiences! 🚀