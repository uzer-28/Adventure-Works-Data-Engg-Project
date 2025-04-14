# Adventure-Works-Data-Engg-Project
### Overview<br>
To build an end-to-end data pipeline with Medallion Architrect that extracts data from raw CSV files, transforms it, and loads it into a data storage for reporting and analytics by using the AdventureWorks datasets from Kaggle.

### Tech Stack<br>
<i>Azure Data Factory (Bronze)</i> - For creating the dynamic pipeline for data ingestion. <br>
<i>Databricks (Silver)</i> - Transformed the data using <i>PySpark</i>. <br>
<i>Azure Synapse Analytics (Gold)</i> - For processing the analytics of data using Serverless SQL Pool. <br>
<i>Azure Data Lake</i> - For data storage. <br>
<i>Power BI</i> - Integrating the visualization tool(optional). <br>

### Pipeline Architecture <br>
![Pipeline Architecture]([images/pipeline-architecture.png](https://media.licdn.com/dms/image/v2/D5622AQF-yuVMPrWBqA/feedshare-shrink_800/B56ZP6MeWUGQAk-/0/1735069395339?e=2147483647&v=beta&t=ng0tS8r5jxEqZ3nTc3Hl3gNAVCpu0_2FHNtSM585O2U))
