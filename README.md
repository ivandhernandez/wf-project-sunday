# Wildflour Data Pipeline Documentation
## Introduction

The following README serves as documentation for an automated data pipeline that processes multi-branch DBF sales files into a consolidated object storage in DigitalOcean Spaces. The data pipeline uses Apache Airflow Directed Acylic Graphs (DAG’s) to orchestrate the workflow between steps.

## Prerequisites

To implement the pipeline, Apache Airflow must be installed and configured on a laptop to run the weekly jobs. Apache Airflow is a platform that allows users to orchestrate and monitor workflows (Apache Airflow documentation). In addition to this, DigitalOcean Spaces must be configured to separate folders by branch and date. There are several parameters in the code (which have been indicated with a note on the file) that need to be edited once the DigitalOcean Spaces bucket system is implemented. It is up to the user to edit this and the wf_data_dag.py file can be tweaked to indicate the folder set up.

## Implementation
The data pipeline workflow can be found in the wf_data_dag.py file. The file consists mainly of three steps. The first step grabs the DBF files of each branch from the DigitalOcean Spaces storage and converts these files into CSV files back into another DigitalOcean Spaces bucket. The second step applies transformations into the sales reports needed by the company. The last step transforms these tables into a master table which is conversely connected to a Tableau Dashboard.

### Set up

To trigger the workflow, Apache Airflow sets a scheduled job to run weekly on Sundays at midnight. This can be edited to your liking using a cron expression on the `schedule_interval` parameter (scheduler documentation). Other default arguments such as the `email`, `email_on_failure`, and `email_on_retry` can be customized to send updates on workflow jobs. 

Once the workflow is triggered, the first step of the workflow begins. `DBF_to_Sales_Reports` converts all newly uploaded DBF files from the target DigitalOcean Spaces bucket into a CSV file in a separate folder that hosts the processed files. Once all the newly transformed CSV’s are uploaded, we can optionally delete the old DBF files to reserve space in the bucket. I have commented out the code for this in the file in case this is an option that you want done. The converted dataframes are then processed to gather sales metrics and reports, and loaded into a separate DigitalOcean Spaces bucket.

After these separate dataframes are created, we concatenate the resulting branches into master files that host data among all branches. These are then uploaded to our DigitalOcean bucket and are used for the Tableau dashboard
