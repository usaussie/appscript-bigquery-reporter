# appscript-bigquery-reporter
Appscript code to query and collect information about bigquery datasets/tables, and write the results back to BigQuery or Google Sheets

# Usage

 - Copy each file into a new appscript project (script.google.com)
 - Add the BigQuery API Service
 - Update the Config.gs file with your own project/table/sheet info
 - Open the Jobs.gs file, and run one of the jobs.
 - This will prompt you to permit your appscript project to have access to BigQuery, Google Drive/Sheets etc
 
 # Optional
 
 - Use AppScript's triggers to run the job_get_bq_stats() function on a regular schedule (daily/hourly depending on your needs).
