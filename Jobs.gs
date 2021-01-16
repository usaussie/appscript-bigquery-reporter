/**
 * 
 * THIS CONTAINS THE FUNCTIONS FOR SCHEDULED JOBS/TRIGGERS
 * 
 * DO NOT CHANGE ANYTHING IN THIS FILE
 * 
 */

/**
 * Only run this once to create the intitial tables to hold the CSV data.
 */

function create_tables_one_time() {
  
  var my_tables = tables_to_create();

  for (i = 0; i < my_tables.length; i++) {
    
    // generate correct function / table info from detected string
    var tableFunction;
    tableFunction = new Function('return ' + my_tables[i]);
    var thisTable = tableFunction()();

    var tableJson = constructTableJson(thisTable, BQ_PROJECT_ID, BQ_DATASET_ID);
    createTable(thisTable.tableId, BQ_PROJECT_ID, BQ_DATASET_ID, tableJson);

  }
  
}

function job_get_bq_stats() {

  var this_timestamp = Utilities.formatDate(new Date(), "UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");
  
  var projects = project_dataset_list();

  // loop the outer array
  var stats_array = []
  for (let i = 0; i < projects.length; i++) {
      
      var my_query = construct_select_query(projects[i][1]);
      var query_data = runQuery(projects[i][0], my_query);

      // if there are results, add the extra info (timestamp, project etc) ready for storage/insert into our sheet/bq table
      if(query_data.length > 0) {

        for (let q = 0; q < query_data.length; q++) {

          stats_array.push([
            this_timestamp,
            projects[i][0],
            projects[i][1],
            query_data[q][0],
            query_data[q][1],
            query_data[q][2],
          ]);

        }

      }
      
  }

  // write to bigquery
  var insert_query = construct_insert_query(stats_array);
  runQuery(BQ_PROJECT_ID, insert_query);

  //write to google sheet now
  // write collected rows arrays to the sheet in one operation (quicker than individual appends)
  var ss = SpreadsheetApp.openByUrl(SHEET_URL).getSheetByName(MAIN_SHEET_TAB_NAME);
  ss.getRange(ss.getLastRow() + 1, 1, stats_array.length, stats_array[0].length).setValues(stats_array);

}
