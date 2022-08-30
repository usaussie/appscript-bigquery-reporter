/**
 * 
 * THIS CONTAINS THE FUNCTIONS FOR SCHEDULED JOBS/TRIGGERS
 * 
 * DO NOT CHANGE ANYTHING IN THIS FILE
 * 
 */

/**
 * Only run this once to create the intitial tables to hold the collected data.
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
  
  //get projects
  var projects = project_list();

  // convert projects to project/dataset array
  var dataset_array = [];
  for (let i = 0; i < projects.length; i++) {
    var this_dataset_data = get_all_data_sets_for_project(projects[i]);
    var converted_data = convert_data_set_request_to_id_array(this_dataset_data);
    
    for (let j = 0; j < converted_data.length; j++) {
      dataset_array.push(converted_data[j]);
    }
  }
  
  // console.log(dataset_array);
  // return;

  // loop the outer array
  var stats_array = []
  for (let i = 0; i < dataset_array.length; i++) {
      
      var my_query = construct_select_query(dataset_array[i][1]);
      var query_data = runQuery(dataset_array[i][0], my_query);

      // if there are results, add the extra info (timestamp, project etc) ready for storage/insert into our sheet/bq table
      if(query_data.length > 0) {

        for (let q = 0; q < query_data.length; q++) {

          stats_array.push([
            this_timestamp,
            dataset_array[i][0],
            dataset_array[i][1],
            query_data[q][0],
            query_data[q][1],
            query_data[q][2],
          ]);

        }

      }
      
  }

   // chunk the insert statement so larger monitoring projects are not limited by insert statement.
  for (let i = 0; i < stats_array.length; i += insert_chunk_size) {
    const stats_array_chunk = stats_array.slice(i, i + insert_chunk_size);
    // write to bigquery
    var insert_query = construct_insert_query(stats_array_chunk);
    runQuery(BQ_PROJECT_ID, insert_query);

    if(USE_SPREADSHEET){
      //write to google sheet now
      // write collected rows arrays to the sheet in one operation (quicker than individual appends)
      var ss = SpreadsheetApp.openByUrl(SHEET_URL).getSheetByName(MAIN_SHEET_TAB_NAME);
      ss.getRange(ss.getLastRow() + 1, 1, stats_array_chunk.length, stats_array_chunk[0].length).setValues(stats_array_chunk);
    }
  }

}

/*
*
* ONLY RUN THIS ONCE TO SET THE HEADER ROWS FOR THE GOOGLE SHEETS
*/
function set_sheet_headers() {
  
  var sheet = SpreadsheetApp.openByUrl(SHEET_URL).getSheetByName(MAIN_SHEET_TAB_NAME);
  sheet.appendRow(["timestamp","project","dataset","table", "size_bytes", "row_count"]);
  
}
