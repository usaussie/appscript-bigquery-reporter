function constructTableJson(thisTableData, thisProjectId, thisDatasetId) {

  return{
      tableReference: {
        projectId: thisProjectId,
        datasetId: thisDatasetId,
        tableId: thisTableData.tableId
      },
      schema: thisTableData.schema
    };

}

/**
 * Create Tables
 */
function createTable(thisTableId, thisProjectId, thisDataSetId, tableReferenceJson) {

  table = BigQuery.Tables.insert(tableReferenceJson, thisProjectId, thisDataSetId);
  Logger.log('Table created: %s', thisTableId);

}

function construct_select_query(dataset) {

  return "SELECT table_id, sum(size_bytes) AS size_bytes, sum(row_count) AS row_count FROM "+dataset+".__TABLES__ GROUP BY 1"

}

function construct_insert_query(stats_array) {

    var query_prefix = "INSERT " + BQ_DATASET_ID + "." + BQ_TABLE_ID + " (timestamp, project, dataset, table, size_bytes, row_count) VALUES ";

    var values_queries = [];
    for (var i = 1, numColumns = stats_array.length; i < numColumns; i++) {

      values_queries.push(["(\"" + stats_array[i][0] + "\",\"" + stats_array[i][1] + "\",\"" + stats_array[i][2] + "\",\"" + stats_array[i][3] + "\"," + stats_array[i][4] + "," + stats_array[i][5] + ")"]);

    }

    var full_query = query_prefix + values_queries.join(',');

    return full_query;


}

/**
 * Runs a BigQuery query and logs the results in a spreadsheet.
 */
function runQuery(projectId, my_query) {
  
  var request = {
    query: my_query,
    useLegacySql: false
  };
  var queryResults = BigQuery.Jobs.query(request, projectId);
  var jobId = queryResults.jobReference.jobId;

  // Check on status of the Query Job.
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId);
  }

  // Get all the rows of results.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(projectId, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }

  if (rows) {

    var data = new Array(rows.length);
    for (var i = 0; i < rows.length; i++) {
      var cols = rows[i].f;
      data[i] = new Array(cols.length);
      for (var j = 0; j < cols.length; j++) {
        data[i][j] = cols[j].v;
      }
    }
 
  } else {
    
    var data = [];
    //Logger.log(projectId + ' - No rows or results returned.');
  }

  return data;
}

/*
*
* ONLY RUN THIS ONCE TO SET THE HEADER ROWS FOR THE GOOGLE SHEETS
*/
function set_sheet_headers() {
  
  var sheet = SpreadsheetApp.openByUrl(SHEET_URL).getSheetByName(MAIN_SHEET_TAB_NAME);
  sheet.appendRow(["timestamp","project","dataset","table", "size_bytes", "row_count"]);
  
}

/**
 * Returns an array used to create the BQ tables needed.
 * References the table functions you've set up below.
 */
function tables_to_create() {

  // these must be the exact names of the functions you will define below
  return table_functions = [
    "table_bq_stats"
  ];

}

/**
 * configure the BQ table you want to use for your stats
*/
function table_bq_stats()
{
  
  table = {};
  table.tableId = BQ_TABLE_ID;
  table.schema = {
      fields: [
        {name: 'timestamp', type: 'TIMESTAMP'},
        {name: 'project', type: 'STRING'},
        {name: 'dataset', type: 'STRING'},
        {name: 'table', type: 'STRING'},
        {name: 'size_bytes', type: 'FLOAT'},
        {name: 'row_count', type: 'FLOAT'}
      ]
    };
  
  return table;
  
}
