/**
 * Only run this once to create the intitial tables to hold the collected data.
 */function constructTableJson(thisTableData, thisProjectId, thisDatasetId) {

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

    var query_prefix = "INSERT \`" + BQ_DATASET_ID + "." + BQ_TABLE_ID + "\` (timestamp, project, dataset, table, size_bytes, row_count) VALUES ";

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
  Logger.log('MyQuery: ' + JSON.stringify(request) + ' ---- Project ID: ' + projectId);
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

function get_all_projects() {

  var data = BigQuery.Projects.list();

  return data;

}

function get_all_data_sets_for_project(project_id) {

  var data = BigQuery.Datasets.list(project_id);

  return data;

}

function convert_data_set_request_to_id_array(request_data) {

  if (request_data.datasets) {

    var data = new Array();
    for (var i = 0; i < request_data.datasets.length; i++) {
      
      if(request_data.datasets[i].datasetReference) {
        
        var this_array = [
          request_data.datasets[i].datasetReference.projectId,
          request_data.datasets[i].datasetReference.datasetId
        ];

        data.push(this_array);

      }
      
    }
 
  } else {
    
    var data = [];
    
  }

  //console.log(data);

  return data;


}
