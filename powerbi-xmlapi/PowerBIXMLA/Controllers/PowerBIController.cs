using Microsoft.AspNetCore.Mvc;
using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Identity.Client;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Data;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace YourNamespace.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PowerBIController : ControllerBase
    {
        private readonly PowerBISettings _config;
        private readonly ILogger<PowerBIController> _logger;

        public PowerBIController(IOptions<PowerBISettings> config, ILogger<PowerBIController> logger)
        {
            _config = config.Value;
            _logger = logger;
        }

        [HttpPost("run-query")]
        public async Task<IActionResult> RunQuery([FromBody] QueryRequest request)
        {
            try
            {
                _logger.LogInformation($"Executing DAX query for dataset: {request.Catalog}");
                
                var accessToken = await GetAccessTokenAsync();
                
                // Build a connection string that includes the token
                var connectionString = $"Data Source={_config.WorkspaceConnection};Initial Catalog={request.Catalog};Provider=MSOLAP;";
                _logger.LogInformation($"Base connection string: {connectionString}");
                
                // Create the connection with the access token embedded
                using var connection = new AdomdConnection(connectionString);
                
                try {
                    // Try to use the property directly - might work with some versions
                    var propertyInfo = typeof(AdomdConnection).GetProperty("AccessToken");
                    if (propertyInfo != null)
                    {
                        propertyInfo.SetValue(connection, accessToken);
                    }
                } catch (Exception ex) {
                    _logger.LogWarning($"Could not set AccessToken directly: {ex.Message}");
                    // Fall back to connection string with token
                    connection.ConnectionString = $"{connectionString}Password={accessToken};";
                }

                _logger.LogInformation("Attempting to open connection...");
                connection.Open();
                _logger.LogInformation("Connection opened successfully");

                var command = new AdomdCommand(request.DaxQuery, connection);
                
                // Try to execute the query and get results
                var results = new List<Dictionary<string, object>>();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }

                return Ok(results);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error executing DAX query");
                return BadRequest($"Error executing DAX query: {ex.Message}\n{ex.InnerException?.Message}");
            }
        }

        [HttpGet("metadata")]
        public async Task<IActionResult> GetMetadata([FromQuery] string catalog)
        {
            try
            {
                _logger.LogInformation($"Getting metadata for dataset: {catalog}");
                
                var accessToken = await GetAccessTokenAsync();
                
                var connectionString = $"Data Source={_config.WorkspaceConnection};Initial Catalog={catalog};Provider=MSOLAP;";
                
                // IMPORTANT: Create a new connection string with token embedded directly
                var connectionStringWithToken = $"{connectionString}Password={accessToken};";
                
                using var connection = new AdomdConnection(connectionStringWithToken);
                
                // No more trying to set AccessToken directly since it's already in connection string
                _logger.LogInformation("Attempting to open connection for metadata...");
                connection.Open();
                _logger.LogInformation("Connection opened successfully for metadata");  
                
                // Get tables and their columns using the DISCOVER_STORAGE_TABLE_COLUMNS DMV
                var tablesAndColumns = GetTablesAndColumns(connection);
                _logger.LogInformation($"Found {tablesAndColumns.Count} tables with columns");
                
                // Get measures using MDSCHEMA_MEASURES
                var measures = GetMeasures(connection);
                _logger.LogInformation($"Found {measures.Count} measures");
                
                var metadata = new
                {
                    Tables = tablesAndColumns,
                    Measures = measures
                };

                return Ok(metadata);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting metadata");
                return BadRequest($"Error getting metadata: {ex.Message}\n{ex.InnerException?.Message}");
            }
        }

        private List<TableInfo> GetTablesAndColumns(AdomdConnection connection)
        {
            var tableInfos = new List<TableInfo>();
            
            try
            {
                _logger.LogInformation("Executing query to get tables and columns using DISCOVER_STORAGE_TABLE_COLUMNS");
                
                // Using DISCOVER_STORAGE_TABLE_COLUMNS with the exact query as requested
                string query = @"
                    SELECT 
                        MEASURE_GROUP_NAME,
                        ATTRIBUTE_NAME 
                    FROM $SYSTEM.DISCOVER_STORAGE_TABLE_COLUMNS";
                
                using var command = new AdomdCommand(query, connection);
                using var reader = command.ExecuteReader();
                
                var columnsByTable = new Dictionary<string, List<ColumnInfo>>();
                
                while (reader.Read())
                {
                    var tableName = reader["MEASURE_GROUP_NAME"]?.ToString() ?? "";
                    var columnName = reader["ATTRIBUTE_NAME"]?.ToString() ?? "";
                    // Since we're not querying TYPE, set a default value
                    var dataType = "Unknown";
                    
                    if (!columnsByTable.ContainsKey(tableName))
                    {
                        columnsByTable[tableName] = new List<ColumnInfo>();
                    }
                    
                    columnsByTable[tableName].Add(new ColumnInfo
                    {
                        Name = columnName,
                        DataType = dataType
                    });
                }
                
                // Convert dictionary to list of TableInfo objects
                foreach (var kvp in columnsByTable.OrderBy(t => t.Key))
                {
                    tableInfos.Add(new TableInfo
                    {
                        Name = kvp.Key,
                        Columns = kvp.Value.OrderBy(c => c.Name).ToList()
                    });
                }
                
                _logger.LogInformation($"Successfully retrieved {tableInfos.Count} tables using DISCOVER_STORAGE_TABLE_COLUMNS");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTablesAndColumns using DISCOVER_STORAGE_TABLE_COLUMNS");
                // Try fallback method
                _logger.LogInformation("Attempting fallback method for getting tables and columns");
                return GetTablesAndColumnsFallback(connection);
            }
            
            return tableInfos;
        }

        private List<TableInfo> GetTablesAndColumnsFallback(AdomdConnection connection)
        {
            var tableInfos = new List<TableInfo>();
            
            try
            {
                _logger.LogInformation("Executing fallback query to get tables and columns");
                
                // Fallback to DBSCHEMA_COLUMNS if DISCOVER_STORAGE_TABLE_COLUMNS fails
                string query = @"
                    SELECT 
                        [TABLE_NAME] as TableName,
                        [COLUMN_NAME] as ColumnName,
                        [DATA_TYPE] as DataType 
                    FROM $SYSTEM.DBSCHEMA_COLUMNS";
                
                using var command = new AdomdCommand(query, connection);
                using var reader = command.ExecuteReader();
                
                // Process results in memory and sort them after fetching
                var results = new List<(string TableName, string ColumnName, string DataType)>();
                while (reader.Read())
                {
                    results.Add((
                        reader["TableName"]?.ToString() ?? "",
                        reader["ColumnName"]?.ToString() ?? "",
                        reader["DataType"]?.ToString() ?? ""
                    ));
                }
                
                // Sort the results in memory
                results = results.OrderBy(r => r.TableName).ThenBy(r => r.ColumnName).ToList();
                _logger.LogInformation($"Sorted {results.Count} columns by table and column name");
                
                // Process the sorted results
                var currentTable = "";
                TableInfo tableInfo = null;
                
                foreach (var (tableName, columnName, dataType) in results)
                {
                    if (currentTable != tableName)
                    {
                        if (tableInfo != null)
                        {
                            tableInfos.Add(tableInfo);
                        }
                        
                        tableInfo = new TableInfo
                        {
                            Name = tableName,
                            Columns = new List<ColumnInfo>()
                        };
                        
                        currentTable = tableName;
                    }
                    
                    tableInfo.Columns.Add(new ColumnInfo
                    {
                        Name = columnName,
                        DataType = dataType
                    });
                }
                
                if (tableInfo != null)
                {
                    tableInfos.Add(tableInfo);
                }
                
                _logger.LogInformation($"Successfully retrieved {tableInfos.Count} tables using fallback method");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetTablesAndColumnsFallback");
                // Return empty list instead of throwing to allow partial metadata return
                return new List<TableInfo>();
            }
            
            return tableInfos;
        }

        private List<MeasureInfo> GetMeasures(AdomdConnection connection)
        {
            var measures = new List<MeasureInfo>();
            
            try
            {
                _logger.LogInformation("Executing query to get measures");
                
                // Standard query for measures
                string query = @"
                SELECT 
                    [MEASURE_NAME],
                    [MEASURE_CAPTION],
                    [MEASUREGROUP_NAME] as TableName
                FROM $SYSTEM.MDSCHEMA_MEASURES";
                
                using var command = new AdomdCommand(query, connection);
                using var reader = command.ExecuteReader();
                
                while (reader.Read())
                {
                    // Filter visible measures in memory instead of in query
                    var isVisible = true;
                    try {
                        // Try to get MEASURE_IS_VISIBLE if it exists
                        var visibleObj = reader["MEASURE_IS_VISIBLE"];
                        if (visibleObj != null && visibleObj != DBNull.Value)
                        {
                            isVisible = Convert.ToBoolean(visibleObj);
                        }
                    }
                    catch {
                        // If the column doesn't exist or can't be converted, keep as visible
                        isVisible = true;
                    }
                    
                    if (isVisible)
                    {
                        measures.Add(new MeasureInfo
                        {
                            Name = reader["MEASURE_NAME"]?.ToString() ?? "",
                            Caption = reader["MEASURE_CAPTION"]?.ToString() ?? "",
                            TableName = reader["TableName"]?.ToString() ?? "",
                            Expression = ""  // We're not querying EXPRESSION field
                        });
                    }
                }
                
                _logger.LogInformation($"Successfully retrieved {measures.Count} measures");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetMeasures");
                // Try fallback method without EXPRESSION field
                return GetMeasuresFallback(connection);
            }
            
            return measures;
        }

        private List<MeasureInfo> GetMeasuresFallback(AdomdConnection connection)
        {
            var measures = new List<MeasureInfo>();
            
            try
            {
                _logger.LogInformation("Executing fallback query to get measures");
                
                // Fallback query without EXPRESSION field
                string query = @"
                SELECT 
                    [MEASURE_NAME],
                    [MEASURE_CAPTION],
                    [MEASUREGROUP_NAME] as TableName
                FROM $SYSTEM.MDSCHEMA_MEASURES";
                
                using var command = new AdomdCommand(query, connection);
                using var reader = command.ExecuteReader();
                
                while (reader.Read())
                {
                    // Filter visible measures in memory instead of in query
                    var isVisible = true;
                    try {
                        // Try to get MEASURE_IS_VISIBLE if it exists
                        var visibleObj = reader["MEASURE_IS_VISIBLE"];
                        if (visibleObj != null && visibleObj != DBNull.Value)
                        {
                            isVisible = Convert.ToBoolean(visibleObj);
                        }
                    }
                    catch {
                        // If the column doesn't exist or can't be converted, keep as visible
                        isVisible = true;
                    }
                    
                    if (isVisible)
                    {
                        measures.Add(new MeasureInfo
                        {
                            Name = reader["MEASURE_NAME"]?.ToString() ?? "",
                            Caption = reader["MEASURE_CAPTION"]?.ToString() ?? "",
                            TableName = reader["TableName"]?.ToString() ?? "",
                            Expression = ""
                        });
                    }
                }
                
                _logger.LogInformation($"Successfully retrieved {measures.Count} measures using fallback method");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in GetMeasuresFallback");
                // Return empty list instead of throwing to allow partial metadata return
                return new List<MeasureInfo>();
            }
            
            return measures;
        }

        private async Task<string> GetAccessTokenAsync()
        {
            try
            {
                var app = ConfidentialClientApplicationBuilder.Create(_config.ClientId)
                    .WithClientSecret(_config.ClientSecret)
                    .WithAuthority(new Uri($"https://login.microsoftonline.com/{_config.TenantId}"))
                    .Build();

                var scopes = new[] { "https://analysis.windows.net/powerbi/api/.default" };
                var result = await app.AcquireTokenForClient(scopes).ExecuteAsync();
                return result.AccessToken;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error getting access token");
                throw;
            }
        }
    }

    public class QueryRequest
    {
        public string Catalog { get; set; } = "";
        public string DaxQuery { get; set; } = "";
        public int? Port { get; set; }
    }

    public class TableInfo
    {
        public string Name { get; set; }
        public List<ColumnInfo> Columns { get; set; }
    }

    public class ColumnInfo
    {
        public string Name { get; set; }
        public string DataType { get; set; }
    }

    public class MeasureInfo
    {
        public string Name { get; set; }
        public string Caption { get; set; }
        public string TableName { get; set; }
        public string Expression { get; set; }
    }
}