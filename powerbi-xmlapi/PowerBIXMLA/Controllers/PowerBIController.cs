using Microsoft.AspNetCore.Mvc;
using Microsoft.AnalysisServices.AdomdClient;
using Microsoft.Identity.Client;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Linq;

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
                
                using var connection = new AdomdConnection(connectionString);
                
                try {
                    var propertyInfo = typeof(AdomdConnection).GetProperty("AccessToken");
                    if (propertyInfo != null)
                    {
                        propertyInfo.SetValue(connection, accessToken);
                    }
                } catch (Exception ex) {
                    _logger.LogWarning($"Could not set AccessToken directly: {ex.Message}");
                    connection.ConnectionString = $"{connectionString}Password={accessToken};";
                }

                connection.Open();
                
                // Get tables and their columns
                var tablesAndColumns = GetTablesAndColumns(connection);
                
                // Get measures
                var measures = GetMeasures(connection);
                
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
            
            // DMV query to get tables and their columns
            string query = @"
                SELECT 
                    [TABLE_NAME] as TableName,
                    [COLUMN_NAME] as ColumnName,
                    [DATA_TYPE] as DataType 
                FROM $SYSTEM.DBSCHEMA_COLUMNS 
                ORDER BY TableName, ColumnName";
            
            using var command = new AdomdCommand(query, connection);
            using var reader = command.ExecuteReader();
            
            var currentTable = "";
            TableInfo tableInfo = null;
            
            while (reader.Read())
            {
                var tableName = reader["TableName"].ToString();
                var columnName = reader["ColumnName"].ToString();
                var dataType = reader["DataType"].ToString();
                
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
            
            return tableInfos;
        }

        private List<MeasureInfo> GetMeasures(AdomdConnection connection)
        {
            var measures = new List<MeasureInfo>();
            
            // DMV query to get measures
            string query = @"
                SELECT 
                    [MEASURE_NAME] as MeasureName,
                    [MEASURE_CAPTION] as MeasureCaption,
                    [MEASURE_AGGREGATOR] as AggregatorType,
                    [TABLE_NAME] as TableName
                FROM $SYSTEM.MDSCHEMA_MEASURES
                WHERE MEASURE_IS_VISIBLE";
            
            using var command = new AdomdCommand(query, connection);
            using var reader = command.ExecuteReader();
            
            while (reader.Read())
            {
                measures.Add(new MeasureInfo
                {
                    Name = reader["MeasureName"].ToString(),
                    Caption = reader["MeasureCaption"].ToString(),
                    TableName = reader["TableName"].ToString(),
                    AggregatorType = reader["AggregatorType"].ToString()
                });
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
        public string AggregatorType { get; set; }
    }
}