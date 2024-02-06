using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Imeplements database provider for the PostgreSQL
    /// </summary>
    public class PostgreSqlProvider : BaseDatabaseProvider
    {
        #region Constructor

        public PostgreSqlProvider()
        {
            queryDatabaseTablesScript = "SELECT TABLE_NAME AS \"TableName\" FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(DatabaseServer server)
        {
            return PostgreSqlManager.GetConnection(server);
        }

        #endregion

        #region Data command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override IDataCommandResolver GetDataCommandResolver()
        {
            return PostgreSqlManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(DataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(PostgreSqlManager.CurrentDatabaseServerType);
        }

        #endregion

        #region Insert

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override Dictionary<string, TIdentity> InsertAndReturnIdentity<TIdentity>(DatabaseMultipleCommand command)
        {
            var dataCommandResolver = GetDataCommandResolver() as PostgreSqlDataCommandResolver;
            var statements = dataCommandResolver.GenerateDatabaseExecutionStatements(command);
            var identityDict = new Dictionary<string, TIdentity>();
            var dbConnection = command.Connection.DbConnection;
            foreach (var statement in statements)
            {
                var commandDefinition = GetCommandDefinition(command, statement);
                dbConnection.Execute(commandDefinition);
                if (commandDefinition.Parameters is DynamicParameters commandParameters && statement.Parameters != null)
                {
                    foreach (var parItem in statement.Parameters.Items)
                    {
                        if (parItem.Value.ParameterDirection == ParameterDirection.Output)
                        {
                            identityDict[parItem.Key] = commandParameters.Get<TIdentity>(parItem.Key);
                        }
                    }
                }
            }
            return identityDict;
        }

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override async Task<Dictionary<string, TIdentity>> InsertAndReturnIdentityAsync<TIdentity>(DatabaseMultipleCommand command)
        {
            var dataCommandResolver = GetDataCommandResolver() as PostgreSqlDataCommandResolver;
            var statements = dataCommandResolver.GenerateDatabaseExecutionStatements(command);
            var identityDict = new Dictionary<string, TIdentity>();
            var dbConnection = command.Connection.DbConnection;
            foreach (var statement in statements)
            {
                var commandDefinition = GetCommandDefinition(command, statement);
                await dbConnection.ExecuteAsync(commandDefinition).ConfigureAwait(false);
                if (commandDefinition.Parameters is DynamicParameters commandParameters && statement.Parameters != null)
                {
                    foreach (var parItem in statement.Parameters.Items)
                    {
                        if (parItem.Value.ParameterDirection == ParameterDirection.Output)
                        {
                            identityDict[parItem.Key.LSplit(dataCommandResolver.ParameterPrefix)[0]] = commandParameters.Get<TIdentity>(parItem.Key);
                        }
                    }
                }
            }
            return identityDict;
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override async Task BulkInsertAsync(DatabaseBulkInsertCommand command)
        {
            var server = command?.Connection?.DatabaseServer;
            ThrowHelper.ThrowArgNullIf(server == null, nameof(DatabaseBulkInsertCommand.Connection.DatabaseServer));
            var dataTable = command.DataTable;
            ThrowHelper.ThrowArgNullIf(dataTable == null, nameof(DatabaseBulkInsertCommand.DataTable));

            var postgreSqlBulkInsertOptions = command.BulkInsertionOptions as PostgreSqlBulkInsertionOptions;
            postgreSqlBulkInsertOptions ??= new PostgreSqlBulkInsertionOptions();
            var columnNames = new List<string>(dataTable.Columns.Count);
            foreach (DataColumn col in dataTable.Columns)
            {
                columnNames.Add(col.ColumnName);
            }
            var tableName = dataTable.TableName;
            var fields = columnNames;
            if (postgreSqlBulkInsertOptions.WrapWithQuotes)
            {
                tableName = PostgreSqlManager.WrapKeyword(tableName);
                fields = fields.Select(c => PostgreSqlManager.WrapKeyword(c)).ToList();
            }
            var copyString = $"COPY {tableName} ({string.Join(",", fields)}) FROM STDIN BINARY";
            using (var conn = new NpgsqlConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    using (var writer = conn.BeginBinaryImport(copyString))
                    {
                        foreach (DataRow row in dataTable.Rows)
                        {
                            writer.StartRow();
                            foreach (var col in columnNames)
                            {
                                await writer.WriteAsync(row[col]).ConfigureAwait(false);
                            }
                        }
                        await writer.CompleteAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override void BulkInsert(DatabaseBulkInsertCommand command)
        {
            var server = command?.Connection?.DatabaseServer;
            ThrowHelper.ThrowArgNullIf(server == null, nameof(DatabaseBulkInsertCommand.Connection.DatabaseServer));
            var dataTable = command.DataTable;
            ThrowHelper.ThrowArgNullIf(dataTable == null, nameof(DatabaseBulkInsertCommand.DataTable));

            var postgreSqlBulkInsertOptions = command.BulkInsertionOptions as PostgreSqlBulkInsertionOptions;
            postgreSqlBulkInsertOptions ??= new PostgreSqlBulkInsertionOptions();
            var columnNames = new List<string>(dataTable.Columns.Count);
            foreach (DataColumn col in dataTable.Columns)
            {
                columnNames.Add(col.ColumnName);
            }
            var tableName = dataTable.TableName;
            var fields = columnNames;
            if (postgreSqlBulkInsertOptions.WrapWithQuotes)
            {
                tableName = PostgreSqlManager.WrapKeyword(tableName);
                fields = fields.Select(c => PostgreSqlManager.WrapKeyword(c)).ToList();
            }
            var copyString = $"COPY {tableName} ({string.Join(",", fields)}) FROM STDIN BINARY";
            using (var conn = new NpgsqlConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    using (var writer = conn.BeginBinaryImport(copyString))
                    {
                        foreach (DataRow row in dataTable.Rows)
                        {
                            writer.StartRow();
                            foreach (var col in columnNames)
                            {
                                writer.Write(row[col]);
                            }
                        }
                        writer.Complete();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        #endregion
    }
}
