using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

using Npgsql;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Exceptions;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Imeplements database provider for the PostgreSQL
    /// </summary>
    public class SixnetPostgreSqlProvider : SixnetBaseDatabaseProvider
    {
        #region Constructor

        public SixnetPostgreSqlProvider(Action<SixnetPostgreSqlOptions> configure = null)
        {
            var postgreSqlOptions = new SixnetPostgreSqlOptions();
            configure?.Invoke(postgreSqlOptions);
            AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior", postgreSqlOptions.EnableLegacyTimestampBehavior);
            AppContext.SetSwitch("Npgsql.DisableDateTimeInfinityConversions", postgreSqlOptions.DisableDateTimeInfinityConversions);
            queryTablesScript = "SELECT T.oid AS \"ID\", T.relname AS \"NAME\", T.relnamespace AS \"SCHEMAID\", S.nspname AS \"SCHEMANAME\" FROM pg_class T INNER JOIN pg_namespace S ON T.relnamespace = S.oid WHERE T.relkind = 'r' AND S.nspname NOT IN ('pg_catalog','information_schema') ORDER BY S.nspname, T.relname;";
        }

        #endregion

        #region Connection

        /// <summary>
        /// Get database connection
        /// </summary>
        /// <param name="server">Database server</param>
        /// <returns></returns>
        public override IDbConnection GetDbConnection(SixnetDatabaseServer server)
        {
            return SixnetPostgreSqlManager.GetConnection(server);
        }

        /// <summary>
        /// Get db connection meta
        /// </summary>
        /// <param name="connection"></param>
        /// <returns></returns>
        public override SixnetDatabaseConnectionMeta GetDbConnectionMeta(IDbConnection connection)
        {
            var sqlBuilder = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
            return new SixnetDatabaseConnectionMeta()
            {
                UserName = sqlBuilder.Username,
                Password = sqlBuilder.Password,
                DataSource = sqlBuilder.Host,
                DatabaseName = sqlBuilder.Database,
            };
        }

        #endregion

        #region Data command resolver

        /// <summary>
        /// Get data command resolver
        /// </summary>
        /// <returns></returns>
        protected override ISixnetDataCommandResolver GetDataCommandResolver(SixnetDatabaseCommand command)
        {
            return SixnetPostgreSqlManager.GetCommandResolver();
        }

        #endregion

        #region Parameter

        /// <summary>
        /// Convert data command parametes
        /// </summary>
        /// <param name="parameters">Data command parameters</param>
        /// <returns></returns>
        protected override DynamicParameters ConvertDataCommandParameters(SixnetDatabaseCommand command, SixnetDataCommandParameters parameters)
        {
            return parameters?.ConvertToDynamicParameters(SixnetPostgreSqlManager.GetCommandResolver().DatabaseType);
        }

        #endregion

        #region Insert

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override Dictionary<string, TIdentity> InsertAndReturnIdentity<TIdentity>(SixnetMultipleDatabaseCommand command)
        {
            try
            {
                var dataCommandResolver = GetDataCommandResolver(command) as SixnetPostgreSqlDataCommandResolver;
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
            catch (Exception ex)
            {
                throw GetSqlException(ex);
            }
        }

        /// <summary>
        /// Insert data and return auto identities
        /// </summary>
        /// <param name="command">Database multiple command</param>
        /// <returns>Added data identities,Key: command id, Value: identity value</returns>
        public override async Task<Dictionary<string, TIdentity>> InsertAndReturnIdentityAsync<TIdentity>(SixnetMultipleDatabaseCommand command)
        {
            try
            {
                var dataCommandResolver = GetDataCommandResolver(command) as SixnetPostgreSqlDataCommandResolver;
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
            catch (Exception ex)
            {
                throw GetSqlException(ex);
            }
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override async Task BulkInsertAsync(SixnetBulkInsertDatabaseCommand command)
        {
            try
            {
                var conn = command.Connection.DbConnection as NpgsqlConnection;
                var dataTable = command.DataTable;
                SixnetDirectThrower.ThrowArgNullIf(dataTable == null, nameof(SixnetBulkInsertDatabaseCommand.DataTable));
                var postgreSqlBulkInsertOptions = command.BulkInsertionOptions as SixnetPostgreSqlBulkInsertionOptions;
                postgreSqlBulkInsertOptions ??= new SixnetPostgreSqlBulkInsertionOptions();
                var columnNames = new List<string>(dataTable.Columns.Count);
                foreach (DataColumn col in dataTable.Columns)
                {
                    columnNames.Add(col.ColumnName);
                }
                var tableName = dataTable.TableName;
                var fields = columnNames;
                var postgresqlResolver = SixnetPostgreSqlManager.GetCommandResolver();
                if (postgreSqlBulkInsertOptions.WrapWithQuotes)
                {
                    tableName = postgresqlResolver.FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(tableName, SixnetDatabaseObjectType.Table));
                    fields = fields.Select(c => postgresqlResolver.FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(c, SixnetDatabaseObjectType.Column))).ToList();
                }
                else
                {
                    tableName = postgresqlResolver.GetObjectFullName(postgresqlResolver.FormatObjectName(SixnetDatabaseObjectName.Create(tableName, SixnetDatabaseObjectType.Table)));
                    fields = fields.Select(c => postgresqlResolver.GetObjectFullName(postgresqlResolver.FormatObjectName(SixnetDatabaseObjectName.Create(c, SixnetDatabaseObjectType.Column)))).ToList();
                }
                var copyString = $"COPY {tableName} ({string.Join(",", fields)}) FROM STDIN BINARY";

                using (var writer = await conn.BeginBinaryImportAsync(copyString).ConfigureAwait(false))
                {
                    foreach (DataRow row in dataTable.Rows)
                    {
                        await writer.StartRowAsync().ConfigureAwait(false);
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
                throw GetSqlException(ex);
            }
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public override void BulkInsert(SixnetBulkInsertDatabaseCommand command)
        {
            try
            {
                var conn = command.Connection.DbConnection as NpgsqlConnection;
                var dataTable = command.DataTable;
                SixnetDirectThrower.ThrowArgNullIf(dataTable == null, nameof(SixnetBulkInsertDatabaseCommand.DataTable));
                var postgreSqlBulkInsertOptions = command.BulkInsertionOptions as SixnetPostgreSqlBulkInsertionOptions;
                postgreSqlBulkInsertOptions ??= new SixnetPostgreSqlBulkInsertionOptions();
                var columnNames = new List<string>(dataTable.Columns.Count);
                foreach (DataColumn col in dataTable.Columns)
                {
                    columnNames.Add(col.ColumnName);
                }
                var tableName = dataTable.TableName;
                var fields = columnNames;
                var postgresqlResolver = SixnetPostgreSqlManager.GetCommandResolver();
                if (postgreSqlBulkInsertOptions.WrapWithQuotes)
                {
                    tableName = postgresqlResolver.FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(tableName, SixnetDatabaseObjectType.Table));
                    fields = fields.Select(c => postgresqlResolver.FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(c, SixnetDatabaseObjectType.Column))).ToList();
                }
                else
                {
                    tableName = postgresqlResolver.GetObjectFullName(postgresqlResolver.FormatObjectName(SixnetDatabaseObjectName.Create(tableName, SixnetDatabaseObjectType.Table)));
                    fields = fields.Select(c => postgresqlResolver.GetObjectFullName(postgresqlResolver.FormatObjectName(SixnetDatabaseObjectName.Create(c, SixnetDatabaseObjectType.Column)))).ToList();
                }
                var copyString = $"COPY {tableName} ({string.Join(",", fields)}) FROM STDIN BINARY";

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
                throw GetSqlException(ex);
            }
        }

        #endregion

        #region Get exception

        protected override Exception GetSqlException(Exception ex)
        {
            if (ex is NpgsqlException sqlException)
            {
                switch (sqlException.ErrorCode)
                {
                    case 23505:
                        return new SixnetSqlAlreadExistsException(sqlException.Message, sqlException);
                }
            }
            return ex;
        }

        #endregion
    }
}
