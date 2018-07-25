using System;
using System.Collections.Generic;
using System.Data.Common;

namespace YBB.DBUtils
{
    public class ProviderFactory
    {
        private static Dictionary<DbProviderType, string> providerInvariantNames = new Dictionary<DbProviderType, string>();
        private static Dictionary<DbProviderType, DbProviderFactory> providerFactoies = new Dictionary<DbProviderType, DbProviderFactory>(20);

        static ProviderFactory()
        {
            providerInvariantNames.Add(DbProviderType.OleDb, "System.Data.OleDb");
            providerInvariantNames.Add(DbProviderType.Odbc, "System.Data.Odbc");
            providerInvariantNames.Add(DbProviderType.SqlServer, "System.Data.SqlClient");
            providerInvariantNames.Add(DbProviderType.Oracle_MS, "System.Data.OracleClient");
            providerInvariantNames.Add(DbProviderType.Oracle_ManagedODP, "Oracle.ManagedDataAccess.Client");
            providerInvariantNames.Add(DbProviderType.MySql, "MySql.Data.MySqlClient");
            providerInvariantNames.Add(DbProviderType.SQLite, "System.Data.SQLite");
            providerInvariantNames.Add(DbProviderType.Firebird, "FirebirdSql.Data.Firebird");
            providerInvariantNames.Add(DbProviderType.PostgreSql, "Npgsql");
            providerInvariantNames.Add(DbProviderType.DB2, "IBM.Data.DB2.iSeries");
            providerInvariantNames.Add(DbProviderType.Informix, "IBM.Data.Informix");
            providerInvariantNames.Add(DbProviderType.SqlServerCe, "System.Data.SqlServerCe");
        }

        public static string GetProviderInvariantName(DbProviderType providerType)
        {
            return providerInvariantNames[providerType];
        }

        public static DbProviderFactory GetDbProviderFactory(DbProviderType providerType)
        {
            if (!providerFactoies.ContainsKey(providerType))
            {
                lock (providerFactoies)
                {
                    if (!providerFactoies.ContainsKey(providerType))
                    {
                        providerFactoies.Add(providerType, ImportDbProviderFactory(providerType));
                    }
                }
            }
            return providerFactoies[providerType];
        }

        private static DbProviderFactory ImportDbProviderFactory(DbProviderType providerType)
        {
            string providerName = providerInvariantNames[providerType];
            DbProviderFactory factory = null;
            try
            {
                factory = DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException e)
            {
                factory = null;
            }
            return factory;
        }
    }
}