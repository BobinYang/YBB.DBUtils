namespace YBB.DBUtils
{


    /// <summary>
    ///
    /// </summary>
    public enum ConnectTypes
    {
        /// <summary>
        ///
        /// </summary>
        ADO,

        /// <summary>
        ///
        /// </summary>
        ADONet
    }

    public enum DbProviderType : byte
    {
        Odbc,
        OleDb,
        SqlServer,

        Oracle_MS,
        Oracle_ManagedODP,
        MySql,
        SQLite,
        Firebird,
        PostgreSql,
        DB2,
        Informix,
        SqlServerCe
    }
}