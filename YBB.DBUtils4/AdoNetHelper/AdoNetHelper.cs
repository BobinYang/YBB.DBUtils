using log4net;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;
using System.Runtime.Remoting.Messaging;
using System.Web;

namespace YBB.DBUtils
{
    /// <summary>
    /// 抽象类 实现了ADONET常用方法。
    /// </summary>
    public abstract class AdoNetHelper : DbHelper, IThreadInstance
    {
        private static ILog log = LogManager.GetLogger(typeof(AdoNetHelper));

        private string connectionStr = "";
        private List<DbConnection> dbs = new List<DbConnection>();//所有连接列表

        #region 构造函数

        protected AdoNetHelper()
        {
        }

        protected AdoNetHelper(DbProviderType providerType)
        {
            DbFactory = ProviderFactory.GetDbProviderFactory(providerType);
            if (DbFactory == null)
            {
                throw new ArgumentException("Can't load DbProviderFactory for given value of providerType");
            }
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="providerType"></param>
        protected AdoNetHelper(string connectionString, DbProviderType providerType)
            : this(providerType)
        {
            this.connectionStr = connectionString;
        }

        #endregion 构造函数

        #region 创建线程缓存的AdoNetHelper子类实例。

        /// <summary>
        /// 根据内部数据，创建线程缓存的AdoNetHelper子类实例。
        /// </summary>
        /// <typeparam name="T">AdoNetHelper 的子类</typeparam>
        /// <param name="userName">数据库用户名</param>
        /// <param name="dbname">数据库实例名</param>
        /// <returns></returns>
        public static AdoNetHelper ThreadInstance(string connectionString, DbProviderType provider)
        {
            return ThreadInstance(connectionString, provider, true);
        }

        /// <summary>
        /// 根据连接字符串， 创建线程缓存的AdoNetHelper子类实例。
        /// </summary>
        /// <typeparam name="T">AdoNetHelper 的子类</typeparam>
        /// <param name="userName">数据库用户名</param>
        /// <param name="dbname">数据库实例名</param>
        /// <param name="createIfNull">如果不存在那么创建</param>
        /// <returns></returns>
        public static AdoNetHelper ThreadInstance(string connectionString, DbProviderType provider, bool createIfNull)
        {
            string CACHEKEY = string.Format("YBB.DBUtils.AdoNetHelper.ThreadInstance::{0}:{1}", connectionString, provider.ToString());
            AdoNetHelper db = null;
            if (HttpContext.Current != null)
            {
                db = HttpContext.Current.Items[CACHEKEY] as AdoNetHelper;
                if (db == null)
                {
                    if (!createIfNull)
                    {
                        return null;
                    }
                    db = CreateInstance(connectionString, provider);
                    HttpContext.Current.Items[CACHEKEY] = db;
                }
            }
            else
            {
                db = CallContext.GetData(CACHEKEY) as AdoNetHelper;
                if (db == null)
                {
                    if (!createIfNull)
                    {
                        return null;
                    }
                    db = CreateInstance(connectionString, provider);
                    CallContext.SetData(CACHEKEY, db);
                }
            }
            if (db == null)
            {
                return null;
            }
            return db;
        }

        /// <summary>
        /// 根据连接字符串， 创建AdoNetHelper子类实例。
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="dbname"></param>
        /// <returns></returns>
        protected static AdoNetHelper CreateInstance(string connectionString, DbProviderType provider)
        {
            AdoNetHelper helper = null;
            switch (provider)
            {
                case DbProviderType.Odbc:
                    helper = new OdbcHelper(connectionString, provider);
                    break;

                case DbProviderType.OleDb:
                    helper = new OleDbHelper(connectionString, provider);
                    break;

                case DbProviderType.Oracle_MS:
                    helper = new OracleHelper(connectionString, provider);
                    break;

                case DbProviderType.Oracle_ManagedODP:
                    helper = new OracleHelper_ManagedODP(connectionString, provider);
                    break;

                case DbProviderType.SqlServer:
                    helper = new SqlHelper(connectionString, provider);
                    break;
            }
            return helper;
        }

        #endregion 创建线程缓存的AdoNetHelper子类实例。

        public event EventHandler DbOpenedByUser;

        public event EventHandler DbClosedByUser;

        /// <summary>
        /// 连接字符串。
        /// </summary>
        public override string ConnectionString
        {
            get
            {
                return this.connectionStr;
            }
            set
            {
                connectionStr = value;
            }
        }

        /// <summary>
        ///
        /// </summary>
        public override DbProviderFactory DbFactory
        {
            get;
            set;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="cmd"></param>
        public abstract override void DeriveParameters(IDbCommand cmd);

        /// <summary>
        ///
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="dbtype"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public abstract override DbParameter MakeParam(string ParamName, DbType dbtype, int Size);

        /// <summary>
        ///
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public DbParameter MakeInParam(string ParamName, object value)
        {
            System.Data.DbType Dbtype = DbType.String;
            if (value is DateTime)
            {
                Dbtype = DbType.Date;
            }
            else if (value is Int64)
            {
                Dbtype = DbType.Int64;
            }
            else if (value is Int32)
            {
                Dbtype = DbType.Int32;
            }
            else if (value is Int16)
            {
                Dbtype = DbType.Int16;
            }
            else if (value is string)
            {
                Dbtype = DbType.String;
            }
            else if (value is float)
            {
                Dbtype = DbType.Decimal;
            }
            else if (value is double)
            {
                Dbtype = DbType.Double;
            }
            else if (value is decimal)
            {
                Dbtype = DbType.Decimal;
            }
            else if (value.GetType().IsEnum)
            {
                Dbtype = DbType.String;
                value = value.ToString();
            }
            return MakeParam(ParamName, Dbtype, 0, ParameterDirection.Input, value);
        }

        #region ADO.net辅助方法

        /// <summary>
        /// 始终获取一个新的数据库连接。
        /// </summary>
        /// <returns></returns>
        public DbConnection OpenNewConnection()
        {
            DbConnection conn = this.DbFactory.CreateConnection();
            conn.ConnectionString = this.ConnectionString;
            conn.Open();
            OnDbOpenedByUser(conn);
            dbs.Add(conn);
            log.Debug(string.Format("new connection created: {0}", conn.Database));
            return conn;
        }

        /// <summary>
        /// 启动Connection()的新事务
        /// </summary>
        /// <param name="newTrans">是否创建一个新事务</param>
        /// <returns></returns>
        public DbTransaction BeginNewTrans(DbConnection _connInstance)
        {
            DbTransaction _transInstance = null;

            if (_connInstance == null)
            {
                throw new Exception(string.Format("database is null{0}", _connInstance));
            }
            else if (_connInstance.State != ConnectionState.Open)
            {
                throw new Exception(string.Format("database not open.{0}", _connInstance));
            }
            else
            {
                if (_connInstance is OdbcConnection)
                {
                    _transInstance = _connInstance.BeginTransaction(IsolationLevel.ReadCommitted);//是否需要指定ReadCommitted？？？
                }
                else
                {
                    _transInstance = _connInstance.BeginTransaction();
                }
            }
            log.Debug(string.Format("new transaction created: {0}", _connInstance.Database));
            return _transInstance;
        }

        /// <summary>
        /// 获取或者创建基于Connection()的命令
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="_transInstance"></param>
        /// <returns></returns>
        public DbCommand CreateNewCommand(string sql, DbTransaction _transInstance)
        {
            DbCommand _commInstance = null;
            DbConnection _connInstance = _transInstance.Connection;
            _commInstance = CreateNewCommand(sql, _connInstance);
            if (_transInstance != null)
            {
                _commInstance.Transaction = _transInstance;
            }
            return _commInstance;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="_connInstance"></param>
        /// <returns></returns>
        public DbCommand CreateNewCommand(string sql, DbConnection _connInstance)
        {
            DbCommand _commInstance = null;
            if (_connInstance == null)
            {
                throw new Exception(string.Format("database is null{0}", _connInstance));
            }
            else if (_connInstance.State != ConnectionState.Open)
            {
                throw new Exception(string.Format("database not open.{0}", _connInstance));
            }
            else
            {
                _commInstance = this.DbFactory.CreateCommand();
                if (_commInstance is Oracle.ManagedDataAccess.Client.OracleCommand)
                {
                    (_commInstance as Oracle.ManagedDataAccess.Client.OracleCommand).BindByName = true;
                }
                _commInstance.CommandText = sql;
                _commInstance.Connection = _connInstance;
            }
            return _commInstance;
        }

        /// <summary>
        /// 执行命令，返回一个DataTable
        /// </summary>
        /// <param name="_commInstance"></param>
        /// <returns></returns>
        public DataTable ExecuteDataset(DbCommand _commInstance)
        {
            if (_commInstance.Connection == null)
            {
                throw new ArgumentNullException("connection");
            }
            if (_commInstance == null)
            {
                throw new ArgumentNullException("command");
            }
            using (DbDataAdapter adapter = Factory.CreateDataAdapter())
            {
                adapter.SelectCommand = _commInstance;
                DataSet dataSet = new DataSet();
                adapter.Fill(dataSet);
                _commInstance.Parameters.Clear();
                return dataSet.Tables[0];
            }
        }

        /// <summary>
        /// 关闭一个数据库连接。
        /// </summary>
        /// <param name="conn"></param>
        public void CloseConnection(DbConnection conn)
        {
            try
            {
                if (conn == null)
                    return;
                if (conn.State != ConnectionState.Closed)
                {
                    conn.Close();
                    OnDbClosedByUser(conn);
                    log.Debug(string.Format("closed conn.{0}", conn.Database));
                }
                else
                {
                    log.Debug(string.Format("data closed before call this method.{0}", conn.Database));
                }
                conn.Dispose();
            }
            catch (Exception ex)
            {
                log.Error(ex);
            }
        }

        #endregion ADO.net辅助方法

        /// <summary>
        ///
        /// </summary>
        /// <param name="conn"></param>
        public void OnDbOpenedByUser(DbConnection conn)
        {
            if (this.DbOpenedByUser != null)
            {
                this.DbOpenedByUser(conn, new EventArgs());
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="conn"></param>
        public void OnDbClosedByUser(DbConnection conn)
        {
            if (this.DbClosedByUser != null)
            {
                this.DbClosedByUser(conn, new EventArgs());
            }
        }

        /// <summary>
        /// 关闭所有数据库连接
        /// </summary>
        public virtual void Dispose()
        {
            foreach (DbConnection conn in this.dbs)
            {
                this.CloseConnection(conn);
            }
            dbs.Clear();
        }
    }
}