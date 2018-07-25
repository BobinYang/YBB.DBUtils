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
    /// ������ ʵ����ADONET���÷�����
    /// </summary>
    public abstract class AdoNetHelper : DbHelper, IThreadInstance
    {
        private static ILog log = LogManager.GetLogger(typeof(AdoNetHelper));

        private string connectionStr = "";
        private List<DbConnection> dbs = new List<DbConnection>();//���������б�

        #region ���캯��

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
        /// ���캯��
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="providerType"></param>
        protected AdoNetHelper(string connectionString, DbProviderType providerType)
            : this(providerType)
        {
            this.connectionStr = connectionString;
        }

        #endregion ���캯��

        #region �����̻߳����AdoNetHelper����ʵ����

        /// <summary>
        /// �����ڲ����ݣ������̻߳����AdoNetHelper����ʵ����
        /// </summary>
        /// <typeparam name="T">AdoNetHelper ������</typeparam>
        /// <param name="userName">���ݿ��û���</param>
        /// <param name="dbname">���ݿ�ʵ����</param>
        /// <returns></returns>
        public static AdoNetHelper ThreadInstance(string connectionString, DbProviderType provider)
        {
            return ThreadInstance(connectionString, provider, true);
        }

        /// <summary>
        /// ���������ַ����� �����̻߳����AdoNetHelper����ʵ����
        /// </summary>
        /// <typeparam name="T">AdoNetHelper ������</typeparam>
        /// <param name="userName">���ݿ��û���</param>
        /// <param name="dbname">���ݿ�ʵ����</param>
        /// <param name="createIfNull">�����������ô����</param>
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
        /// ���������ַ����� ����AdoNetHelper����ʵ����
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

        #endregion �����̻߳����AdoNetHelper����ʵ����

        public event EventHandler DbOpenedByUser;

        public event EventHandler DbClosedByUser;

        /// <summary>
        /// �����ַ�����
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

        #region ADO.net��������

        /// <summary>
        /// ʼ�ջ�ȡһ���µ����ݿ����ӡ�
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
        /// ����Connection()��������
        /// </summary>
        /// <param name="newTrans">�Ƿ񴴽�һ��������</param>
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
                    _transInstance = _connInstance.BeginTransaction(IsolationLevel.ReadCommitted);//�Ƿ���Ҫָ��ReadCommitted������
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
        /// ��ȡ���ߴ�������Connection()������
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
        /// ִ���������һ��DataTable
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
        /// �ر�һ�����ݿ����ӡ�
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

        #endregion ADO.net��������

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
        /// �ر��������ݿ�����
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