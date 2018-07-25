using ADODB;
using log4net;
using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Messaging;
using System.Web;

namespace YBB.DBUtils
{
    /// <summary>
    /// ADO���ӵ�һ�������ࡣ
    /// </summary>
    public class AdoHelper : IThreadInstance
    {
        private static ILog log = LogManager.GetLogger(typeof(AdoHelper));
        private string connectionStr = "";

        private List<Connection> dbs = new List<Connection>();

        private Connection _connection;

        /// <summary>
        ///
        /// </summary>
        public event EventHandler DbOpenedByUser;

        /// <summary>
        ///
        /// </summary>
        public event EventHandler DbClosedByUser;

        #region ���캯��

        public AdoHelper(string connectionString)
        {
            this.ConnectionString = connectionString;
        }

        #endregion ���캯��

        #region �߳�ʵ��

        /// <summary>
        /// ����AdoHelper���ࡣ
        /// </summary>
        /// <typeparam name="T">AdoNetHelper ������</typeparam>
        /// <param name="userName">���ݿ��û���</param>
        /// <param name="dbname">���ݿ�ʵ����</param>
        /// <returns></returns>
        public static AdoHelper ThreadInstance(string connectionString, DbProviderType provider)
        {
            return ThreadInstance(connectionString, provider, true);
        }

        /// <summary>
        /// ����AdoNetHelper���ࡣ
        /// </summary>
        /// <typeparam name="T">AdoNetHelper ������</typeparam>
        /// <param name="userName">���ݿ��û���</param>
        /// <param name="dbname">���ݿ�ʵ����</param>
        /// <param name="createIfNull">�����������ô����</param>
        /// <returns></returns>
        public static AdoHelper ThreadInstance(string connectionString, DbProviderType provider, bool createIfNull)
        {
            string CACHEKEY = string.Format("YBB.DBUtils.AdoNetHelper.ThreadInstance::{0}:", connectionString);
            AdoHelper db = null;
            if (HttpContext.Current != null)
            {
                db = HttpContext.Current.Items[CACHEKEY] as AdoHelper;
                if (db == null)
                {
                    if (!createIfNull)
                    {
                        return null;
                    }
                    db = new AdoHelper(connectionString);
                    HttpContext.Current.Items[CACHEKEY] = db;
                }
            }
            else
            {
                db = CallContext.GetData(CACHEKEY) as AdoHelper;
                if (db == null)
                {
                    if (!createIfNull)
                    {
                        return null;
                    }
                    db = new AdoHelper(connectionString);
                    CallContext.SetData(CACHEKEY, db);
                }
            }
            if (db == null)
            {
                return null;
            }
            return db;
        }

        #endregion �߳�ʵ��

        /// <summary>
        /// �����ַ�����
        /// </summary>
        public string ConnectionString
        {
            get
            {
                {
                    return this.connectionStr;
                }
            }
            set
            {
                connectionStr = value;
            }
        }

        /// <summary>
        /// ����һ���µ����ݿ����ӡ�
        /// </summary>
        /// <returns></returns>
        public Connection GetNewConnection()
        {
            Connection conn = new Connection();
            conn.ConnectionString = this.ConnectionString;
            conn.ConnectionTimeout = 10;
            conn.Open("", "", "", -1);
            OnDbOpenedByUser(conn);
            dbs.Add(conn);
            log.Debug(string.Format("new db connection created. {0}", conn.DefaultDatabase));
            return conn;
        }

        /// <summary>
        /// ��ȡһ�����ݿ����ӣ����֮ǰ�Ѿ���ȡ������ô����֮ǰ�Ķ��󣬷��ߴ���һ�����ӣ����֮ǰ�������Ѿ��رգ����´�����
        /// </summary>
        /// <returns></returns>
        public Connection Connection()
        {
            if (_connection == null)
            {
                _connection = GetNewConnection();
            }
            else
            {
                if (_connection.State != 1)
                {
                    _connection.Open("", "", "", -1);
                    OnDbOpenedByUser(_connection);
                }
            }
            return _connection;
        }

        /// <summary>
        ///
        /// </summary>
        public void GetOrBeginTrans()
        {
            if (_connection == null || _connection.State != 1)
            {
                throw new Exception(string.Format("database is null or not open.{0}", _connection));
            }
            else
            {
                _connection.BeginTrans();
            }
        }

        /// <summary>
        /// �ر�һ�����ݿ�����
        /// </summary>
        /// <param name="conn"></param>
        public void CloseDatabase(ADODB.Connection conn)
        {
            if (conn == null)
                return;
            int orgState = 0;
            try
            {
                orgState = conn.State;
                if (orgState == 1)
                {
                    conn.Close();
                    OnDbClosedByUser(conn);
                }
            }
            catch (Exception ex)
            {
                if (orgState == 1)
                {
                    log.Error(ex);
                }
                else
                {
                    log.Error(ex.Message);
                }
            }
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="conn"></param>
        protected void OnDbOpenedByUser(ADODB.Connection conn)
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
        protected void OnDbClosedByUser(ADODB.Connection conn)
        {
            if (this.DbClosedByUser != null)
            {
                this.DbClosedByUser(conn, new EventArgs());
            }
        }

        #region IDisposable ��Ա

        /// <summary>
        /// �ͷ���Դ�����ر�����GetNewAdoConnection,GetConnection�򿪵����ݿ����ӡ�
        /// </summary>
        public void Dispose()
        {
            this.CloseDatabase(this._connection);
            foreach (Connection con in this.dbs)
            {
                if (con != this._connection)
                    this.CloseDatabase(con);
            }
            this._connection = null;
        }

        #endregion IDisposable ��Ա
    }
}