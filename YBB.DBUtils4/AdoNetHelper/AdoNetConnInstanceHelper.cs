using log4net;
using System;
using System.Data.Common;
using System.Runtime.Remoting.Messaging;
using System.Web;

namespace YBB.DBUtils
{
    /// <summary>
    ///同一线程中，无论实例化多少个AdoNetConnInstanceHelper，Conn都是同一个
    /// </summary>
    public class AdoNetConnInstanceHelper : IDisposable
    {
        private const string CACHEDORACLEKEY = "YBB.DBUtils.AdoNetConnInstanceHelper.Conn";
        private static ILog log = LogManager.GetLogger(typeof(AdoNetConnInstanceHelper));

        /// <summary>
        ///
        /// </summary>
        /// <param name="helper"></param>
        public AdoNetConnInstanceHelper(AdoNetHelper helper)//
        {
            this.Helper = helper;
        }

        /// <summary>
        ///
        /// </summary>
        public AdoNetHelper Helper { set; get; }

        /// <summary>
        ///打开新连接，或获取线程缓存连接。
        /// </summary>
        public DbConnection Conn
        {
            get
            {
                DbConnection conn;
                if (HttpContext.Current != null)
                {
                    conn = HttpContext.Current.Items[CACHEDORACLEKEY] as DbConnection;
                    if (conn == null)
                    {
                        conn = Helper.OpenNewConnection();
                        HttpContext.Current.Items[CACHEDORACLEKEY] = conn;
                    }
                    if (conn.State != System.Data.ConnectionState.Open)
                    {
                        conn.Open();
                        Helper.OnDbOpenedByUser(conn);
                    }
                }
                else
                {
                    conn = CallContext.GetData(CACHEDORACLEKEY) as DbConnection;
                    if (conn == null)
                    {
                        conn = Helper.OpenNewConnection();
                        CallContext.SetData(CACHEDORACLEKEY, conn);
                    }
                    if (conn.State != System.Data.ConnectionState.Open)
                    {
                        conn.Open();
                        Helper.OnDbOpenedByUser(conn);
                    }
                }
                return conn;
            }
        }

        /// <summary>
        ///
        /// </summary>
        public void CloseOracle()
        {
            try
            {
                if (HttpContext.Current != null)
                {
                    DbConnection conn = HttpContext.Current.Items[CACHEDORACLEKEY] as DbConnection;
                    if (conn != null)
                    {
                        if (conn.State == System.Data.ConnectionState.Open)
                        {
                            Helper.CloseConnection(conn);
                            conn.Dispose();
                        }
                        HttpContext.Current.Items[CACHEDORACLEKEY] = null;
                    }
                }
                else
                {
                    DbConnection conn = CallContext.GetData(CACHEDORACLEKEY) as DbConnection;
                    if (conn != null)
                    {
                        if (conn.State == System.Data.ConnectionState.Open)
                        {
                            Helper.CloseConnection(conn);
                            conn.Dispose();
                        }
                        CallContext.SetData(CACHEDORACLEKEY, null);
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        ///
        /// </summary>
        public void Dispose()
        {
            CloseOracle();
        }
    }
}