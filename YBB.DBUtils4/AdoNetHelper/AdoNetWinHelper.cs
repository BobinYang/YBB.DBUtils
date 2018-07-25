using System;
using System.Data;
using System.Data.Common;
using log4net;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class AdoNetWinHelper : IDisposable
    {
        private static ILog log = LogManager.GetLogger(typeof(AdoNetWinHelper));

        #region 内部连接管理

        private DbConnection _connInstance;

        public AdoNetWinHelper(AdoNetHelper helper)//实例化多个AdoNetConnInstanceHelper，Conn属性各属于自己的AdoNetConnInstanceHelper。
        {
            this.Helper = helper;
        }

        public AdoNetHelper Helper { set; get; }

        public DbConnection Conn
        {
            get
            {
                if (_connInstance == null)
                {
                    _connInstance = Helper.OpenNewConnection();
                }
                else
                {
                    //其他状态不考虑了,,,
                    if (_connInstance.State != ConnectionState.Open)
                    {
                        log.Debug(string.Format("connection reopen {0}", _connInstance.Database));
                        _connInstance.Open();
                        Helper.OnDbOpenedByUser(_connInstance);
                    }
                }
                return _connInstance;
            }
        }

        /// <summary>
        /// 释放所有数据库的连接。包括：GetNewConnection(),Connection()创建的数据库连接。
        /// </summary>
        public virtual void Dispose()
        {
            Helper.CloseConnection(_connInstance);
            this._connInstance = null;
        }

        #endregion 内部连接管理
    }
}