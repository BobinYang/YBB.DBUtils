using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class OleDbHelper : AdoNetHelper
    {
        #region 构造函数



        /// <summary>
        ///
        /// </summary>
        public OleDbHelper(string connectionString, DbProviderType provider)
            : base(connectionString, provider)
        {
        }

        private OleDbHelper()
        {
        }

        #endregion 构造函数

        /// <summary>
        /// <see cref="OleDbCommandBuilder.DeriveParameters"/>
        /// </summary>
        /// <param name="cmd"></param>
        public override void DeriveParameters(System.Data.IDbCommand cmd)
        {
            if (cmd is OleDbCommand)
            {
                OleDbCommandBuilder.DeriveParameters(cmd as OleDbCommand);
            }
        }

        /// <summary>
        /// <see cref="OleDbFactory.Instance"/>
        /// </summary>
        //public override DbProviderFactory DbFactory
        //{
        //    get
        //    {
        //        return OleDbFactory.Instance;
        //    }
        //}

        /// <summary>
        /// DbType -> OleDbType 类型是否安全?
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="dbType"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public override DbParameter MakeParam(string ParamName, DbType dbType, Int32 Size)
        {
            OleDbParameter param = null;

            if (Size > 0)
                param = new OleDbParameter(ParamName, DbTypeConvert(dbType), Size);
            else
                param = new OleDbParameter(ParamName, DbTypeConvert(dbType));

            return param;
        }

        /// <summary>
        /// 将DbType转换成OracleType枚举，确保枚举类型之间转换的安全性
        /// 同时避免数据库字段类型统一修改带来的巨大修改量
        /// http://msdn.microsoft.com/en-us/library/yk72thhd%28VS.80%29.aspx
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private OleDbType DbTypeConvert(DbType type)
        {
            OleDbType oracleType = OleDbType.VarChar;
            string exception = String.Format("No mapping exists from DbType {0} to a known OracleType.", type);
            switch (type)
            {
                case DbType.AnsiString:
                    oracleType = OleDbType.VarChar;
                    break;

                case DbType.AnsiStringFixedLength:
                    oracleType = OleDbType.Char;
                    break;

                case DbType.Binary:
                    oracleType = OleDbType.Binary;
                    break;

                case DbType.Guid:
                    oracleType = OleDbType.Guid;
                    break;

                case DbType.Boolean:
                    oracleType = OleDbType.Boolean;
                    break;

                case DbType.Byte:
                    oracleType = OleDbType.UnsignedTinyInt;
                    break;

                case DbType.Currency:
                    oracleType = OleDbType.Currency;
                    break;

                case DbType.Decimal:
                    oracleType = OleDbType.Decimal;
                    break;

                case DbType.Int64:
                    oracleType = OleDbType.BigInt;
                    break;

                case DbType.Date:
                    oracleType = OleDbType.DBDate;
                    break;

                case DbType.DateTime:
                    oracleType = OleDbType.DBTimeStamp;
                    break;

                case DbType.Time:
                    oracleType = OleDbType.DBTime;
                    break;

                case DbType.Double:
                    oracleType = OleDbType.Double;
                    break;

                case DbType.Int16:
                    oracleType = OleDbType.SmallInt;
                    break;

                case DbType.Int32:
                    oracleType = OleDbType.Integer;
                    break;

                case DbType.Object:
                    oracleType = OleDbType.IUnknown;
                    break;

                case DbType.Single:
                    oracleType = OleDbType.Single;
                    break;

                case DbType.String:
                    oracleType = OleDbType.VarWChar;
                    break;

                case DbType.StringFixedLength:
                    oracleType = OleDbType.WChar;
                    break;

                default:
                    throw new ArgumentException(exception);
            }
            return oracleType;
        }
    }
}