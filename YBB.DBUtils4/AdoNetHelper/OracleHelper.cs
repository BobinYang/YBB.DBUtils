using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class OracleHelper : AdoNetHelper
    {
        #region 构造函数



        public OracleHelper(string connectionString, DbProviderType provider)
            : base(connectionString, provider)
        {
        }

        private OracleHelper()
        {
        }

        #endregion 构造函数

        #region AdoNetHelper 成员

        /// <summary>
        /// <see cref="OracleCommandBuilder.DeriveParameters"/>
        /// </summary>
        /// <param name="cmd"></param>
        public override void DeriveParameters(System.Data.IDbCommand cmd)
        {
            if (cmd is OracleCommand)
            {
                OracleCommandBuilder.DeriveParameters(cmd as OracleCommand);
            }
        }

        /// <summary>
        ///  return OracleClientFactory.Instance;
        /// </summary>
        //public override DbProviderFactory DbFactory
        //{
        //    get
        //    {
        //        return OracleClientFactory.Instance;
        //    }
        //}

        /// <summary>
        /// 构造<see cref="OracleParameter"/>对象
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="Dbtype"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public override DbParameter MakeParam(string ParamName, System.Data.DbType Dbtype, int Size)
        {
            if (Size > 0)
            {
                return new OracleParameter(ParamName, DbTypeConvert(Dbtype), Size);
            }
            return new OracleParameter(ParamName, DbTypeConvert(Dbtype));
        }

        /// <summary>
        /// 将DbType转换成OracleType枚举，确保枚举类型之间转换的安全性
        /// 同时避免数据库字段类型统一修改带来的巨大修改量
        /// http://msdn.microsoft.com/en-us/library/yk72thhd%28VS.80%29.aspx
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private OracleType DbTypeConvert(DbType type)
        {
            OracleType oracleType = OracleType.VarChar;
            string exception = String.Format("No mapping exists from DbType {0} to a known OracleType.", type);
            switch (type)
            {
                case DbType.AnsiString:
                    oracleType = OracleType.VarChar;
                    break;

                case DbType.AnsiStringFixedLength:
                    oracleType = OracleType.Char;
                    break;

                case DbType.Binary:
                case DbType.Guid:
                    oracleType = OracleType.Raw;
                    break;

                case DbType.Boolean:
                case DbType.Byte:
                    oracleType = OracleType.Byte;
                    break;

                case DbType.Currency:
                case DbType.Decimal:
                case DbType.Int64:
                    oracleType = OracleType.Number;
                    break;

                case DbType.Date:
                case DbType.DateTime:
                case DbType.Time:
                    oracleType = OracleType.DateTime;
                    break;

                case DbType.Double:
                    oracleType = OracleType.Double;
                    break;

                case DbType.Int16:
                    oracleType = OracleType.Int16;
                    break;

                case DbType.Int32:
                    oracleType = OracleType.Int32;
                    break;

                case DbType.Object:
                    oracleType = OracleType.Blob;
                    break;

                case DbType.Single:
                    oracleType = OracleType.Float;
                    break;

                case DbType.String:
                    oracleType = OracleType.NVarChar;
                    break;

                case DbType.StringFixedLength:
                    oracleType = OracleType.NChar;
                    break;

                default:
                    throw new ArgumentException(exception);
            }
            return oracleType;
        }

        #endregion AdoNetHelper 成员

    }
}