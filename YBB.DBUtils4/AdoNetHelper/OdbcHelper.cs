using System;
using System.Data;
using System.Data.Common;
using System.Data.Odbc;

namespace YBB.DBUtils
{
    /// <summary>
    ///
    /// </summary>
    public class OdbcHelper : AdoNetHelper
    {
        public OdbcHelper(string connectionString, DbProviderType provider)
            : base(connectionString, provider)
        {
        }

        private OdbcHelper()
        {
        }

        #region IDbProvider 成员

        /// <summary>
        /// <see cref="OdbcFactory.Instance"/>
        /// </summary>
        public override DbProviderFactory DbFactory
        {
            get
            {
                return OdbcFactory.Instance;
            }
        }

        /// <summary>
        /// <see cref="OdbcCommandBuilder.DeriveParameters"/>
        /// </summary>
        /// <param name="cmd"></param>
        public override void DeriveParameters(System.Data.IDbCommand cmd)
        {
            if (cmd is OdbcCommand)
            {
                OdbcCommandBuilder.DeriveParameters(cmd as OdbcCommand);
            }
        }

        /// <summary>
        /// DbType -> OdbcType 类型是否安全?
        /// </summary>
        /// <param name="ParamName"></param>
        /// <param name="dbType"></param>
        /// <param name="Size"></param>
        /// <returns></returns>
        public override DbParameter MakeParam(string ParamName, DbType dbType, Int32 Size)
        {
            OdbcParameter param;

            if (Size > 0)
                param = new OdbcParameter(ParamName, DbTypeConvert(dbType), Size);
            else
                param = new OdbcParameter(ParamName, DbTypeConvert(dbType));

            return param;
        }

        /// <summary>
        /// 将DbType转换成OdbcType枚举，确保枚举类型之间转换的安全性
        /// 同时避免数据库字段类型统一修改带来的巨大修改量
        /// http://msdn.microsoft.com/en-us/library/yk72thhd%28VS.80%29.aspx
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private OdbcType DbTypeConvert(DbType type)
        {
            OdbcType oracleType = OdbcType.NVarChar;
            string exception = String.Format("No mapping exists from DbType {0} to a known OracleType.", type);
            switch (type)
            {
                case DbType.AnsiString:
                    oracleType = OdbcType.VarChar;
                    break;

                case DbType.AnsiStringFixedLength:
                    oracleType = OdbcType.Char;
                    break;

                case DbType.String:
                    oracleType = OdbcType.NVarChar;
                    break;

                case DbType.StringFixedLength:
                    oracleType = OdbcType.NChar;
                    break;

                case DbType.Binary:
                    oracleType = OdbcType.Binary;
                    break;

                case DbType.Guid:
                    oracleType = OdbcType.UniqueIdentifier;
                    break;

                case DbType.Boolean:
                    oracleType = OdbcType.Bit;
                    break;

                case DbType.Byte:
                    oracleType = OdbcType.TinyInt;
                    break;

                case DbType.Date:
                    oracleType = OdbcType.Date;
                    break;

                case DbType.DateTime:
                    oracleType = OdbcType.DateTime;
                    break;

                case DbType.Time:
                    oracleType = OdbcType.Time;
                    break;

                case DbType.Int16:
                    oracleType = OdbcType.SmallInt;
                    break;

                case DbType.Int32:
                    oracleType = OdbcType.Int;
                    break;

                case DbType.Int64:
                    oracleType = OdbcType.BigInt;
                    break;

                case DbType.Single:
                case DbType.Currency:
                    oracleType = OdbcType.Numeric;
                    break;

                case DbType.Decimal:
                    oracleType = OdbcType.Decimal;
                    break;

                case DbType.Double:
                    oracleType = OdbcType.Double;
                    break;

                case DbType.Object:
                    throw new ArgumentException(exception);
                default:
                    throw new ArgumentException(exception);
            }
            return oracleType;
        }

        #endregion IDbProvider 成员
    }
}