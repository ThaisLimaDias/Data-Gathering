using Dapper;
using log4net;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SPI_Data_Gathering.Models
{
    public class Wise
    {
        public long ID_WISE {get;set;}
        public string IP {get;set;}
        public int CHANEL { get; set; }
        public string URL { get; set; }
        public bool ENABLE { get; set; }
        public string STATUS { get; set;}
        public DateTime? DT_STATUS { get; set;}

    }
    public class WiseModel
    {
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        public IEnumerable<Wise> SelectWise()
        {
            try
            {
                string sSql = string.Empty;

                sSql = "SELECT ID_WISE,IP,CHANEL,URL,ENABLE,STATUS,DT_STATUS FROM SPI_TB_DG_WISE WHERE ENABLE=1";

                IEnumerable<Wise> wiseList;
                using (IDbConnection db = new SqlConnection(ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString))
                {
                    wiseList = db.Query<Wise>(sSql);
                }
                return wiseList;
            }           
            catch (Exception ex)
            {
                log.Error(ex.ToString());
                return null;
            }
        }
        public bool UpdateWise(Wise ws)
        {
            try
            {
                string sSql = string.Empty;

                sSql = "UPDATE SPI_TB_DG_WISE SET STATUS='"+ ws.STATUS + "',DT_STATUS= GETDATE() WHERE ID_WISE="+ ws.ID_WISE;

                int update;
                using (IDbConnection db = new SqlConnection(ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString))
                {
                    update = db.Execute(sSql);
                }
                if (update > 0)
                {
                    return true;
                }
                return false;
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
                return false;
            }
        }
    }
}
