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

    public class Migration
    {
        public string Ip { get; set; } // Campo Ambos Analógico e Digital

        //[DisplayName("Chanel")]
        public string Ch { get; set; } // Campo Ambos Analógico e Digital

        //[DisplayName("En")]
        public string En { get; set; } // Campo exclusivo Analógico

        //[DisplayName("Rng")]
        public string Rng { get; set; } // Campo exclusivo Analógico

        //[DisplayName("Tag Value")]
        public decimal Val { get; set; } // Campo Ambos Analógico e Digital

        //[DisplayName("EgF")]
        public string EgF { get; set; } // Campo exclusivo Analógico

        //[DisplayName("Evt")]
        public string Evt { get; set; } // Campo exclusivo Analógico

        //[DisplayName("LoA")]
        public string LoA { get; set; } // Campo exclusivo Analógico

        //[DisplayName("HiA")]
        public string HiA { get; set; } // Campo exclusivo Analógico

        //[DisplayName("HVal")]
        public string HVal { get; set; } // Campo exclusivo Analógico

        //[DisplayName("HEgF")]
        public string HEgF { get; set; } // Campo exclusivo Analógico

        //[DisplayName("LVal")]
        public string LVal { get; set; } // Campo exclusivo Analógico

        //[DisplayName("LEgF")]
        public string LEgF { get; set; } // Campo exclusivo Analógico

        //[DisplayName("SVal")]
        public string SVal { get; set; } // Campo exclusivo Analógico

        //[DisplayName("ClrH")]
        public string ClrH { get; set; } // Campo exclusivo Analógico

        //[DisplayName("ClrL")]
        public string ClrL { get; set; } // Campo exclusivo Analógico

        //[DisplayName("PEgF")]
        public string PEgF { get; set; } // Campo exclusivo Analógico

        ////[DisplayName("Uni")]
        public string Uni { get; set; } // Campo exclusivo Analógico

        public string Md { get; set; } // Campo exclusivo Digital
        public string Stat { get; set; } // Campo exclusivo Digital
        public string Cnting { get; set; } // Campo exclusivo Digital
        public string OvLch { get; set; } // Campo exclusivo Digital
    }

    public class MigrationModel
    {
        private static readonly ILog log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        public bool Sp_Migration(Migration mgrt)
        {
            try
            {
                string sSql = string.Empty;

                sSql = "EXEC SPI_SP_DG_MIGRATION " + "'" + mgrt.Ip + "',";
                sSql = sSql + mgrt.Ch + ",";
                sSql = sSql + ((mgrt.En == null) ? "NULL" : mgrt.En) + ",";
                sSql = sSql + ((mgrt.Rng == null) ? "NULL" : mgrt.Rng) + ",";
                sSql = sSql + mgrt.Val + ",";
                sSql = sSql + ((mgrt.EgF == null) ? "NULL" : mgrt.EgF) + ",";
                sSql = sSql + ((mgrt.Evt == null) ? "NULL" : mgrt.Evt) + ",";
                sSql = sSql + ((mgrt.LoA == null) ? "NULL" : mgrt.LoA) + ",";
                sSql = sSql + ((mgrt.HiA == null) ? "NULL" : mgrt.HiA) + ",";
                sSql = sSql + ((mgrt.HVal == null) ? "NULL" : mgrt.HVal) + ",";
                sSql = sSql + ((mgrt.HEgF == null) ? "NULL" :mgrt.HEgF) + ",";
                sSql = sSql + ((mgrt.LVal == null) ? "NULL" : mgrt.LVal) + ",";
                sSql = sSql + ((mgrt.LEgF == null) ? "NULL" :mgrt.LEgF) + ",";
                sSql = sSql + ((mgrt.SVal == null) ? "NULL" : mgrt.SVal) + ",";
                sSql = sSql + ((mgrt.ClrH == null) ? "NULL" :mgrt.ClrH) + ",";
                sSql = sSql + ((mgrt.ClrL == null) ? "NULL" : mgrt.ClrL) + ",";
                sSql = sSql + ((mgrt.PEgF == null) ? "NULL" :mgrt.PEgF) + ",";
                sSql = sSql + ((mgrt.Uni == null) ? "NULL" : (mgrt.Uni.ToString() == "")? "NULL": "'" + mgrt.Uni + "'") + ",";
                sSql = sSql + ((mgrt.Md == null) ? "NULL" : mgrt.Md) + ",";
                sSql = sSql + ((mgrt.Stat == null) ? "NULL" : mgrt.Stat) + ",";
                sSql = sSql + ((mgrt.Cnting == null) ? "NULL" : mgrt.Cnting) + ",";
                sSql = sSql + ((mgrt.OvLch == null) ? "NULL" : mgrt.OvLch) + "";
              

                int update = 0;
                using (IDbConnection db = new SqlConnection(ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString))
                {
                    update = db.Execute(sSql);
                }

                if (update <= 0)
                {
                    return false;
                }
                return true;
            }
            catch (Exception ex)
            {
                log.Error(ex.ToString());
                return false;
            }
        }
        
    }
}
