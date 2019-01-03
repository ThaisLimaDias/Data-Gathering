using log4net;
using Newtonsoft.Json;
using SPI_Data_Gathering.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SPI_Data_Gathering
{
    class Logic
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        public bool Ativo = true;

        public void Process()
        {
            try
            {
                Log.Debug("Iniciou Thread");

                int timer = Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["TimeProcess"].ToString());

                Log.Info("Monitoramento de Canais Ativo");

                MigrationModel _migration = new MigrationModel();
                Migration mg = new Migration();
                                
                WiseModel _wiseModel = new WiseModel();
                IEnumerable <Wise> wiseList;

                while (Ativo)
                {
                    //LISTANDO OS CANAIS MONITORADOS
                    wiseList= _wiseModel.SelectWise();
                    
                    foreach(var item in wiseList) //varre lista de canais ativos
                    {
                        Log.Debug(":" + DateTime.Now);
                        string retorno = string.Empty;
                        Log.Debug("Get Na URL do CH_"+ item.CHANEL + " IP:" + item.URL);
                        Log.Debug("antes do Get " + DateTime.Now);
                        mg = ReturnChanel(item.URL, out retorno);                        
                        if (retorno != "OK")
                        {
                            item.STATUS = "OFF";
                            _wiseModel.UpdateWise(item);
                            Log.Error("Erro no retorno da Url get da API Rest do Advantech ");
                        }

                        else //Caso retorne valor ok no get chama procedure de migração
                        {
                            item.STATUS = "ON";
                            _wiseModel.UpdateWise(item);
                            var obj = JsonConvert.SerializeObject(mg);
                            Log.Debug("Json que retornou do " + item.CHANEL + ":" + obj);
                            Log.Debug("Depois do Get " + DateTime.Now);
                            _migration.Sp_Migration(mg);
                        }
                    }
                    System.Threading.Thread.Sleep(timer);
                }
            }
            catch (Exception ex)
            {

                Log.Error("Erro " + ex.ToString());
                Process();
            }
            finally
            {

                Log.Debug("Finalizou Thread");
            }
        }
        public Migration ReturnChanel(string url, out string status)
        {
            HttpRequestOtherAPI getAPI = new HttpRequestOtherAPI();


            HttpStatusCode result = HttpStatusCode.Unused;

            var retorno = getAPI.RequestOtherAPI(out result, "GET", url, "xpto", "application/json", null);
            
            if (result != HttpStatusCode.Created && result != HttpStatusCode.OK)
            {
                status = "Erro no retorno da Url get da API Rest do Advantech ";
                return null;
            }
            else
            {
                Migration mg = JsonConvert.DeserializeObject<Migration>(retorno);
                status = "OK";
                return mg;
            }
        }
   }    
}
