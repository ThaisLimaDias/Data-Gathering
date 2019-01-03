using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using log4net;
using System.Reflection;
using System.Configuration;
using System.IO;
using System.Net;

namespace SPI_Data_Gathering
{
    class HttpRequestOtherAPI
    {
        private static readonly ILog Log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        public string RequestOtherAPI(out HttpStatusCode result, string method, string url, string authHeader, string contentType, string data)
        {
          
            string content = null;

            WebRequest webRequest = null;
            WebResponse webResponse = null;
            HttpWebResponse httpResponse = null;
            Stream dataStream = null;
            StreamReader streamReader = null;

            try
            {
                webRequest = WebRequest.Create(url);
                webRequest.Method = method;
                webRequest.ContentType = contentType;

                String username = ConfigurationManager.AppSettings["UserAPI"].ToString();
                String password = ConfigurationManager.AppSettings["PassAPI"].ToString();
                String encoded = System.Convert.ToBase64String(System.Text.Encoding.GetEncoding("ISO-8859-1").GetBytes(username + ":" + password));

                webRequest.Headers.Add("Authorization", "Basic " + encoded);

                if (!string.IsNullOrEmpty(data))
                {
                    byte[] byteArray = Encoding.UTF8.GetBytes(data);
                    webRequest.ContentLength = byteArray.Length;
                    dataStream = webRequest.GetRequestStream();
                    dataStream.Write(byteArray, 0, byteArray.Length);
                    dataStream.Close();
                }

                webResponse = webRequest.GetResponse();

                httpResponse = (HttpWebResponse)webResponse;

                dataStream = webResponse.GetResponseStream();

                streamReader = new StreamReader(dataStream);

                content = streamReader.ReadToEnd();

                result = httpResponse.StatusCode;
            }
            catch (Exception ex)
            {
                Log.Error("Erro " + ex.ToString());
                result = HttpStatusCode.NotFound;
            }
            finally
            {
                if (streamReader != null) streamReader.Close();
                if (dataStream != null) dataStream.Close();
                if (httpResponse != null) httpResponse.Close();
                if (webResponse != null) webResponse.Close();
                if (webRequest != null) webRequest.Abort();
            }
            Log.Debug("Retorno da API" + result.ToString());
            return content;
        }
    }
}
