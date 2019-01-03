using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace SPI_Data_Gathering
{
    static class Program
    {        
        static void Main()
        {
            #if DEBUG
                //If the mode is in debugging
                var myService = new Service1();
                myService.OnStartInDebug(null);
                
         }

            #else
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[] 
                { 
                    new Service1() 
                };
                ServiceBase.Run(ServicesToRun);
            }
            #endif
        
     }
}
