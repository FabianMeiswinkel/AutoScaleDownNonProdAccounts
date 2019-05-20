using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using System.Text;

namespace Meiswinkel.CosmosDB.CostWatchdog
{
    public static class NonProdAutoScaleHttp
    {
        [FunctionName("NonProdAutoScaleHttp")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log,
            ExecutionContext context)
        {
            if (log == null)
            {
                throw new ArgumentNullException(nameof(log));
            }

            try
            {
                log.LogInformation("C# HTTP trigger function processing a request.");

                var config = new ConfigurationBuilder()
                    .SetBasePath(context.FunctionAppDirectory)
                    .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                    .AddEnvironmentVariables()
                    .Build();

                string subscriptionId = config["subscriptionId"];
                string resourceGroup = config["resourceGroup"];

                MSILoginInformation loginInfo = new MSILoginInformation(MSIResourceType.AppService);
                AzureCredentials creds = new AzureCredentialsFactory().FromMSI(loginInfo, AzureEnvironment.AzureGlobalCloud);

                string response = await (NonProdAutoScale.RunAsync(log, creds, subscriptionId, resourceGroup));

                return new OkObjectResult(response);
            }
            catch (Exception error)
            {
                log.LogCritical(error.ToString());

                return new ObjectResult(error.ToString()) { StatusCode = 500 };
            }
        }
    }
}
