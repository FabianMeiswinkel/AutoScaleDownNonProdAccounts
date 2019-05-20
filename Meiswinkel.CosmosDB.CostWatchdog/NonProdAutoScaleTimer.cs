using System;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Meiswinkel.CosmosDB.CostWatchdog
{
    public static class NonProdAutoScaleTimer
    {
        [FunctionName("NonProdAutoScaleTimer")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            log.LogInformation($"C# Timer trigger function executing at: {DateTime.Now}");

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string subscriptionId = config["subscriptionId"];
            string resourceGroup = config["resourceGroup"];

            MSILoginInformation loginInfo = new MSILoginInformation(MSIResourceType.AppService);
            AzureCredentials creds = new AzureCredentialsFactory().FromMSI(loginInfo, AzureEnvironment.AzureGlobalCloud);

            NonProdAutoScale.RunAsync(log, creds, subscriptionId, resourceGroup).Wait();
        }
    }
}
