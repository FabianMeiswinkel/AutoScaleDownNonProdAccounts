using System;
using Meiswinkel.CosmosDB.CostWatchdog;
using Microsoft.Azure.Management.ResourceManager.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Microsoft.IdentityModel.Clients.ActiveDirectory;

namespace TestDummy
{
    class Program
    {
        const string ClientId = "c27925f1-83ab-48d5-916e-725608134224";
        const string ClientSecret = "Secret";
        const string TenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47";
        const string SubscriptionId = "12053b8f-cab5-4f5c-9c1a-870580142abd";
        static void Main(string[] args)
        {
            try
            {
                IServiceCollection serviceCollection = new ServiceCollection();
                serviceCollection.AddLogging(builder => builder
                    .AddConsole()
                    .AddFilter(level => level >= Microsoft.Extensions.Logging.LogLevel.Debug)
                );
                var loggerFactory = serviceCollection.BuildServiceProvider().GetService<ILoggerFactory>();

                string output = NonProdAutoScale.RunAsync(
                    log: loggerFactory.CreateLogger("TestDummy"),
                    creds: new AzureCredentialsFactory().FromServicePrincipal(
                        ClientId,
                        ClientSecret,
                        TenantId,
                        AzureEnvironment.AzureGlobalCloud),
                    subscriptionId: SubscriptionId,
                    resourceGroup: "fabianm").Result;

                Console.WriteLine(output);
            }
            catch (AggregateException error)
            {
                foreach (Exception inner in error.InnerExceptions)
                {
                    Console.WriteLine("EXCEPTION: {0}", inner);
                }
            }
            catch (Exception error)
            {
                Console.WriteLine("EXCEPTION: {0}", error);
            }

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        static bool HandleDeviceResult(DeviceCodeResult result)
        {
            Console.WriteLine(result);

            return true;
        }
    }
}
