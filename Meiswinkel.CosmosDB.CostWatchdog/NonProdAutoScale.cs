using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Management.CosmosDB.Fluent.Models;
using Microsoft.Azure.Management.Fluent;
using Microsoft.Azure.Management.ResourceManager.Fluent.Authentication;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Microsoft.Extensions.Logging;

namespace Meiswinkel.CosmosDB.CostWatchdog
{
    public static class NonProdAutoScale
    {
        const int MinThroughput = 400;
        const string MaxRUPerSecondMetricName = "Max RUs Per Second";
        const string MinRUHeaderName = "x-ms-cosmos-min-throughput";

        public static async Task<string> RunAsync(
            ILogger log,
            AzureCredentials creds,
            string subscriptionId,
            string resourceGroup)
        {
            if (creds == null)
            {
                throw new ArgumentNullException(nameof(creds));
            }

            StringBuilder response = new StringBuilder();
            log.LogInformation("Function triggered.");

            IAzure azure = Azure.Authenticate(creds).WithSubscription(subscriptionId);

            IPagedCollection<ICosmosDBAccount> cosmosDBAccounts;

            if (String.IsNullOrWhiteSpace(resourceGroup))
            {
                cosmosDBAccounts = await azure.CosmosDBAccounts.ListAsync(loadAllPages: true);
            }
            else
            {
                cosmosDBAccounts = await azure.CosmosDBAccounts.ListByResourceGroupAsync(resourceGroup, loadAllPages: true);
            }

            foreach (var account in cosmosDBAccounts)
            {
                if (!account.IsNonProdAccount())
                {
                    continue;
                }

                log.LogInformation(account.Name);
                response.AppendLine(account.Name);

                string key = null;
                try
                {
                    key = (await account.ListKeysAsync()).PrimaryMasterKey;
                }
                // TODO make more specific
                catch (Exception error)
                {
                    log.LogWarning(error.ToString());
                    continue;
                }

                ConnectionPolicy connectionPolicy = new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                };

                using (DocumentClient client = new DocumentClient(
                    new Uri(account.DocumentEndpoint),
                    key,
                    connectionPolicy,
                    ConsistencyLevel.Eventual))
                {
                    string continuation = string.Empty;
                    do
                    {
                        // Read the feed 10 items at a time until there are no more items to read
                        FeedResponse<Offer> offersResponse = await client.ReadOffersFeedAsync(
                            new FeedOptions
                            {
                                MaxItemCount = 100,
                                RequestContinuation = continuation
                            });

                        foreach (OfferV2 offer in offersResponse)
                        {
                            int provisionedThroughput = offer.Content.OfferThroughput;

                            if (provisionedThroughput == MinThroughput)
                            {
                                continue;
                            }

                            string[] fragments = offer.ResourceLink.Split('/');

                            bool isSharedOffer = !offer.ResourceLink.Contains("/colls/");

                            double maxConsumedThroughput = 0;
                            if (isSharedOffer)
                            {
                                // For shared database offers the provisioned throughput is shared across all
                                // collections. So we need to aggregate the consumed RU across collections.
                                IList<string> collectionLinks = await client.GetCollectionLinksAsync(offer.ResourceLink);

                                maxConsumedThroughput = await GetMaxConsumedRequestUnitsAsync(
                                    account,
                                    client,
                                    log,
                                    provisionedThroughput,
                                    collectionLinks);
                            }
                            else
                            {
                                maxConsumedThroughput = await GetMaxConsumedRequestUnitsAsync(
                                    account,
                                    client,
                                    log,
                                    provisionedThroughput,
                                    new string[] { offer.ResourceLink });
                            }

                            log.LogDebug(
                                "{0} - {1}: {2:#,###,###,###,##0} max RU consumed, {3:#,###,###,###,##0} partitioned throughput",
                                offer.ResourceLink,
                                isSharedOffer ? "SharedDatabaseOffer" : "CollectionOffer",
                                maxConsumedThroughput,
                                provisionedThroughput);

                            if (maxConsumedThroughput > 0)
                            {
                                continue;
                            }

                            await DownScaleOfferAsync(
                                client,
                                log,
                                response,
                                offer,
                                isSharedOffer);
                        }

                        // Get the continuation so that we know when to stop.
                        continuation = offersResponse.ResponseContinuation;
                    } while (!string.IsNullOrEmpty(continuation));
                }
            }

            return response.ToString();
        }

        // For shared database offers the provisioned throughput is shared across all
        // collections. So we need to aggregate the consumed RU across collections.

        private static async Task<double> GetMaxConsumedRequestUnitsAsync(
            ICosmosDBAccount account,
            DocumentClient client,
            ILogger log,
            int provisionedThroughput,
            IList<string> collectionLinks)
        {
            DateTime nowSnapshot = DateTime.UtcNow;
            IEnumerable<Metric> metrics = null;
            var aggregatedMetricValues = new ConcurrentDictionary<DateTime, double>();

            foreach (string collectionLink in collectionLinks)
            {
                int partitionCount = (await client.ReadPartitionKeyRangeFeedAsync(collectionLink)).Count;
                var collectionMetadata = await client.PopulateCollectionUsageAsync(collectionLink);
                double maxCollectionConsumedThroughput = 0;

                for (int i = 1; i <= 3; i++)
                {
                    DateTime endTime = nowSnapshot.AddMinutes(-1 * (i - 1) * 5).AddMinutes(-10);
                    DateTime startTime = nowSnapshot.AddMinutes(-1 * i * 5).AddMinutes(-10);

                    // collection Link has the Format "dbs/<DatabaseId>/colls/<CollectionId>
                    string[] fragments = collectionLink.Split('/');

                    metrics = await account.Manager.Inner.CollectionPartition.ListMetricsAsync(
                        account.ResourceGroupName,
                        account.Name,
                        fragments[1],
                        fragments[3],
                        ConstructFilterExpression(MaxRUPerSecondMetricName, startTime, endTime, "PT1M"));

                    if (metrics == null ||
                        metrics.Count() == 0)
                    {
                        continue;
                    }

                    foreach (var metricValue in metrics.Single().MetricValues)
                    {
                        if (metricValue.Maximum == null ||
                            metricValue.Timestamp == null ||
                            metricValue.Maximum.Value <= 0)
                        {
                            continue;
                        }

                        aggregatedMetricValues.AddOrUpdate(
                            metricValue.Timestamp.Value,
                            metricValue.Maximum.Value,
                            (timestamp, currentValue) => currentValue + metricValue.Maximum.Value);

                        if (metricValue.Maximum.Value > maxCollectionConsumedThroughput)
                        {
                            maxCollectionConsumedThroughput = metricValue.Maximum.Value;
                        }
                    }
                }

                log.LogDebug("{0}.{1}:\r\n  {2:#,###,###,###,##0} documents,\r\n  {3:#,###,###,###,##0} size of documents,\r\n  {4:#,###,###,###,##0} total size,\r\n  {5} partitions,\r\n  {6:#,###,###,###,##0} Max Consumed RU per partition,\r\n  {7:#,###,###,###,##0} provisioned throughput per partition",
                    collectionMetadata.DatabaseName,
                    collectionMetadata.CollectionName,
                    collectionMetadata.DocumentCount,
                    collectionMetadata.DocumentsSize,
                    collectionMetadata.CollectionSize,
                    partitionCount,
                    maxCollectionConsumedThroughput,
                    provisionedThroughput / partitionCount);
            }

            double maxConsumedThroughput = 0;
            foreach (var aggregatedValue in aggregatedMetricValues.Values)
            {
                if (aggregatedValue > maxConsumedThroughput)
                {
                    maxConsumedThroughput = aggregatedValue;
                }
            }

            return maxConsumedThroughput;
        }

        private static string ConstructFilterExpression(string metricName, DateTime startTime, DateTime endTime, string granularity)
        {
            if (String.IsNullOrWhiteSpace(metricName))
            {
                throw new ArgumentNullException(nameof(metricName));
            }

            if (String.IsNullOrWhiteSpace(granularity))
            {
                throw new ArgumentNullException(nameof(granularity));
            }

            string encodedMetricName = Uri.EscapeDataString(metricName);
            string formattedStartTime = startTime.ToString("yyyy-MM-ddTHH:mm:00.000Z");
            string formattedEndTime = endTime.ToString("yyyy-MM-ddTHH:mm:00.000Z");
            string metricNameFilter = $"name.value eq '{metricName}'";
            string timeGrainFilter = $" and timeGrain eq duration'{granularity}'";
            string startTimeFilter = $" and startTime eq {formattedStartTime}";
            string endTimeFilter = $" and endTime eq {formattedEndTime}";

            return $"{metricNameFilter}{timeGrainFilter}{startTimeFilter}{endTimeFilter}";
        }

        private static async Task DownScaleOfferAsync(
            DocumentClient client,
            ILogger log,
            StringBuilder response,
            OfferV2 offer,
            bool isSharedOffer)
        {
            int provisionedThroughput = offer.Content.OfferThroughput;

            ResourceResponse<Offer> offerResponse = await client.ReadOfferAsync(offer.SelfLink);
            int targetedThroughput = Int32.Parse(offerResponse.ResponseHeaders[MinRUHeaderName]);

            if (offer.Content.OfferThroughput == targetedThroughput)
            {
                return;
            }

            await client.ReplaceOfferAsync(new OfferV2(offer, targetedThroughput));

            log.LogInformation(
                "{0} - {1}: Scaled down to {2:#,###,###,###,##0} RUs",
                offer.ResourceLink,
                isSharedOffer ? "SharedDatabaseOffer" : "CollectionOffer",
                targetedThroughput);

            response.AppendFormat("{0} - {1}: Scaled down to {2:#,###,###,###,##0} RUs",
                offer.ResourceLink,
                isSharedOffer ? "SharedDatabaseOffer" : "CollectionOffer",
                targetedThroughput).AppendLine();
        }
    }
}
