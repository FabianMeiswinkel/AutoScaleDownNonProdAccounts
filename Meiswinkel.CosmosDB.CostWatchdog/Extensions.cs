using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Management.CosmosDB.Fluent;

namespace Meiswinkel.CosmosDB.CostWatchdog
{
    public static class Extensions
    {
        const string AutoScaleCategoryKeyName = "CosmosDBAutoScaleCategory";
        const string AutoScaleNonProdCategory = "NonProd";

        public static bool IsNonProdAccount(this ICosmosDBAccount thisPtr)
        {
            if (thisPtr == null)
            {
                throw new ArgumentNullException(nameof(thisPtr));
            }

            // Either based on Tags (CosmosDBAutoScaleCategory == NonProd)
            if (thisPtr.Tags != null &&
                thisPtr.Tags.ContainsKey(AutoScaleCategoryKeyName))
            {
                return AutoScaleNonProdCategory.Equals(
                    thisPtr.Tags[AutoScaleCategoryKeyName],
                    StringComparison.OrdinalIgnoreCase);
            }

            // or based on name
            return thisPtr.Name.Contains("dev", StringComparison.OrdinalIgnoreCase) ||
                thisPtr.Name.Contains("test", StringComparison.OrdinalIgnoreCase) ||
                thisPtr.Name.Contains("tst", StringComparison.OrdinalIgnoreCase) ||
                thisPtr.Name.Contains("qa", StringComparison.OrdinalIgnoreCase) ||
                thisPtr.Name.Contains("stag", StringComparison.OrdinalIgnoreCase);
        }

        public static async Task<IList<string>> GetCollectionLinksAsync(
            this DocumentClient client,
            string databaseLink)
        {
            List<string> collectionLinks = new List<string>();
            string continuation = string.Empty;
            do
            {
                FeedResponse<DocumentCollection> offersResponse = await client.ReadDocumentCollectionFeedAsync(
                    databaseLink + "/colls",
                    new FeedOptions
                    {
                        MaxItemCount = 100,
                        RequestContinuation = continuation
                    });

                foreach (DocumentCollection collection in offersResponse)
                {
                    collectionLinks.Add(collection.AltLink);
                }

                // Get the continuation so that we know when to stop.
                continuation = offersResponse.ResponseContinuation;
            } while (!string.IsNullOrEmpty(continuation));

            return collectionLinks;
        }

        public static Task<(
            string DatabaseName,
            string CollectionName,
            long DocumentCount,
            long DocumentsSize,
            long CollectionSize)> PopulateCollectionUsageAsync( this DocumentClient client, string databaseName, string collectionName)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            return PopulateCollectionUsageAsync(client, collectionUri.AbsoluteUri);
        }

        public static async Task<(
            string DatabaseName,
            string CollectionName,
            long DocumentCount,
            long DocumentsSize,
            long CollectionSize)> PopulateCollectionUsageAsync(this DocumentClient client, string collectionResourceLink)
        {
            if (client == null)
            {
                throw new ArgumentNullException(nameof(client));
            }

            if (String.IsNullOrWhiteSpace(collectionResourceLink))
            {
                throw new ArgumentNullException(nameof(collectionResourceLink));
            }

            var options = new RequestOptions
            {
                PopulateQuotaInfo = true,
            };

            ResourceResponse<DocumentCollection> collectionResponse =
                                    await client.ReadDocumentCollectionAsync(collectionResourceLink, options);

            string collectionName = collectionResponse.Resource.AltLink;
            string databaseName = "unknown";

            string[] altLinkFragments = collectionResponse.Resource.AltLink.Split("/");

            if (altLinkFragments.Length >= 4)
            {
                collectionName = altLinkFragments[3];
                databaseName = altLinkFragments[1];
            }

            // sample header value
            // "functions=0;storedProcedures=0;triggers=0;documentSize=10178;documentsSize=5781669;documentsCount=17151514;collectionSize=10422760";
            String quotaUsage = collectionResponse.ResponseHeaders["x-ms-resource-usage"];

            long documentCount = 0;
            long collectionSize = 0;
            long collectionDocumentSize = 0;

            string[] quotaUsageFragments = quotaUsage.Split(';');
            foreach (string quotaUsageFragment in quotaUsageFragments)
            {
                string[] valueFragments = quotaUsageFragment.Split('=');
                if (valueFragments.Length != 2)
                {
                    continue;
                }

                if ("documentsSize".Equals(valueFragments[0], StringComparison.OrdinalIgnoreCase))
                {
                    collectionDocumentSize = Int64.Parse(valueFragments[1]) * 1024;
                }

                if ("documentsCount".Equals(valueFragments[0], StringComparison.OrdinalIgnoreCase))
                {
                    documentCount = Int64.Parse(valueFragments[1]);
                }

                if ("collectionSize".Equals(valueFragments[0], StringComparison.OrdinalIgnoreCase))
                {
                    collectionSize = Int64.Parse(valueFragments[1]) * 1024;
                }
            }

            return (databaseName, collectionName, documentCount, collectionDocumentSize, collectionSize);
        }
    }
}
