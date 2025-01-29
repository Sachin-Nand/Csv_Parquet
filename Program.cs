using CsvHelper;
using CsvHelper.Configuration;
using Parquet;
using Parquet.Serialization;
using Microsoft.Identity.Client;
using Newtonsoft.Json.Linq;
using Spectre.Console;
using System.Globalization;
using System.IO.Compression;
using System.Net.Http.Headers;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace ParquetWriter
{
    internal class Program
    {
        private static string RedirectURI = "http://localhost";
        private static string[] scopes_s = new string[] { "https://storage.azure.com/.default" };
        private static string[] scopes_f = new string[] { "https://api.fabric.microsoft.com/.default" };
        private static string Authority = "https://login.microsoftonline.com/organizations";
        private static readonly HttpClient client = new HttpClient();
        private static MemoryStream DataStream = new();

        private static string clientId = "";
        private static string workSpace = "";
        private static string workSpaceId = "";
        private static string lakeHouse = "";
        private static string lakeHouseId = "";
        private static string directory = "";
        private static string csvFilePath = "";
        private static string tablename = "";
        private static string filename = "";
        private static string mode = "";

        async static Task Main(string[] args)
        {
            ReadConfig();
            filename = csvFilePath.Split('\\')[^1];


            AnsiConsole.MarkupLine("");
            AnsiConsole.MarkupLine($"Fetching workspaceId for workspace [Red]{workSpace}[/] and lakehouseId for lakehouse [Red]{lakeHouse}[/]");
            AnsiConsole.MarkupLine("");
            workSpaceId = await GetWorkspaceId(workSpace);
            lakeHouseId = await GetLakeHouseId(workSpaceId);
            AnsiConsole.MarkupLine($"WorkspaceId for [Red]{workSpace}[/] is [Blue]{workSpaceId}[/] and lakehouseId for [Red]{lakeHouse}[/] is [Blue]{lakeHouseId}[/]");
            AnsiConsole.MarkupLine("");
            AnsiConsole.MarkupLine("");
            Thread.Sleep(1000);


            AnsiConsole.MarkupLine($"Converting the csv file [Red]{filename}[/] to parquet");
            AnsiConsole.MarkupLine("");
            await ConvertToParquetAsync(csvFilePath, false, ",");
            AnsiConsole.MarkupLine($"Conversion completed");
            AnsiConsole.MarkupLine("");
            AnsiConsole.MarkupLine("");

            AnsiConsole.MarkupLine($"Uploading the parquet file [Red]{filename.Replace("csv", "parquet")}[/] to lakehouse [Red]{lakeHouse}[/]");
            AnsiConsole.MarkupLine("");
            await FileStreamSendAsync(DataStream, directory, filename.Replace("csv", "parquet"));
            Thread.Sleep(1000);
            AnsiConsole.MarkupLine($"Upload to lakehouse completed");
            AnsiConsole.MarkupLine("");
            AnsiConsole.MarkupLine("");


            AnsiConsole.MarkupLine($"Uploading the parquet file [Red]{filename.Replace("csv", "parquet")}[/] to table [Red]{tablename}[/]");
            await LoadTable(tablename);
            Thread.Sleep(1000);
            AnsiConsole.MarkupLine($"Upload to table completed");

        }

        public static void ReadConfig()
        {
            var builder = new ConfigurationBuilder()
            .AddJsonFile($"local.settings.json", true, true);
            var config = builder.Build();
            clientId = config["ClientId"];
            workSpace = config["WorkSpace"];
            lakeHouse = config["LakeHouse"];
            csvFilePath = config["FileLocation"];
            tablename = config["Table"];
            directory = config["Directory"];
            mode = config["Mode"];
        }

        public static async Task ConvertToParquetAsync(string csvFilePath, bool hasHeader, string delimiter)
        {
            try
            {
                using var reader = new StreamReader(csvFilePath);
                var csvConfiguration = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    Delimiter = $"{delimiter}",
                    HasHeaderRecord = hasHeader
                };


                using (var csv = new CsvReader(reader, csvConfiguration))
                {
                    csv.Read();
                    workSpaceId = await GetWorkspaceId(workSpace);
                    lakeHouseId = await GetLakeHouseId(workSpaceId);
                    await ParquetSerializer.SerializeAsync(csv.GetRecords<NycTaxiTableData>().ToList(), DataStream, new ParquetSerializerOptions { CompressionMethod = CompressionMethod.Gzip, CompressionLevel = CompressionLevel.Fastest });
                }
            }


            catch (Exception ex)
            {
                throw ex;
            }
        }


        public async static Task<string> FileStreamSendAsync(Stream stream, string directory, string filename)
        {

            string RequestUri = $"https://onelake.dfs.fabric.microsoft.com/{workSpace}/{lakeHouse}.Lakehouse/{directory}/{filename}?resource=file";
            string jsonString = System.String.Empty;
            var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
            var response = await PutAsync(RequestUri, content, scopes_s);


            stream.Seek(0, SeekOrigin.Begin);
            var content_s = new StreamContent(stream);
            string url = $"https://onelake.dfs.fabric.microsoft.com/{workSpace}/{lakeHouse}.Lakehouse/{directory}/{filename}?action=append&position=0";
            var streamMessage = new HttpRequestMessage
            {
                Method = HttpMethod.Patch,
                RequestUri = new Uri(url),
                Content = content_s
            };

            HttpResponseMessage response_s = await client.SendAsync(streamMessage);
            try
            {
                response_s.EnsureSuccessStatusCode();
                await response_s.Content.ReadAsStringAsync();
            }
            catch
            {
                Console.WriteLine(response_s.Content.ReadAsStringAsync().Result);
                return null;
            }


            if (response_s.IsSuccessStatusCode)
            {
                url = $"https://onelake.dfs.fabric.microsoft.com/{workSpace}/{lakeHouse}.Lakehouse/{directory}/{filename}?action=flush&position={stream.Length}";

                var flushMessage = new HttpRequestMessage
                {
                    Method = HttpMethod.Patch,
                    RequestUri = new Uri(url)
                };
                response_s = await client.SendAsync(flushMessage);
                response_s.EnsureSuccessStatusCode();

            }
            return null;
        }

        static async Task LoadTable(string tablename)
        {

            string baseUrl = $"https://api.fabric.microsoft.com/v1/workspaces/{workSpaceId}/lakehouses/{lakeHouseId}/tables/{tablename}/load";

            var jsonData = new Dictionary<string, object>
                        {
                            { "relativePath",string.Concat(directory,"/",filename.Replace("csv", "parquet")) },
                            { "pathType", "File" },
                            { "mode", mode },
                            { "recursive", false },
                            { "formatOptions", new Dictionary<string, object>
                                {
                                    { "format", "Parquet" }
                                }
                            }
                        };

            string jsonString = Newtonsoft.Json.JsonConvert.SerializeObject(jsonData);
            var content = new StringContent(jsonString, Encoding.UTF8, "application/json");
            string response = await PostAsync(baseUrl, content, scopes_f);
            Console.WriteLine(response);

        }

        public async static Task<string> GetWorkspaceId(string Workspaace)
        {
            string baseUrl = $"https://api.fabric.microsoft.com/v1/workspaces";
            string response = await GetAsync(baseUrl, scopes_f);
            JObject workspace_jobject = JObject.Parse(response);
            JArray workspace_array = (JArray)workspace_jobject["value"];
            foreach (JObject workspace_array_0 in workspace_array)
            {
                if (workspace_array_0["displayName"].ToString() == workSpace)
                {
                    return workspace_array_0["id"].ToString();
                }

            }
            return null;

        }

        public async static Task<string> GetLakeHouseId(string WorkspaceId)
        {
            string baseUrl = $"https://api.fabric.microsoft.com/v1/workspaces/{WorkspaceId}/lakehouses";
            string response = await GetAsync(baseUrl, scopes_f);
            JObject lakehouse_jobject = JObject.Parse(response);
            JArray lakehouse_array = (JArray)lakehouse_jobject["value"];
            foreach (JObject lakehouse_array_0 in lakehouse_array)
            {
                if (lakehouse_array_0["displayName"].ToString() == lakeHouse)
                {
                    return lakehouse_array_0["id"].ToString();
                }


            }
            return null;

        }

        public async static Task<AuthenticationResult> ReturnAuthenticationResult(string[] scopes)
        {
            string AccessToken;
            PublicClientApplicationBuilder PublicClientAppBuilder =
                PublicClientApplicationBuilder.Create(clientId)
                .WithAuthority(Authority)
                .WithCacheOptions(CacheOptions.EnableSharedCacheOptions)
                .WithRedirectUri(RedirectURI);

            IPublicClientApplication PublicClientApplication = PublicClientAppBuilder.Build();
            var accounts = await PublicClientApplication.GetAccountsAsync();
            AuthenticationResult result;

            try
            {

                result = await PublicClientApplication.AcquireTokenSilent(scopes, accounts.First())
                                 .ExecuteAsync()
                                 .ConfigureAwait(false);
            }
            catch
            {
                result = await PublicClientApplication.AcquireTokenInteractive(scopes)
                                 .ExecuteAsync()
                                 .ConfigureAwait(false);
            }
            return result;
        }

        public async static Task<string> PutAsync(string url, HttpContent content, string[] scopes)
        {
            AuthenticationResult result = await ReturnAuthenticationResult(scopes);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", result.AccessToken);
            HttpResponseMessage response = await client.PutAsync(url, content);
            try
            {
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch
            {
                Console.WriteLine(response.Content.ReadAsStringAsync().Result);
                return null;
            }

        }
        public async static Task<string> GetAsync(string url, string[] scopes)
        {
            AuthenticationResult result = await ReturnAuthenticationResult(scopes);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", result.AccessToken);
            HttpResponseMessage response = await client.GetAsync(url);
            try
            {
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch
            {
                Console.WriteLine(response.Content.ReadAsStringAsync().Result);
                return null;
            }

        }

        public async static Task<string> PostAsync(string url, HttpContent content, string[] scopes)
        {

            AuthenticationResult result = await ReturnAuthenticationResult(scopes);
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", result.AccessToken);
            HttpResponseMessage response = await client.PostAsync(url, content);
            try
            {
                response.EnsureSuccessStatusCode();
                return await response.Content.ReadAsStringAsync();
            }
            catch
            {
                Console.WriteLine(response.Content.ReadAsStringAsync().Result);
                return null;
            }

        }


    }
}

public class NycTaxiTableData
{

    public string VendorID { get;}
    public Int32? passenger_count { get;}
    public double trip_distance { get;}
    public double RatecodeID { get;}
    public string store_and_fwd_flag { get;}
    public string PULocationID { get;}
    public string DOLocationID { get;}
    public string payment_type { get;}
    public double fare_amount { get;}
    public double extra { get;}
    public double mta_tax { get;}
    public double tip_amount { get;}
    public double tolls_amount { get;}
    public double improvement_surcharge { get;}
    public double total_amount { get;}
    public double congestion_surcharge { get;}
    public double airport_fee { get;}

}





