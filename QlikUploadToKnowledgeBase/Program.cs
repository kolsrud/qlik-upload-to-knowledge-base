using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Qlik.Sense.RestClient;

namespace QlikUploadToKnowledgeBase
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var conf = JObject.FromObject(new
            {
                url = "",
                apiKey = "",
                kbName = ""
            });

            const string answersUploadConfFile = "answersUploadConf.json";
            if (!File.Exists(answersUploadConfFile))
            {
                Console.WriteLine("No configuration file exists. Writing empty file.");
                File.WriteAllText(answersUploadConfFile, conf.ToString(Formatting.Indented));
                return;
            }

            conf = JObject.Parse(File.ReadAllText(answersUploadConfFile));

            var url = conf["url"].Value<string>();
            var apiKey = conf["apiKey"].Value<string>();
            var kbName = conf["kbName"].Value<string>();

            var client = new RestClientQcs(url);
            client.AsApiKey(apiKey);

            var folder = @".";
            var files = args;

            UploadFiles(client, folder, files);
            UpdateKnowledgeBase(client, kbName, files.Select(Path.GetFileName));
        }

        private static void UpdateKnowledgeBase(RestClientQcs client, string kbName, IEnumerable<string?> fileNames)
        {
            var kbs = client.Get<JToken>("/api/v1/knowledgebases");
            var kb = kbs["data"].OfType<JObject>().Where(o => o["name"].Value<string>() == kbName).ToArray();
            if (kb.Length != 1)
            {
                Console.WriteLine($"Error: Found {kb.Length} knowledge bases with name \"{kbName}\"");
                return;
            }

            var kbId = kb.First()["id"].Value<string>();
            Console.WriteLine($"KB \"{kbName}\" has ID {kbId}");
            // Console.WriteLine(kb.First());
            // Console.WriteLine(client.Get<JToken>($"/api/v1/knowledgebases/{kbId}"));
            var fileSource = client.Get<JObject>($"/api/v1/knowledgebases/{kbId}")["datasources"].OfType<JObject>()
                .SingleOrDefault(o => o["type"].Value<string>() == "file");
            // Console.WriteLine(fileSource);
            var dataSourceId = fileSource["id"].Value<string>();
            var oldFiles = fileSource["fileConfig"]["files"].Values<string>().ToArray();
            Console.WriteLine("Old files:");
            Console.WriteLine(string.Join("\n", oldFiles));
            var newFiles = fileNames.Except(oldFiles).ToArray();
            Console.WriteLine("New files:");
            Console.WriteLine(string.Join("\n", newFiles));
            if (!newFiles.Any())
            {
                Console.WriteLine("No new files added. Nothing to do.");
                return;
            }
            var allFiles = oldFiles.Concat(newFiles).ToArray();
            Console.WriteLine($"Total number of files: {allFiles.Length}");
            fileSource["fileConfig"]["files"] = new JArray(allFiles);
            Console.WriteLine($"PUT /api/v1/knowledgebases/{kbId}/datasources/{dataSourceId}");
            var rsp = client.Put<JToken>($"/api/v1/knowledgebases/{kbId}/datasources/{dataSourceId}", fileSource);
            client.Post($"/api/v1/knowledgebases/{kbId}/datasources/{dataSourceId}/actions/dryruns");
            Console.WriteLine("Update complete.");
        }

        private static void UploadFiles(RestClientQcs client, string folder, string[] files)
        {
            foreach (var file in files)
            {
                Console.Write(file + " : ");
                var rsp = client.Get<JObject>($"/api/v1/data-files?name={Path.GetFileName(file)}");
                var alreadyExists = rsp["data"].HasValues;
                if (alreadyExists)
                {
                    Console.WriteLine("Already exists. Skipping.");
                }
                else
                {
                    Console.Write("Uploading... ");
                    UploadFile(client, Path.Combine(folder, file));
                    Console.WriteLine("Done!");
                }
            }
        }

        static void UploadFile(IRestClientGeneric restClient, string filePath)
        {
            var fileName = Path.GetFileName(filePath);

            // ********************************************************
            // * Large files must first be uploaded to temp-contents
            // ********************************************************
            // Console.WriteLine($"Uploading file \"{fileName}\" to temp-contents... ");
            var streamContent = new StreamContent(File.OpenRead(filePath));
            var rsp = restClient.PostHttp($"/api/v1/temp-contents?filename={fileName}", streamContent);
            var location = rsp.Headers.Location.ToString();
            // Console.WriteLine("File created: " + location);

            // ********************************************************
            // * The created file can now be imported to data-files
            // ********************************************************
            // Console.WriteLine("Importing temporary file to data-files... ");
            var json = JObject.FromObject(new
            {
                name = fileName,
                tempContentFileId = location.Split('/').Last()
            });
            var content = new MultipartFormDataContent
            {
                { new StringContent(json.ToString(Formatting.None)), "Json" },
            };

            restClient.Post("/api/v1/data-files", content);
            // Console.WriteLine("Done!");
        }
    }

}
