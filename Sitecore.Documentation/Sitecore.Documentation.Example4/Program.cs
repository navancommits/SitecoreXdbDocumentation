using Sitecore.XConnect;
using Sitecore.XConnect.Client;
using Sitecore.XConnect.Client.WebApi;
using Sitecore.XConnect.Collection.Model;
using Sitecore.XConnect.Schema;
using Sitecore.Xdb.Common.Web;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
/*
 * Bulk insert multiple contacts with known identifier - customised based on below url
 * https://doc.sitecore.com/xp/en/developers/104/sitecore-experience-platform/walkthrough--creating-a-contact-and-an-interaction.html#create-a-contact-with-a-known-identifier
 */
namespace Sitecore.Documentation
{
    public class Program
    {
        // From <xConnect instance>\App_Config\AppSettings.config
        static string CERTIFICATE_OPTIONS = ConfigurationManager.AppSettings["certConnString"];

        // From your installation
        static string XCONNECT_URL = ConfigurationManager.AppSettings["xConnectUrl"];

        private static void Main(string[] args)
        {
            MainAsync(args).ConfigureAwait(false).GetAwaiter().GetResult();
            System.Console.ForegroundColor = ConsoleColor.DarkGreen;
            System.Console.WriteLine("");
            Console.WriteLine("END OF PROGRAM.");
            Console.ReadKey();
        }

        private static async Task MainAsync(string[] args)
        {
            CertificateHttpClientHandlerModifierOptions options = CertificateHttpClientHandlerModifierOptions.Parse(CERTIFICATE_OPTIONS);

            var certificateModifier = new CertificateHttpClientHandlerModifier(options);

            List<IHttpClientModifier> clientModifiers = new List<IHttpClientModifier>();
            var timeoutClientModifier = new TimeoutHttpClientModifier(new TimeSpan(0, 0, 20));
            clientModifiers.Add(timeoutClientModifier);

            var collectionClient = new CollectionWebApiClient(
                new Uri(XCONNECT_URL + "/odata"),
                clientModifiers,
                new[] { certificateModifier }
            );

            var searchClient = new SearchWebApiClient(
                new Uri(XCONNECT_URL + "/odata"),
                clientModifiers,
                new[] { certificateModifier }
            );

            var configurationClient = new ConfigurationWebApiClient(
                new Uri(XCONNECT_URL + "/configuration"),
                clientModifiers,
                new[] { certificateModifier }
            );

            var cfg = new XConnectClientConfiguration(
                new XdbRuntimeModel(CollectionModel.Model),
                collectionClient,
                searchClient,
                configurationClient
            );

            try
            {
                await cfg.InitializeAsync();

                // Print xConnect if configuration is valid
                var arr = new[]
                {
                    @"            ______                                                       __     ",
                    @"           /      \                                                     |  \    ",
                    @" __    __ |  $$$$$$\  ______   _______   _______    ______    _______  _| $$_   ",
                    @"|  \  /  \| $$   \$$ /      \ |       \ |       \  /      \  /       \|   $$ \  ",
                    @"\$$\/  $$| $$      |  $$$$$$\| $$$$$$$\| $$$$$$$\|  $$$$$$\|  $$$$$$$ \$$$$$$   ",
                    @" >$$  $$ | $$   __ | $$  | $$| $$  | $$| $$  | $$| $$    $$| $$        | $$ __  ",
                    @" /  $$$$\ | $$__/  \| $$__/ $$| $$  | $$| $$  | $$| $$$$$$$$| $$_____   | $$|  \",
                    @"|  $$ \$$\ \$$    $$ \$$    $$| $$  | $$| $$  | $$ \$$     \ \$$     \   \$$  $$",
                    @" \$$   \$$  \$$$$$$   \$$$$$$  \$$   \$$ \$$   \$$  \$$$$$$$  \$$$$$$$    \$$$$ "
                };
                Console.WindowWidth = 160;
                foreach (string line in arr)
                    Console.WriteLine(line);

            }
            catch (XdbModelConflictException ce)
            {
                Console.WriteLine("ERROR:" + ce.Message);
                return;
            }

            // Initialize a client using the validated configuration
            using (var client = new XConnectClient(cfg))
            {
                try
                {
                    // loop multiple inserts
                    for (int i = 1; i <= 100; i++)
                    {
                        // Create a unique identifier for each contact
                        var identifier = new ContactIdentifier[]
                        {
                            new ContactIdentifier(
                                "twitter",
                                "myrtlesitecore" + Guid.NewGuid().ToString("N"),
                                ContactIdentifierType.Known
                            )
                        };

                        Console.WriteLine($"Creating Contact #{i}: {identifier[0].Identifier}");

                        Contact knownContact = new Contact(identifier);

                        client.AddContact(knownContact);
                    }

                    // Submit contact and interaction - a total of two operations
                    await client.SubmitAsync();

                    // Get the last batch that was executed
                    var operations = client.LastBatch;

                    // Loop through operations and check status
                    foreach (var operation in operations)
                    {
                        Console.WriteLine(
                            operation.OperationType
                            + operation.Target.GetType().ToString()
                            + " Operation: "
                            + operation.Status
                        );
                    }

                    Console.ReadLine();
                }
                catch (XdbExecutionException ex)
                {
                    // Deal with exception
                }
            }
        }
    }
}
