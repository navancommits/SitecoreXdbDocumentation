﻿using Sitecore.XConnect;
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
 * Establish contact between the Sitecore.Documentation app and your XConnect instance
 * https://doc.sitecore.com/xp/en/developers/104/sitecore-experience-platform/walkthrough--creating-a-contact-and-an-interaction.html#establish-contact-between-the-sitecoredocumentation-app-and-your-xconnect-instance
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
                    // This is where we add content in later code samples
                    Console.WriteLine("connection successful");
                }
                catch (XdbExecutionException ex)
                {
                    // Deal with exception
                }
            }
        }
    }
}
