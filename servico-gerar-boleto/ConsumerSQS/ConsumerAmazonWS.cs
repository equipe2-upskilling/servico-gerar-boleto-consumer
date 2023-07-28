using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using iText.Html2pdf;

namespace Servico_Gerar_Boleto.ConsumerSQS
{
    public class ConsumerAmazonWS
    {
        private readonly AmazonSQSClient _sqsClient;
        private readonly string? _inputqueueUrl;
        private readonly string? _outputqueueUrl;

        public ConsumerAmazonWS(string awsAccessKeyId, string awsSecretAccessKey, string awsRegion, string _inputQueueUrl, string _outputQueueUrl)
        {
            var sqsConfig = new AmazonSQSConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(awsRegion)
            };
            _sqsClient = new AmazonSQSClient(awsAccessKeyId, awsSecretAccessKey, sqsConfig);
            _inputqueueUrl = GetQueueUrlByName(_inputQueueUrl).GetAwaiter().GetResult();
            _outputqueueUrl = GetQueueUrlByName(_outputQueueUrl).GetAwaiter().GetResult();
        }

        public async Task StartListening(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var request = new ReceiveMessageRequest
                    {
                        QueueUrl = _inputqueueUrl,
                        MaxNumberOfMessages = 10, 
                        WaitTimeSeconds = 20 
                    };

                    var response = await _sqsClient.ReceiveMessageAsync(request, cancellationToken);

                    if (response.Messages.Count > 0)
                    {
                        foreach (var message in response.Messages)
                        {
                            Console.WriteLine($"Mensagem Recebida: {message.Body}");

                            await CreatePdfRequest(message.Body);

                            await DeleteMessageFromQueue(message.ReceiptHandle, cancellationToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Erro ao receber mensagens da fila SQS: {ex.Message}");
                }
            }
        }

        private async Task CreatePdfRequest(string messageBody)
        {
            string solutionDir = Directory.GetCurrentDirectory();
            string filePath = Path.Combine(solutionDir, "templateBoleto.html");
            
            FileStream htmlSource = File.Open(filePath, FileMode.Open);
            
            string filePathPdf = Path.Combine(solutionDir, "boleto.pdf");
            
            FileStream pdfDest = File.Open(filePathPdf, FileMode.OpenOrCreate);
            
            ConverterProperties converterProperties = new ConverterProperties();
            HtmlConverter.ConvertToPdf(htmlSource, pdfDest, converterProperties);
            
            Console.WriteLine("PDF do boleto gerado com sucesso!");
            
            if (!File.Exists(filePathPdf))
            {
                Console.WriteLine("Arquivo PDF do boleto não encontrado.");
                return;
            }

            try
            {
                byte[] fileBytes = File.ReadAllBytes(filePathPdf);

                var sendMessageRequest = new SendMessageRequest
                {
                    QueueUrl = _outputqueueUrl,
                    MessageBody = messageBody
                };

                var response = await _sqsClient.SendMessageAsync(sendMessageRequest);

                Console.WriteLine($"Arquivo de boleto enviado com sucesso para a fila SQS. MessageId: {response.MessageId}");
            }
            catch (AmazonSQSException ex)
            {
                Console.WriteLine($"Erro ao enviar o arquivo de boleto para a fila SQS: {ex.Message}");
            }

        }

        private async Task DeleteMessageFromQueue(string receiptHandle, CancellationToken cancellationToken)
        {
            try
            {
                var request = new DeleteMessageRequest
                {
                    QueueUrl = _inputqueueUrl,
                    ReceiptHandle = receiptHandle
                };

                await _sqsClient.DeleteMessageAsync(request, cancellationToken);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao deletar mensagem da fila SQS: {ex.Message}");
            }
        }

        private async Task<string> GetQueueUrlByName(string queueName)
        {
            try
            {
                var request = new GetQueueUrlRequest
                {
                    QueueName = queueName
                };

                var response = await _sqsClient.GetQueueUrlAsync(request);
                return response.QueueUrl;
            }
            catch (Exception ex)
            {
                throw new Exception($"Erro: {ex.Message}");
            }
        }
    }
}
