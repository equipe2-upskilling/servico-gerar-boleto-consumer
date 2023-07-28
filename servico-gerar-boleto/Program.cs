
using Servico_Gerar_Boleto.ConsumerSQS;
using System.Configuration;

// Configurações da AWS
string? awsAccessKeyId = ConfigurationManager.AppSettings["AWSAccessKeyId"];
string? awsSecretAccessKey = ConfigurationManager.AppSettings["AWSSecretAccessKey"];
string? awsRegion = ConfigurationManager.AppSettings["AWSRegion"];
string? inputQueueName = ConfigurationManager.AppSettings["QueueName"];
string? outputQueueName = ConfigurationManager.AppSettings["QueueNamePut"];


if (awsAccessKeyId == null) throw new Exception("Chave de Acesso faltando.");
if (awsSecretAccessKey == null) throw new Exception("Segredo da Chave de Acesso faltando.");
if (awsRegion == null) throw new Exception("Região faltando.");
if (inputQueueName == null) throw new Exception("Nome da fila de entrada faltando.");
if (outputQueueName == null) throw new Exception("Nome da fila de saida faltando.");

var consumer = new ConsumerAmazonWS(awsAccessKeyId, awsSecretAccessKey, awsRegion, inputQueueName, outputQueueName);

using var cancellationTokenSource = new CancellationTokenSource();

while (!cancellationTokenSource.Token.IsCancellationRequested)
{
    Console.WriteLine("Esperando Mensagem para gerar boleto....");
    await consumer.StartListening(cancellationTokenSource.Token);
    await Task.Delay(TimeSpan.FromSeconds(30), cancellationTokenSource.Token);
}

