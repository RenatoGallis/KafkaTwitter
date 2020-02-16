
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	public TwitterProducer() {}
	//criando rastreio de loger na classe
		Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
	
		private static Properties config = new Properties();
		private static String arquivo = "C:\\Users\\Renato Gallis\\twitterconfig.ini";
		
//		String consumerKey="dqXM1Ch9CAUwsi6J5ltJ4QPK4";
//		String consumerSecret="4eHvb7iTPInDmtQxfGBMLfiP7tjqjWNFoxbQOIuErUj1E3zQro";
//		String token="792793562064748544-DlNskS8nobKVRir5k7HIdgfJKc7yeUt";
//		String secret="J0fx96ge9hyLZQI3xaGmkKu3K3If5GxbxkVrFSlfNOZQd";
//		
		
		List<String> terms = Lists.newArrayList("Coronavirus");
		
	public static void main(String[] args) {
	
	new TwitterProducer().run();
		
	}
	
	public void run() {
	   
		logger.info("Inicio");
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		//criar um twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to establish a connection.
		client.connect();
		//criar um kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//Adicionando shutdownhook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			
			logger.info("Parando aplicação...");
			logger.info("Parando conexão com o Twitter...");
			client.stop();
			logger.info("Fechando producer...");
			producer.close();
		} ));
		//loop para mandar mensagens para o kafka
		
		while (!client.isDone()) {
			  String msg = null; 
			  try {
			  msg = msgQueue.poll(5,TimeUnit.SECONDS);
			  }catch(InterruptedException e) {
				  e.printStackTrace();
				  client.stop();
			  }
			  if(msg!= null) {
				  logger.info(msg);
				  producer.send(new ProducerRecord<String, String>("twitter_topic",null, msg),new Callback() {
					
					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if(e!= null) {
							logger.error("Something bad happened",e);
						}
						
					}
				});
			  }
			  
			}
		logger.info("End of application");
	}
	

   //Criando o client para o twitter
	public Client createTwitterClient(BlockingQueue<String> msgQueue) {
		try {
		config.load(new FileInputStream(arquivo));
		}catch(IOException e) {e.getStackTrace();}
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		//List<Long> followings = Lists.newArrayList(1234L, 566788L);
		
		//hosebirdEndpoint.followings(followings);
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
//		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
		Authentication hosebirdAuth = new OAuth1(config.getProperty("consumerKey"), config.getProperty("consumerSecret"), config.getProperty("token"), config.getProperty("secret"));
		ClientBuilder builder = new ClientBuilder()
				  .name("Hosebird-Client-01")      // optional: mainly for the logs
				  .hosts(hosebirdHosts)
				  .authentication(hosebirdAuth)
				  .endpoint(hosebirdEndpoint)
				  .processor(new StringDelimitedProcessor(msgQueue));
				//  .eventMessageQueue(eventQueue);    // optional: use this if you want to process client events

				Client hosebirdClient = builder.build();
				return hosebirdClient;
				
	}
	
	//Criando um kafkaProducer
	public KafkaProducer<String, String> createKafkaProducer() {
		// TODO Auto-generated method stub
		
		  String bootstrapServers = "127.0.0.1:9092";
		
		  
		 // 1 - Fazer as propriedades do producer
		    Properties properties = new Properties();
		   //Configurando as propriedades do topico utilizando o ProducerConfig do pacote clients do kafka    
		    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); //endereço do broker kafka
		    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Informando que o tipo de serializador é String ou seja vou mandar texto para o kafka
		    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());//Informando que o tipo de serializador é String ou seja vou mandar texto para o kafka
		  
		    //Proprieddes para um producer seguro
		    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		    properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
		    properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
		    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");// se for kafka>1.0 então 5 se for menor 1
		    
		    // Configuração para dar alto rendimento do producer
		    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");//atraso para receber as mensagens quanto mais atrasar melhor o fator de compressão(fazer um micro-batch) atraso sempre em milesegundos
		    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));//32KB tamanho do batch
		    
		 // 2 - Criar o producer
		    //Crio um produtor com chave e valor sendo String e passo as configurações acima para o mesmo
		    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer ;
	}
}


