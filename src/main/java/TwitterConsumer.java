
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterConsumer {

	public static void main(String[] args) {
		new TwitterConsumer().run();// Acesso o mmetodo da classe pelo construtor que instanciei
	}

	// construtor da classe ConsumerDemoThread para inicializa-la
	private TwitterConsumer() {
	}

	private void run() {

		Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());// logar o que desejar da classe
		String bootstrapserver = "127.0.0.1:9092";
		String groupID = "my-seventh-application";
		String auto_offset_reset_config = "latest";//latest - earliest 
		// variavel do topico:
		String topic = "twitter_topic";
		// Trava para para o consumer
		CountDownLatch trava = new CountDownLatch(1);

		// Criando o consumidor com a thread
		logger.info("Criando minha thread de consumer");
//		Runnable myConsumerThread = new ConsumerThreads(trava, topic, bootstrapserver, groupID,
//				auto_offset_reset_config);

		Runnable myConsumerThread = new ConsumerThreads(trava, topic, bootstrapserver, groupID,
				auto_offset_reset_config);

		// começa o fluxo da thread
		Thread myThread = new Thread(myConsumerThread);
		myThread.start();// começa com a thread

		// Começo o tratamento para fechar a aplicação corretamente
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Desligando o consumer");
			((ConsumerThreads) myConsumerThread).shutdown();
			try {
				trava.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
				logger.error("Aplicação interrompida");
			}
			logger.info("Aplicação terminada");
			
		}));

		try {
			trava.await();
		} catch (InterruptedException e) {
			logger.error("Aplicação interrompida" + e);
		} finally {
			//
			logger.info("forçando o final do rolê");
		}
	}

	// Classe para disparar uma thread precisa implementar Runnable
	public class ConsumerThreads implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerThreads.class.getName());// logar o que desejar da
																							// classe
		private CountDownLatch trava;// para fazer a contagem da trava para parar o consumo
		private KafkaConsumer<String, String> consumer;

		public ConsumerThreads(CountDownLatch trava, String topic, String bootstrapserver, String groupID,
				String auto_offset_reset_config) {

			this.trava = trava;
			// Criar as propriedades do consumer
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);// servidor do broker kafka
																								// onde vai se conectar
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());// deserializando
																														// chave
																														// tem
																														// que
																														// ser
																														// o
																														// mesmo
																														// formato
																														// que
																														// o
																														// produtor
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());// deserializando
																														// valor
																														// tem
																														// que
																														// ser
																														// o
																														// mesmo
																														// formato
																														// que
																														// no
																														// produtor
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID); // ID do grupo de consumidores que esse
																				// consumer faz parte (boa pratica todo
																				// consumer precisa pertencer a um
																				// consumer-group)
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, auto_offset_reset_config); // Define onde
																										// meu offset
																										// irá ler as
																										// mensagens
																										// (mais cedo
																										// mais antigo
																										// pulando
																										// alguns
																										// offsets etc)
			// Aqui termina a configuração do consumidor

			consumer = new KafkaConsumer<String, String>(properties);// Criando o consumidor com o tipo de dado String e
																		// passando as propridades acima como
																		// configuração
			consumer.subscribe(Arrays.asList(topic));// o metodo Arrays.asList da a capacidade de consumir de varios
														// tópicos distintos
		}

		public void run() {
			// poll data temos que lançar uma exceção
			try {
				
				// setando variaveis de configuração do hadoop
				Configuration conf = new Configuration();
//				conf.set("fs.defaultFS", hdfuri);
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				System.setProperty("HADOOP_USER_NAME", "hdfs");
				System.setProperty("hadoop.home.dir", "/");
//				FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);
				
				//Criando diretorio no HDFS
//				Path hdfsDir = fs.getWorkingDirectory();
//				Path newFolderPath = new Path("");//Colocar o caminho que será do hadoop
//				if(!fs.exists(newFolderPath)) {
//					fs.mkdirs(newFolderPath);
//				}
//				Path hdfswritepath = new Path(newFolderPath + "/" + "twitter.txt");
//				FSDataOutputStream outDataOutputStream = fs.create(hdfswritepath);
				File file = new File("C:\\Users\\Renato Gallis\\Desktop\\twitter.txt");
				file.getParentFile().mkdir();
				file.createNewFile();
				//passsa o caminho do arquivo que deve ser escrito
		    	FileWriter writer = new FileWriter(file);
		    	BufferedWriter buffWriter = new BufferedWriter(writer);
				while (true) {
					// Criando um consumer records para pegar a mensagem que esta sendo pesquisada
					// de mils em  mils
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

					for (ConsumerRecord<String, String> record : records) {
						// Gerar arquivo e mandar para o HDFS nesse ponto *** Renato Gallis****
//						logger.info("Key:" + record.key() + "\n" + "Value:" + record.value() + "\n" + "Partition:"
//								+ record.partition() + "\n" + "Offset:" + record.offset() + "TimeStamp:" + record.timestamp());
//						
						//Escreve dados no arquivo pré existente fazendo o append dos mesmos	
						 buffWriter.write(record.value() + System.lineSeparator());
						 buffWriter.flush();
						 //Escreve o conteudo no arquivo gerado no HDFS
//						 outDataOutputStream.writeBytes(record.key()+ record.value());
//						 outDataOutputStream.flush();
					}
//					buffWriter.close();
				}
			
			} catch (WakeupException exception) {
				logger.info("As mensagens foram recebidas!");
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// fechar o consumidor depois de ler a mensagens
				consumer.close();
				// fale para o main() que fechei o consumidor
				trava.countDown();
			}
		}

		public void shutdown() {
			// interromper o consumer.poll(); que é a pesquisa por mensagens
			consumer.wakeup();
		}
	}

}
