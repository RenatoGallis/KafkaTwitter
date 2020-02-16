# KafkaTwitter
Projeto de demonstração explicando um pipeline de ingestão de dados no Kafka e HDFS
Este projeto é constituido das seguintes classes:
TwitterProducer - Tem a função de fazer uma conexão com a api client do twitter para buscar tags pré especificadas no projeto e produzir essas mensagens em um tópico kafka.
TwitterConsumer - Será o consumidor dessas mensagens fazendo um arquivo das menssagens consumidas e posteriormente realizando a ingestão do mesmo no hadoop.

<H6><b>Documentação das ferramentas utilizadas</b></h6>
Kafka: https://kafka.apache.org/documentation/
Api Twitter: https://developer.twitter.com/en/docs
Hadoop - HDFS: https://hadoop.apache.org/docs/current/
