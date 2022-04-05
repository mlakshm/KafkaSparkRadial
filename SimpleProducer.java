import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.text.SimpleDateFormat;


// Create java class named “SimpleProducer”
public class SimpleProducer {
  
  
   

  public ArrayList<String> readFolderFiles() throws IOException {

           ArrayList<String> msgs = new ArrayList<String>();
           //Creating a File object for directory
           //File directoryPath = new File("/Users/mlakshm/test_msg");
           //File directoryPath = new File("/Users/mlakshm/Downloads/folder_splitter/to_garage_10000_order_created0");
           File directoryPath = new File("/Users/mlakshm/Downloads/to_garage_10000_order_created");
           //File directoryPath = new File("/Users/mlakshm/Downloads/to_garage_10000_order_created_5k");
           //List of all files and directories
           File filesList[] = directoryPath.listFiles();
           System.out.println("List of files and directories in the specified directory:");
           for(File file : filesList) {
              //System.out.println("File name: "+file.getName());
              //System.out.println("File path: "+file.getAbsolutePath());
              //System.out.println("Size :"+file.getTotalSpace());
              //System.out.println(" ");
              //System.out.println(Paths.get(file.getAbsolutePath()));
              String message = new String ( Files.readAllBytes( Paths.get(file.getAbsolutePath()) ) );
              message = message.replace("\n","");
              msgs.add(message);
              FileReader fileStream = new FileReader( file );
              BufferedReader bufferedReader = new BufferedReader( fileStream );
              String line = null;

              while( (line = bufferedReader.readLine()) != null ) {
                      //System.out.println(line);
                      line=line.replace("\n","");
                      //message=line;
               }
           }

       return msgs;

  }



  public static void main(String[] args) throws Exception {

    // Assign topicName to string variable
    String topicName = "gtest50p";

    // create instance for properties to access producer configs
    Properties props = new Properties();

    // Assign localhost id
    props.put(
        "bootstrap.servers",
        "minimal-prod-kafka-bootstrap-cp4i.itzroks-270006dwv1-tdbuym-6ccd7f378ae819553d37d5f2ee142bd6-0000.us-south.containers.appdomain.cloud:443");

    props.put("security.protocol", "SASL_SSL");

    props.put("ssl.truststore.location", "/Users/mlakshm/Downloads/es-cert.p12");

    props.put("ssl.truststore.password", "JCi4nt0DkN9B");

    props.put("ssl.protocol", "TLSv1.2");

    props.put("sasl.mechanism", "SCRAM-SHA-512");

    props.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required username='cred2' password='yRenuVTcidCV';");

    // Set acknowledgements for producer requests.
    props.put("acks", "all");

    // If the request fails, the producer can automatically retry,
    props.put("retries", 0);

    // Specify buffer size in config
    // props.put("batch.size", 16384);

    // Reduce the no of requests less than 0
    // props.put("linger.ms", 1);

    // The buffer.memory controls the total amount of memory available to the producer for
    // buffering.
    // props.put("buffer.memory", 33554432);

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    

    ClassLoader original = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(null);


    Producer<String, String> producer = new KafkaProducer<String, String>(props);
    
    Thread.currentThread().setContextClassLoader(original);
    
    SimpleProducer obj = new SimpleProducer();
    
    ArrayList<String> messages = new ArrayList<String>();


    try {
                        messages=obj.readFolderFiles();
                        //System.out.println(message);
    }
    catch(IOException e) {

                         e.printStackTrace();

    }

    String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());


    
    System.out.println(timeStamp);
    System.out.println(messages.size());
    for (int i = 0; i < messages.size(); i++ ) {
            //while (!closing) {
                //String key = "key";
                String key = String.valueOf(i);
                //String message = "This is a beautiful day!!!";
                String message = messages.get(i);
                //System.out.println(message);
                 producer.send(
                   new ProducerRecord<String, String>(topicName, key, message));

    }
    
   // for (int i = 0; i < 10; i++)
     // producer.send(
          //new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
    System.out.println("Message sent successfully");

    String timeStamp1 = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
    
    System.out.println(timeStamp1);

    producer.close();



  }

}
