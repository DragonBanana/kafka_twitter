package kafka;

import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import com.google.gson.Gson;
import kafka.model.Tweet;
import kafka.rest.TweetRoute;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.AvroSchema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.codehaus.jackson.map.ObjectMapper;
import spark.Spark;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.Date;

import static spark.Spark.*;

public class App {

    public static void main(String[] args) throws Exception {
        Tweet tweet = new Tweet("author", "content", Long.toString(new Date().getSeconds()), "s", Arrays.asList("tags"), Arrays.asList("mention"));
        TweetRoute.configureRoutes();
    }


}
