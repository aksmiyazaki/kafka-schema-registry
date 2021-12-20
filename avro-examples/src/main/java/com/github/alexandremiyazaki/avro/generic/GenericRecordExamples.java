package com.github.alexandremiyazaki.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;

import java.io.File;
import java.io.IOException;

public class GenericRecordExamples {
    public static void main(String[] args) {
        // step 0: define schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "\t\"type\": \"record\",\n" +
                "\t\"namespace\": \"com.company\",\n" +
                "\t\"name\": \"customer\",\n" +
                "\t\"doc\": \"first exercise of the course, where you have to define a schema\",\n" +
                "\t\"fields\": [\n" +
                "\t{\n" +
                "\t\t\"name\": \"first_name\",\n" +
                "\t\t\"type\": \"string\",\n" +
                "\t\t\"doc\": \"First name of the customer\"\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"name\": \"last_name\",\n" +
                "\t\t\"type\": \"string\",\n" +
                "\t\t\"doc\": \"Last name of the customer\"\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"name\": \"age\",\n" +
                "\t\t\"type\": \"int\",\n" +
                "\t\t\"doc\": \"Age of the customer\"\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"name\": \"height\",\n" +
                "\t\t\"type\": \"int\",\n" +
                "\t\t\"doc\": \"Height in Centimeters\"\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"name\": \"weight\",\n" +
                "\t\t\"type\": \"float\",\n" +
                "\t\t\"doc\": \"Weight of the customer in Kgs\"\n" +
                "\t},\n" +
                "\t{\n" +
                "\t\t\"name\": \"is_automatic_email_on\",\n" +
                "\t\t\"type\": \"boolean\",\n" +
                "\t\t\"doc\": \"True if the customer wants automated emails\",\n" +
                "\t\t\"default\": true\n" +
                "\t}\n" +
                "\t]\n" +
                "}");

        // step 1: generate a generic record
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 42);
        customerBuilder.set("height", 170);
        customerBuilder.set("weight", 88.9f);
        customerBuilder.set("is_automatic_email_on", false);

        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        GenericRecordBuilder customerDefaultBuilder = new GenericRecordBuilder(schema);
        customerDefaultBuilder.set("first_name", "Dohn");
        customerDefaultBuilder.set("last_name", "Joe");
        customerDefaultBuilder.set("age", 59);
        customerDefaultBuilder.set("height", 199);
        customerDefaultBuilder.set("weight", 77.9f);

        GenericData.Record customerWithDefaults = customerDefaultBuilder.build();
        System.out.println(customerWithDefaults);

        // step 2: write to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
            dataFileWriter.append(customer);
            System.out.println("Appended to file.");
        } catch (IOException e) {
            System.out.println("Cannot create file");
            e.printStackTrace();
        }

        // step 3: read a file
        final File avroFile = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datumReader)) {
            customerRead = dataFileReader.next();

            System.out.println("Read successfully.");
            System.out.println(customerRead.toString());

            System.out.println("First name: " + customerRead.get("first_name"));
            System.out.println("Last name: " + customerRead.get("last_name"));
            System.out.println("Non Existent field: " + customerRead.get("potato"));
        } catch (IOException e) {
            System.out.println("Error reading file");
            e.printStackTrace();
        }
    }
}
