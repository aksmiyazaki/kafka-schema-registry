package com.github.alexandremiyazaki.avro.evolution;

import com.company.CustomerV1;
import com.company.CustomerV2;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/// TODO: V's methods (reads and writes) could be generics and avoid code duplication
public class SchemaEvolutionExample {

    public static void createAndWriteFile(DataFileWriter writer, Object customer, Schema schema, String filename) {
        try {
            writer.create(schema, new File(filename));
            writer.append(customer);
            writer.close();
            System.out.println("Wrote successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String writeCustomer(CustomerV1 customer) {
        System.out.println("Writing customerv1 " + customer);
        String fileName = "customerV1.avro";

        final DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);
        final DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter);

        createAndWriteFile(dataFileWriter, customer, customer.getSchema(), fileName);
        return fileName;
    }

    public static String writeCustomer(CustomerV2 customer) {
        System.out.println("Writing customerv2 " + customer);
        String fileName = "customerV2.avro";

        final DatumWriter<CustomerV2> datumWriter = new SpecificDatumWriter<>(CustomerV2.class);
        final DataFileWriter<CustomerV2> dataFileWriter = new DataFileWriter<>(datumWriter);

        createAndWriteFile(dataFileWriter, customer, customer.getSchema(), fileName);
        return fileName;
    }

    public static void readCustomerV1(String fileName) {
        final File file = new File(fileName);
        final DatumReader<CustomerV1> datumReaderV1 = new SpecificDatumReader<>(CustomerV1.class);
        try {
            final DataFileReader<CustomerV1> dataFileReaderV1 = new DataFileReader<>(file, datumReaderV1);
            while (dataFileReaderV1.hasNext()) {
                CustomerV1 customerV1Read = dataFileReaderV1.next();
                System.out.println("Read customer V1: " + customerV1Read);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void readCustomerV2(String fileName) {
        final File file = new File(fileName);
        final DatumReader<CustomerV2> datumReaderV2 = new SpecificDatumReader<>(CustomerV2.class);
        try {
            final DataFileReader<CustomerV2> dataFileReaderV2 = new DataFileReader<>(file, datumReaderV2);
            while (dataFileReaderV2.hasNext()) {
                CustomerV2 customerV2Read = dataFileReaderV2.next();
                System.out.println("Read customer V2: " + customerV2Read);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        CustomerV1 customerV1 = CustomerV1.newBuilder()
                .setAge(22)
                .setIsAutomaticEmailOn(false)
                .setFirstName("Potato")
                .setLastName("V1")
                .setHeight(177)
                .setWeight(99.0f)
                .build();

        String fileNameV1 = writeCustomer(customerV1);

        CustomerV2 customerV2 = CustomerV2.newBuilder()
                .setAge(22)
                .setFirstName("Potato")
                .setLastName("V2")
                .setHeight(177)
                .setWeight(99.0f)
                .setPhoneNumber("555-999-222")
                .setEmail("potato@example.com")
                .build();

        String fileNameV2 = writeCustomer(customerV2);

        readCustomerV1(fileNameV1);
        readCustomerV1(fileNameV2);
        readCustomerV2(fileNameV1);
        readCustomerV2(fileNameV2);
    }
}
