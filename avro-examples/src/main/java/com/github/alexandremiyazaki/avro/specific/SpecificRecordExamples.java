package com.github.alexandremiyazaki.avro.specific;


import com.company.CustomerV1;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExamples {
    public static void main(String[] args) {
        // Step 1: Create specific Record
        CustomerV1.Builder customerBuilder = CustomerV1.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("Potato");
        customerBuilder.setLastName("Baked");
        customerBuilder.setHeight(155);
        customerBuilder.setWeight(59.0f);
        customerBuilder.setIsAutomaticEmailOn(false);
        CustomerV1 customer = customerBuilder.build();

        System.out.println(customer);

        // Step 2: Write to a file
        DatumWriter<CustomerV1> datumWriter = new SpecificDatumWriter<>(CustomerV1.class);

        try (DataFileWriter<CustomerV1> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Successfully wrote customer-specific.avro");
        } catch (IOException e) {
            System.out.println("Cannot write file");
            e.printStackTrace();
        }

        // Step 3: Read from a file
        final File file = new File("customer-specific.avro");
        final DatumReader<CustomerV1> datumReader = new SpecificDatumReader<>(CustomerV1.class);
        final DataFileReader<CustomerV1> dataFileReader;

        try {
            System.out.println();
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                CustomerV1 readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("FirstName: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
