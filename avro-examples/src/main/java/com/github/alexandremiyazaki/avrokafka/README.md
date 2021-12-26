## Running
This folder contains a mix of classes used with the avro schemas.
Note that the only schema that we use is Customer (without V's), because to evolve
schemas, you can't rename the schema. Because of that, whenever you are using 
a specific version of a schema, you have to comment the code that uses a different version.

This could be put in another project (in fact, it is what the Course instructor does), but I've opted to avoid
duplicating the boilerplate. 