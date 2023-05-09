
# Contributing to the Kafka EMS Sink project

## Local dev environment

This project top-level folder contains a [docker-compose file](docker-compose.yml) allowing to run locally a Kafka broker,
Schema-Registry, Kafka-connect and Lenses all packaged into a single fat Docker container.
This is intended to facilitate exploratory testing and interacting with the connector.

In order for Kafka Connect to discover the EMS Sink connector, before running `docker-compose` you will have to set the environment variable
`CONNECTOR_PATH` to an absolute path pointing at a directory containing the EMS Kafka sink jar.
Additionally, you will have to request a [License to use Lenses Box](https://lenses.io/downloads/lenses/). The license key is issued automatically and delivered
to your email address once you submit the form.

```bash
# assuming you have previously run `sbt assembly`
export CONNECTOR_PATH=$PWD/connector/target/scala-2.13/
export LENSES_EULA="https://licenses.lenses.io/download/lensesdl?id=<LICENSE_ID_HERE>"
```

With the above variable set, running `docker-compose up` should spin up the Lenses box container. After a couple of minutes, you should be
able to access the Lenses UI by pointing your browser to `http://localhost:3030`.

## Key connector components

The following sections is an attempt at walking new contributors through the key components of the EMS Kafka Sink
codebase.

_WARNING!:_ The information in this document is likely to eventually go out of sync with the implementation, so always refer the actual source code to validate it!

### The configuration system

The EMS sink taps into Kafka Connect built-in configuration system to expose a set of properties
to its users. Such configuration system provides a basic validation via a canonical
Kafka Connect endpoint whereby users can interactively check their connector properties 
before actually running the connector.

In order to introduce new configuration properties, you will have to:

- Appending a new property definition in `EmsSinkConfigDef.config`. A definition consists of the property name, its inline documentation text, its type, and default value and allow for the Kafka Connect framework to parse and validate the property value.
- Extend the `EmsSinkConfig` case class (or child classes) with a new field exposing the parsed property value to the internal connector logic.

The mapping from Kafka connect properties to the `EmsSinkConfig` case class happens in the `EmsSinkConfig.from` method,
which is also a good place to handle incompatible properties and perform necessary overrides.

### Writing Parquet files

The connector "buffers" batches of outgoing records in temporary Parquet files which get periodically uploaded to
EMS (via CBP) and then - depending on configuration - cleaned or renamed. The entry point component into such a file-based record buffering logic is the `ems.storage.WriterManager` class.

While there will always be effectively only one running instance of this class for each individual connector Instance, `WriterManager` will dispatch the operation of writing incoming
records to the appropriate `Writer` instance, depending on the incoming record partition (i.e. there will be as many writer as the number of partitions in the source topic).

We provide an experimental `inmemfs.enable` configuration option allowing to accumulate Parquet files in memory rather than
persisting them on the file system. This is intended to facilitate running the connector in constrained environments where the local file system 
might not accessible.

### Record transformations

The record transformation logic is encapsulated within the `ems.transform.RecordTransformer` class. This class wraps the 
_glue code_ necessary to address a number of transformations steps which we detail in the following sections. It's important to notice that:

- Transformation functions take as their inputs both the schema and the actual values. This is because the Kafka Connect API
  expects a matching schemas to be supplied as a constructor dependency of each `Struct` value created.
- The record key is currently left untouched and outside of domain of the transformation function.
- The output of the `RecordTransformer#transform` method is always an AVRO `GenericRecord`. This is intended to facilitate the interaction with
  the java library we use to create and manipulate Parquet files.

#### Schema Inference

In this step we attempt at inferring a Schema when the incoming Kafka Connect record has a null schema value
; this is the case of JSON formatted topic. The inferred schema is then fed into the flattening function, so it will
contain nested structures if present.

There corner cases that cause the inference to fail: most noticeably these include arrays containing values of different types
(e.g. `[1, true, 1029.0, "x"]`).

#### Record flattening

The connector provides a default flattening strategy which applies the following simple rules:
- array fields are JSON-encoded and transformed into nullable strings
- nested fields names are concatenated to their parent path and appended to the top-level struct

#### Chunked JSON blobs

An alternative flattening strategy is the _chunked JSON blob_ encoding. This strategy is aimed at providing an escape hatch in cases
where the input record has to be ingested in its original shape there exist records that are too big to fit within the maximum EMS string field length.
(i.e. 65,000 bytes).

#### Obfuscation of sensitive fields

We also provide support for some basic field-level obfuscation. Please refer to the project Wiki for usage details and to the
`ObfuscationUtils` module for the relevant source code.
