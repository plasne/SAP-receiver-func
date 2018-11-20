# Purpose

A customer needed to accept IDOCs from an SAP server at up to 1,000 per second. They wanted those documents to be written to Azure Blob Storage without any validation that the documents are legitimate (ie. are XML, conform to an identified schema, etc.). Those documents will be written into time-based partitions for later processing. Then the customer wanted to be able to kick off batch processing of those files, loading each, matching the document to a schema, and then saving fields to CSV files.

This collection of Azure Functions v2 support the above scenario:

-   **Receiver**: This function listens for incoming documents in an HTTP message body and saves them to Azure Blob Storage within a time-partitioned "folder".

-   **Start**: This function starts a batch process given a partition. This does not do any processing of the actual files but rather creates the CSV append blobs and then chunks up the processing into queued messages.

-   **Status**: This function returns the status of batch processing.

-   **Processor**: This function monitors the processing queue and and will process messages. Messages are a collection of filenames that will be loaded, matched to schemas, and then appended to CSV files.

# Storage

A named Azure Storage Account is used for all persistance required for this application. This Account should contain the following containers:

-   **STORAGE_CONTAINER_SCHEMA**: You must create a container to hold all your schema files. The name of that container should be stored in the configuration as STORAGE_CONTAINER_SCHEMA.

-   **STORAGE_CONTAINER_INPUT**: You must create a container to hold the partitions to accept incoming data. The name of that container should be stored in the configuration as STORAGE_CONTAINER_INPUT.

-   **STORAGE_CONTAINER_OUTPUT**: You must create a container to hold the partitions to accept outgoing data. The name of that container should be stored in the configuration as STORAGE_CONTAINER_OUTPUT.

For example, you might have the following hierarchy:

-   input _(container)_
    -   20180910T144500 _(auto-created partition)_
    -   20180910T150000 _(auto-created partition)_
    -   20180910T151500 _(auto-created partition)_
-   output
    -   20180910T144500 _(auto-created partition)_
    -   20180910T150000 _(auto-created partition)_
    -   20180910T151500 _(auto-created partition)_
-   schemas
    -   MATMAS05.json
    -   MATMAS06.json

In addition, there will be another Azure Storage Account created when the Azure Function is published. The Start Function will create:

-   **Processing**: This queue will hold the messages for the Processor Function.

There is no technical reason why you could not use the Functions Storage Account for the input, output, and schemas containers as well, so if you want to combine those, feel free.

# Multi-tenant

When using this set of Functions with multiple customers, you can follow these steps:

1. Create a new Function in portal.azure.com.

1. Enable deployment via **Local Git**.

1. Add a remote Git endpoint to your project.

1. Push to that endpoint.

When the Azure Function is created, a Storage Account will be created where the **Processing** queue can be created. In this way, the Status Function will support a separate status for each tenant.

# Configuration

These settings should appear in the Azure Functions AppSettings:

-   **FUNCTIONS_WORKER_RUNTIME**: Always "node".
-   **AzureWebJobsStorage**: The connection string for the Function's Storage Account, ex. "DefaultEndpointsProtocol=https;AccountName=pelasnefunca269;AccountKey=58...==".
-   **STORAGE_ACCOUNT**: The name of the Storage Account that will contain the input, output, and schemas containers.
-   **STORAGE_CONTAINER_INPUT**: The name of the input container, ex. "input".
-   **STORAGE_CONTAINER_OUTPUT**: The name of the output container, ex. "output".
-   **STORAGE_CONTAINER_SCHEMAS**: The name of the schemas container, ex. "schemas".
-   **STORAGE_SAS**: The SAS key to access the Storage Account, ex. "?st=2018-09-04T21%3A54%3A49Z&se=2019-09-04T21%3A54%3A00Z&sp=rwl&sv=2018-03-28&sr=c&sig=X...D". You must specify either STORAGE_SAS or STORAGE_KEY, but not both.
-   **STORAGE_KEY**: The key to access the Storage Account.
-   **FOLDER_PERIOD**: See the Partitioning section below, ex. "15 minutes".
-   **FOLDER_FORMAT**: See the Partitioning section below, ex. "YYYYMMDDTHHmmss".
-   **FILES_PER_MESSAGE**: The number of filenames to pack into a queue message, ex. "10". This will determine the number of files that will be processed by an instance of Processor.

In the host.json file, you can control how the Processor function reads from the queue:

```json
{
    "queues": {
        "maxPollingInterval": 1000,
        "visibilityTimeout": "00:00:30",
        "batchSize": 16,
        "maxDequeueCount": 4,
        "newBatchThreshold": 8
    }
}
```

For example, if you increased FILES_PER_MESSAGE such that the processing could not reasonably happen within 30 seconds, you might make the visibilityTimeout greater.

For another example, if each file has a large number of entries in it, you may also need to increase the visibilityTimeout to compensate.

## Schemas

A schema defines the files and columns within those files that will serve as the output from processing. You should create a JSON file for each desired schema in STORAGE_CONTAINER_SCHEMAS. Each input file can have multiple schemas applied.

A schema should look like this sample:

```json
{
    "name": "MATMA05",
    "filename": "matmas-block.csv",
    "identifier": "/MATMAS05/IDOC",
    "selector": "/MATMAS05/IDOC",
    "columns": [
        {
            "header": "DOCNUM",
            "path": "EDI_DC40/DOCNUM",
            "default": "0000000000000000",
            "enclosure": "\""
        },
        {
            "header": "BESKZ",
            "path": "E1MARAM/E1MARCM/BESKZ"
        }
    ]
}
```

-   **name**: The name appears in logging and helps you identify the specific schema.
-   **filename**: The name of the blob CSV that will contain the output for this schema.
-   **identifier**: This XPATH select statement must evaluate to 1+ results for this schema to be applied to the input file.
-   **selector**: This XPATH select statement is used to identify each document in the input file. If there is not at least 1 document identified, the schema is not applied to the input file.
-   **columns**: Each column must have a _header_ and _path_, but may also have a _default_ and/or _enclosure_.
    -   **header**: The name of the column that will appear in the header of the CSV.
    -   **path**: The XPATH select statement relative to each document determined by _selector_. The first result will be considered the value of the column.
    -   **default**: If _path_ did not return any results, this value will be used. If a _default_ is not specified, an empty string will be used.
    -   **enclosure**: If you want double-quotes or something else around the value, you can specify that as the enclosure.

## Multiple documents per input file

As mentioned in the schema section above, it is possible for a document to contain more than 1 document. The _selector_ determines where the document or documents will be located in the file.

Consider that large numbers of documents in a file might require:

-   setting FILES_PER_MESSAGE to a smaller value
-   setting visibilityTimeout to a longer duration

## Partitioning

When a IDOC file is received by the Receiver Function it must be written into a partition in the input container. The partition name is determed by the FOLDER_PERIOD and FOLDER_FORMAT.

FOLDER_PERIOD describes how often a new partition should be considered, for example, if it is set to "15 minutes", then the application would assume a partition at the top of the hour, 15 minutes after, 30 minutes after, 45 minutes after, and so on. After the partition time is determined, it is rendered per FORMAT_FORMAT. For example, "YYYYMMDDTHHmmss" would then assume partitions like "20180101T000000", "20180101T001500", "20180101T003000", "20180101T004500". The partitions are always rendered in UTC.

Please note that partitions are not created except when a file would need to be written into it, so there very well could be gaps in the input container.

# Usage

After publishing the Functions to Azure, you can go into the portal and get the endpoints for Receiver, Start, and Status (which will include a Function code for authentication). There is no endpoint for Processor as that simply monitors the Processing queue to start.

-   The customer should push SAP IDOCs to the Receiver endpoint.
-   When you are ready to start the processing, call the Start endpoint. When calling the Start endpoint you must include a querystring parameter of _partition_ specifying the name of the "folder" containing the files.
-   To monitor the completion, call the Status endpoint.

# Output

The output folder will contain a CSV file for each schema and an errors.txt file. The errors file will contain a log of any problems processing the batch.

Should a batch be processed more than once, these files will be reset.

## Write Limitations

There is a limit of 50,000 writes that can be made to any specific CSV file. The first write is the header. After that, each Processor call will flush a single write to the CSV file if it has any rows for that file. For example, if FILES_PER_MESSAGE is set to 10 and all 10 files in that message are identified for a specific schema, the file associated with that schema will have 1 write that contains 10 rows.

There is also a limit of 50,000 writes that can be made to the errors.txt file. Each Processor call will flush a single write to this file if it has any errors.

If you are close to the above write limitations, you could increase FILES_PER_MESSAGE so that the write chunks are larger. If you increase this too much, you should consider increasing "visibilityTimeout" as well.
