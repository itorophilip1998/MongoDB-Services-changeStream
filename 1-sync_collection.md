### Comprehensive Documentation for Synchronization Code

#### Overview:
This code facilitates synchronization between two MongoDB collections, `properties` and `properties_map`. It iterates through the documents in the `properties` collection in batches, extracts relevant information, and syncs it to the `properties_map` collection based on certain conditions. Additionally, it handles duplicates in the `properties_map` collection by creating a unique index and deleting redundant documents.

#### Dependencies:
- `mongodb`: This code utilizes the MongoDB Node.js driver for interaction with MongoDB databases.
- `dotenv`: This package allows loading environment variables from a `.env` file into `process.env`.

#### Environment Variables:
- `MONGODB_URI`: The URI for connecting to the MongoDB instance.
- `DATABASE_NAME`: The name of the MongoDB database.

#### Constants:
- `BATCH_SIZE`: Specifies the number of documents to process in each batch during synchronization.

#### Functions:

1. **initializeSync():**
   - Establishes a connection to the MongoDB database using the provided URI.
   - Calculates the total number of documents in the `properties` collection and the number of batches required.
   - Iterates through each batch of documents, processes them, and logs the progress.
   - Calls `createUniqueIndexAndHandleDuplicates()` function after synchronization is complete.
   - Closes the MongoDB client connection.

2. **syncProperty(propertiesMapCollection, property):**
   - Extracts relevant information from a property document and prepares it for synchronization.
   - Compares the current property with existing records in the `properties_map` collection based on `buildingIds`.
   - Determines whether to insert the new document or update existing ones.
   - Inserts the new document if necessary.

3. **createUniqueIndexAndHandleDuplicates(propertiesMapCollection):**
   - Aggregates documents in the `properties_map` collection to identify duplicates based on `buildingIds`.
   - Iterates through the duplicates and retains only the document with the most `buildingIds`.
   - Deletes redundant documents to ensure uniqueness based on the `buildingIds` field.

#### Usage:
- Call the `initializeSync()` function to initiate the synchronization process.

#### Notes:
- Ensure that the required environment variables (`MONGODB_URI` and `DATABASE_NAME`) are properly configured.
- Adjust the `BATCH_SIZE` constant according to system capacity and performance requirements.

#### Error Handling:
- Errors are logged to the console for easy debugging and troubleshooting.

#### Recommendations:
- Consider adding error handling and logging mechanisms for improved reliability and monitoring.
- Evaluate and optimize the synchronization process based on the size and complexity of the datasets.