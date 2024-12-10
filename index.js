const { MongoClient } = require("mongodb");
const { handleUpdate } = require("./collectionsActions/update");
const { handleDelete } = require("./collectionsActions/delete");
const { handleInsert } = require("./collectionsActions/insert");

async function main() {
    const uri = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=myReplicaSet";
    const client = new MongoClient(uri);

    try {
        // Connect to the MongoDB cluster
        await client.connect();
        console.log("Connected to MongoDB replica set!");

        // Specify the database
        const database = client.db("homiee");

        // Listen to changes in the specified collection
        const collection = database.collection("philip_test_propertyV3");
        const changeStream = collection.watch();

        console.log("Listening for changes in philip_test_propertyV3...");

        // Process each change as it occurs
        changeStream.on("change", async (changeEvent) => {
            console.log("Change detected:", changeEvent);

            try {
                if (changeEvent.operationType === "update") {
                    await handleUpdate(database, changeEvent);
                } else if (changeEvent.operationType === "insert") {
                    await handleInsert(database, changeEvent);
                } else if (changeEvent.operationType === "delete") {
                    await handleDelete(database, changeEvent);
                } else {
                    console.log("Unhandled change operation type:", changeEvent.operationType);
                }
            } catch (err) {
                console.error("Error handling change event:", err);
            }
        });

    } catch (err) {
        console.error("Error:", err);
    } finally {
        // Optionally close the connection when necessary
        // await client.close();
    }
}

// Start the main function
main().catch(console.error);
