const { MongoClient } = require('mongodb');
require('dotenv').config();

// Connection URI and database name
const uri = process.env.MONGODB_URI;
const databaseName = process.env.DATABASE_NAME;

const BATCH_SIZE = 10000; // Adjust batch size based on your system's capacity

const START_FROM = 0 // Use if Stuck in Middle

async function initializeSync() {
    const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

    try {
        await client.connect();
        console.log('Connected to the database');

        const database = client.db(databaseName);

        const propertiesCollection = database.collection('propertyV3');
        const propertiesMapCollection = database.collection('properties_mapV3');

        // Get the total count of documents
        const totalDocuments = await propertiesCollection.countDocuments();

        // Calculate the number of batches
        const totalBatches = Math.ceil(totalDocuments / BATCH_SIZE);

        // Process each batch
        for (let i = START_FROM; i < totalBatches; i++) {
            const properties = await propertiesCollection
                .find({})
                .skip(i * BATCH_SIZE)
                .limit(BATCH_SIZE)
                .toArray();

            // Sync properties in this batch
            for (const property of properties) {
                // Check if property._id already exists in propertiesMapCollection
                const existingProperty = await propertiesMapCollection.findOne({ _id: property._id });
                if (!existingProperty) {
                    await syncProperty(propertiesMapCollection, property);
                }
            }

            console.log(`Processed batch ${i + 1} out of ${totalBatches}`);
        }

        await createUniqueIndexAndHandleDuplicates(propertiesMapCollection);

        console.log('Initial synchronization completed');

    } finally {
        // Close the MongoDB client
        await client.close();
        console.log('Connection closed');
    }
}

// --------------------------- OLD CODE ---------------------------- //
// async function syncProperty(propertiesMapCollection, property) {
//     const propertyId = property._id;

//     // Extract relevant information
//     const status = property.status;
//     const type = property.type;
//     const suburb = property.address && property.address.geometricShape ? property.address.geometricShape.suburb : null;

//     const buildingIds1 = [];
//     const geometricShape = property.address.geometricShape;

//     const buildingId = property.address.geometricShape.polygon?.id || null;

//     if (buildingId !== null) {
//         buildingIds1.push(buildingId);
//     }

//     if (geometricShape && Array.isArray(geometricShape.building_parts)) {
//         geometricShape.building_parts.forEach(buildingPart => {
//             buildingIds1.push(buildingPart.id);
//         });
//     }

//     property.transactionDetails.soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));
//     const mostRecentSale = property.transactionDetails.soldHistory[property.transactionDetails.soldHistory.length - 1];
//     const price = mostRecentSale?.price;

//     if (buildingId !== null) {
//         const newDocument = {
//             _id: propertyId,
//             type: type,
//             status: status,
//             price: price || null,
//             suburb: suburb,
//             // buildingId: buildingId,
//             buildingIds: buildingIds1
//         };

//         // Find existing records with matching buildingId
//         const existingRecordsCursor = await propertiesMapCollection.find({ buildingIds: buildingId });

//         // Iterate through the cursor
//         let insertNewDocument = true;
//         await existingRecordsCursor.forEach(async (existingRecord) => {

//             if (existingRecord.type !== 'Apartment') {

//                 if (buildingIds1.length >= existingRecord.buildingIds.length) {
//                     // Delete the existing record if both existing and new types are 'Apartment'
//                     await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
//                 } else {
//                     insertNewDocument = false;
//                 }

//             } else if (existingRecord.type === 'Apartment' && type === 'Apartment') {

//                 if (buildingIds1.length >= existingRecord.buildingIds.length) {
//                     // Delete the existing record if both existing and new types are 'Apartment'
//                     await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
//                 } else {
//                     insertNewDocument = false;
//                 }

//             } else if (existingRecord.type === 'Apartment' && type !== 'Apartment') {
//                 // Set insertNewDocument to false if existing type is 'Apartment' and new type is not 'Apartment'
//                 insertNewDocument = false;
//             } else {

//                 if (buildingIds1.length >= existingRecord.buildingIds.length) {
//                     // Delete the existing record if both existing and new types are 'Apartment'
//                     await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
//                 } else {
//                     insertNewDocument = false;
//                 }
//             }
//         });

//         // Insert the new document if insertNewDocument is true
//         if (insertNewDocument) {
//             await propertiesMapCollection.insertOne(newDocument);
//         }

//     }

// }


async function syncProperty(propertiesMapCollection, property) {
    const propertyId = property._id;

    // Check if property.address and property.address.geometricShape are defined
    if (property.address && property.address.geometricShape) {
        // Extract relevant information
        const status = property.status;
        const type = property.type;
        const suburb = property.address.geometricShape.suburb;

        const buildingIds1 = [];
        const geometricShape = property.address.geometricShape;

        const buildingId = property.address.geometricShape.polygon?.id || null;

        if (buildingId !== null) {
            buildingIds1.push(buildingId);
        }

        if (geometricShape && Array.isArray(geometricShape.building_parts)) {
            geometricShape.building_parts.forEach(buildingPart => {
                buildingIds1.push(buildingPart.id);
            });
        }

        property.transactionDetails.soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));
        const mostRecentSale = property.transactionDetails.soldHistory[property.transactionDetails.soldHistory.length - 1];
        const price = mostRecentSale?.price;

        if (buildingId !== null) {
            const newDocument = {
                _id: propertyId,
                type: type,
                status: status,
                price: price || null,
                suburb: suburb,
                // buildingId: buildingId,
                buildingIds: buildingIds1
            };

            // Find existing records with matching buildingId
            const existingRecordsCursor = await propertiesMapCollection.find({ buildingIds: buildingId });

            // Iterate through the cursor
            let insertNewDocument = true;
            await existingRecordsCursor.forEach(async (existingRecord) => {

                if (existingRecord.type !== 'Apartment') {

                    if (buildingIds1.length >= existingRecord.buildingIds.length) {
                        // Delete the existing record if both existing and new types are 'Apartment'
                        await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
                    } else {
                        insertNewDocument = false;
                    }

                } else if (existingRecord.type === 'Apartment' && type === 'Apartment') {

                    if (buildingIds1.length >= existingRecord.buildingIds.length) {
                        // Delete the existing record if both existing and new types are 'Apartment'
                        await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
                    } else {
                        insertNewDocument = false;
                    }

                } else if (existingRecord.type === 'Apartment' && type !== 'Apartment') {
                    // Set insertNewDocument to false if existing type is 'Apartment' and new type is not 'Apartment'
                    insertNewDocument = false;
                } else {

                    if (buildingIds1.length >= existingRecord.buildingIds.length) {
                        // Delete the existing record if both existing and new types are 'Apartment'
                        await propertiesMapCollection.deleteOne({ _id: existingRecord._id });
                    } else {
                        insertNewDocument = false;
                    }
                }
            });

            // Insert the new document if insertNewDocument is true
            if (insertNewDocument) {
                await propertiesMapCollection.insertOne(newDocument);
            }

        }
    }
}


async function createUniqueIndexAndHandleDuplicates(propertiesMapCollection) {
    try {
        const agg = [
            {
                '$unwind': '$buildingIds'
            }, {
                '$group': {
                    '_id': '$buildingIds',
                    'count': {
                        '$sum': 1
                    }
                }
            }, {
                '$match': {
                    'count': {
                        '$gt': 1
                    }
                }
            }
        ];

        // const client = await MongoClient.connect('mongodb://127.0.0.1:27017/?replicaSet=rs0');
        // const coll = client.db('homiee_au_stagging').collection('properties_map');
        const cursor = await propertiesMapCollection.aggregate(agg); // Await the cursor
        const duplicatedRecords = await cursor.toArray();

        // Iterate through duplicates and handle them
        for (const duplicate of duplicatedRecords) {
            // Find documents with this buildingId
            const documents = await propertiesMapCollection.find({ buildingIds: duplicate._id }).toArray(); // Use 'coll' instead of 'propertiesMapCollection'
            // Sort documents by the number of buildingIds
            documents.sort((a, b) => b.buildingIds.length - a.buildingIds.length);

            // Delete extra documents, keeping the one with the most buildingIds
            for (let i = 1; i < documents.length; i++) {
                await propertiesMapCollection.deleteOne({ _id: documents[i]._id }); // Use 'coll' instead of 'propertiesMapCollection'
            }
        }
    } catch (error) {
        console.error('Error:', error);
    }
}

// Call the initialization function
initializeSync();
