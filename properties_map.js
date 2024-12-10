const { MongoClient, ObjectId } = require('mongodb');
require('dotenv').config();

// Connection URI and database name
// MongoDB connection URI and database name
const uri = process.env.MONGODB_URI || "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=myReplicaSet";
const databaseName = process.env.DATABASE_NAME || "homiee";


async function syncCollections() {
  console.debug(process.env.MONGODB_URI)
  const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });
  try {
    await client.connect();
    console.log('Connected to the database', databaseName);

    const database = client.db(databaseName);

    // Create a change stream on the properties collection
    const changeStream = database.collection('philip_test_propertyV3').watch();

    // Listen for changes in the properties collection
    changeStream.on('change', async (change) => {
      if (change.operationType === 'update') {
        const propertyId = change.documentKey._id;

        // Retrieve the updated document
        const updatedProperty = await database.collection('philip_test_propertyV3').findOne({ _id: propertyId });

        // Extract relevant information
        const status = updatedProperty.status;
        const type = updatedProperty.type;
        const suburb = updatedProperty.address.geometricShape.suburb;

        const buildingIds = [];
        const geometricShape = updatedProperty.address.geometricShape;

        const buildingId = updatedProperty.address.geometricShape.polygon
          ? updatedProperty.address.geometricShape.polygon.id
          : null;

        if (buildingId !== null) {
          buildingIds.push(buildingId)
        }

        if (geometricShape && Array.isArray(geometricShape.building_parts)) {
          geometricShape.building_parts.forEach(buildingPart => {
            buildingIds.push(buildingPart.id);
          });
        }


        updatedProperty.transactionDetails.soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));
        const mostRecentSale = updatedProperty.transactionDetails.soldHistory[updatedProperty.transactionDetails.soldHistory.length - 1];
        const price = mostRecentSale?.price;

        if (type !== 'UNIT' && type !== 'SE' && type !== 'SHOP' && buildingId !== null) {
          


          // Update document in philip_test_properties_mapV3 collection
          await database.collection('philip_test_properties_mapV3').updateOne(
            { _id: propertyId },
            {
              $set: {
                _id: propertyId,
                type: type,
                status: status,
                price: price || null,
                suburb: suburb,
                buildingIds: buildingIds
              },
            },
            { upsert: true }
          );

          console.log(`Synced property ${propertyId} with philip_test_properties_mapV3`);

        }


      } else if (change.operationType === 'insert') {
        const propertyId = change.fullDocument._id;

        // Extract relevant information
        const status = change.fullDocument.status;
        const type = change.fullDocument.type;
        const suburb = change.fullDocument.address.geometricShape.suburb || null;

        const buildingIds = [];
        const geometricShape = change.fullDocument.address.geometricShape;

        const buildingId = change.fullDocument.address.geometricShape.polygon
          ? change.fullDocument.address.geometricShape.polygon.id
          : null;

        if (buildingId !== null) {
          buildingIds.push(buildingId)
        }

        if (geometricShape && Array.isArray(geometricShape.building_parts)) {
          geometricShape.building_parts.forEach(buildingPart => {
            buildingIds.push(buildingPart.id);
          });
        }

        // Price
        change.fullDocument.transactionDetails.soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));
        const mostRecentSale = change.fullDocument.transactionDetails.soldHistory[change.fullDocument.transactionDetails.soldHistory.length - 1];
        const price = mostRecentSale?.price;

        if (type !== 'UNIT' && type !== 'SE' && type !== 'SHOP' && buildingId !== null) {
          // Insert document into philip_test_properties_mapV3 collection
          await database.collection('philip_test_properties_mapV3').updateOne(
            { _id: propertyId },
            {
              $set: {
                _id: propertyId,
                type: type,
                status: status,
                price: price || null,
                suburb: suburb,
                buildingIds: buildingIds,
              },
            },
            { upsert: true }
          );

          console.log(`Added new property ${propertyId} to philip_test_properties_mapV3`);
        }

      } else if (change.operationType === 'delete') {
        const propertyId = change.documentKey._id;

        // Delete document from philip_test_properties_mapV3 collection
        await database.collection('philip_test_properties_mapV3').deleteOne({ _id: propertyId });

        console.log(`Deleted property ${propertyId} from philip_test_properties_mapV3`);
      }
    });

    console.log('Change stream started. Listening for property updates, inserts, and deletes...');
  } finally {
    // You may want to handle cleanup and closing of resources here
    // For example, closing the MongoDB client
    // await client.close();
    console.log('Connection closed');
  }
}

// Call the synchronization function
syncCollections();

