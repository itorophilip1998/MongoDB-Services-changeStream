const handleUpdate = async (database, changeEvent) => {
    const propertyId = changeEvent.documentKey._id;

    // Fetch the updated property document
    const updatedProperty = await database.collection('philip_test_propertyV3').findOne({ _id: propertyId });
    if (!updatedProperty) {
        console.error(`Property with ID ${propertyId} not found.`);
        return;
    }

    const { status, type, suburb, buildingIds, price } = updatedProperty;

    // Extract data for the update
    // const suburb = address?.geometricShape?.suburb || null;
    // const buildingIds = []; 

    // if (address?.geometricShape?.polygon?.id) {
    //     buildingIds.push(address.geometricShape.polygon.id);
    // }

    // if (Array.isArray(address?.geometricShape?.building_parts)) {
    //     address.geometricShape.building_parts.forEach(part => buildingIds.push(part.id));
    // }

    // const soldHistory = transactionDetails?.soldHistory || [];
    // soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));

    // Construct the payload for forced update
    const updatePayload = {
        type: type || null,
        status: status || null,
        suburb: suburb,
        buildingIds: buildingIds.length > 0 ? buildingIds : null,
        price: price,
    };

    console.log(updatePayload)

    // Update the target collection (forcefully update all fields)
    await database.collection('philip_test_properties_mapV3').updateOne(
        { _id: propertyId },
        { $set: updatePayload },
        { upsert: true }
    );

    console.log(`Forcefully updated property ${propertyId} in philip_test_properties_mapV3`);
};

module.exports = { handleUpdate };
