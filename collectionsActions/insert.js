 const handleInsert = async (database, changeEvent) => {
    const propertyId = changeEvent.documentKey._id;

    const newProperty = await database.collection('philip_test_propertyV3').findOne({ _id: propertyId });
    if (!newProperty) {
        console.error(`Newly inserted property with ID ${propertyId} not found.`);
        return;
    }

    // Extract fields similar to `handleUpdate`

     const { status, type, suburb, buildingIds, price } = newProperty;
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
    // const price = soldHistory.length > 0 ? soldHistory[soldHistory.length - 1].price : null;

    // Insert into the second collection
    await database.collection('philip_test_properties_mapV3').insertOne({
        _id: propertyId,
        type: type || null,
        status: status || null,
        price: price,
        suburb: suburb,
        buildingIds: buildingIds.length > 0 ? buildingIds : null,
    });

    console.log(`Inserted new property ${propertyId} into philip_test_properties_mapV3`);
}

module.exports = {handleInsert};