const handleUpdate = async (database, changeEvent) => {
    const propertyId = changeEvent.documentKey._id;
    const updatedFields = changeEvent.updateDescription?.updatedFields || {};

    // Check if no fields are updated
    if (Object.keys(updatedFields).length === 0) {
        console.log(`No relevant changes for property ${propertyId}. Skipping update.`);
        return;
    }

    const updatedProperty = await database.collection('philip_test_propertyV3').findOne({ _id: propertyId });
    if (!updatedProperty) {
        console.error(`Property with ID ${propertyId} not found.`);
        return;
    }

    const { status, type, address, transactionDetails } = updatedProperty;
    const suburb = address?.geometricShape?.suburb || null;
    const buildingIds = [];

    if (address?.geometricShape?.polygon?.id) {
        buildingIds.push(address.geometricShape.polygon.id);
    }

    if (Array.isArray(address?.geometricShape?.building_parts)) {
        address.geometricShape.building_parts.forEach(part => buildingIds.push(part.id));
    }

    const soldHistory = transactionDetails?.soldHistory || [];
    soldHistory.sort((a, b) => new Date(a.contractDate) - new Date(b.contractDate));
    const price = soldHistory.length > 0 ? soldHistory[soldHistory.length - 1].price : null;

    // Check if any relevant fields have changed
    const updatePayload = {};
    if (updatedFields.type !== undefined) updatePayload.type = type || null;
    if (updatedFields.status !== undefined) updatePayload.status = status || null;
    if (updatedFields['address.geometricShape.suburb'] !== undefined) updatePayload.suburb = suburb;
    if (updatedFields['address.geometricShape.polygon.id'] !== undefined ||
        updatedFields['address.geometricShape.building_parts'] !== undefined) {
        updatePayload.buildingIds = buildingIds.length > 0 ? buildingIds : null;
    }
    if (updatedFields['transactionDetails.soldHistory'] !== undefined) updatePayload.price = price;

    // Skip update if no relevant fields are present in the update payload
    if (Object.keys(updatePayload).length === 0) {
        console.log(`No relevant changes detected for property ${propertyId}. Skipping update.`);
        return;
    }

    // Update the second collection
    await database.collection('philip_test_properties_mapV3').updateOne(
        { _id: propertyId },
        { $set: updatePayload },
        { upsert: true }
    );

    console.log(`Updated property ${propertyId} in philip_test_properties_mapV3`);
};

module.exports = { handleUpdate };
