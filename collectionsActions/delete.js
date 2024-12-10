const handleDelete = async (database, changeEvent) => {
    const propertyId = changeEvent.documentKey._id;

    // Delete from the second collection
    await database.collection('philip_test_properties_mapV3').deleteOne({ _id: propertyId });
    console.log(`Deleted property ${propertyId} from philip_test_properties_mapV3`);
}

module.exports = {handleDelete};