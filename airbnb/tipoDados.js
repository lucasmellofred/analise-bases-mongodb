db.listingsAndReviews.aggregate([
  {
    $project: {
      fieldNamesAndTypes: {
        $map: {
          input: { $objectToArray: "$$ROOT" },
          as: "field",
          in: {
            k: "$$field.k",
            v: { $type: "$$field.v" }
          }
        }
      },
      fieldCount: { $size: { $objectToArray: "$$ROOT" } }
    }
  },
  {
    $unwind: "$fieldNamesAndTypes"
  },
  {
    $group: {
      _id: "$fieldNamesAndTypes.k",
      types: { $addToSet: "$fieldNamesAndTypes.v" }
    }
  },
  {
    $group: {
      _id: null,
      fieldTypes: {
        $push: {
          k: "$_id",
          v: "$types"
        }
      },
      averageFieldCount: { $avg: "$fieldCount" }
    }
  },
  {
    $project: {
      _id: 0,
      fieldTypes: {
        $arrayToObject: "$fieldTypes"
      },
      averageFieldCount: 1
    }
  }
]).forEach(function(doc) {
  printjson(doc);
});
