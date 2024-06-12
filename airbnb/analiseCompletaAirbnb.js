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
    $facet: {
      fieldTypes: [
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
                v: { $arrayElemAt: ["$types", 0] } // Ajuste aqui
              }
            }
          }
        },
        {
          $project: {
            _id: 0,
            fieldTypes: {
              $arrayToObject: "$fieldTypes"
            }
          }
        }
      ],
      fieldCounts: [
        {
          $group: {
            _id: null,
            averageFieldCount: { $avg: "$fieldCount" },
            maxFieldCount: { $max: "$fieldCount" },
            minFieldCount: { $min: "$fieldCount" }
          }
        },
        {
          $project: {
            _id: 0,
            averageFieldCount: 1,
            maxFieldCount: 1,
            minFieldCount: 1
          }
        }
      ]
    }
  },
  {
    $project: {
      fieldTypes: { $arrayElemAt: ["$fieldTypes.fieldTypes", 0] },
      fieldCounts: { $arrayElemAt: ["$fieldCounts", 0] }
    }
  },
  {
    $project: {
      fieldTypes: {
        $objectToArray: "$fieldTypes"
      },
      fieldCounts: 1
    }
  },
  {
    $unwind: "$fieldTypes"
  },
  {
    $sort: {
      "fieldTypes.k": 1
    }
  },
  {
    $group: {
      _id: null,
      fieldTypes: { $push: "$fieldTypes" },
      fieldCounts: { $first: "$fieldCounts" }
    }
  }
]).forEach(function(doc) {
  printjson(doc);
});
