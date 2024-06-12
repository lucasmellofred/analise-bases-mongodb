db.listingsAndReviews.aggregate([
  {
    $project: {
      fieldCount: { $size: { $objectToArray: "$$ROOT" } }
    }
  },
  {
    $group: {
      _id: null,
      averageFieldCount: { $avg: "$fieldCount" },
      maxFieldCount: { $max: "$fieldCount" },
      minFieldCount: { $min: "$fieldCount" }
    }
  }
]).forEach(printjson);
