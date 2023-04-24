db.actor.aggregate([
    {
      $lookup: {
        from: "film_actor",
        localField: "_id",
        foreignField: "actor_id",
        as: "films"
      }
    },
    {
      $addFields: {
        film_count: { $size: "$films" }
      }
    },
    {
      $match: {
        film_count: { $gt: 35 }
      }
    },
    {
      $project: {
        _id: 0,
        first_name: 1,
        last_name: 1,
        film_count: 1
      }
    }
  ]);
