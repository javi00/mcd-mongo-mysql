db.actor.aggregate([

  {
    $lookup: {
      from: "film_actor",
      localField: "_id",
      foreignField: "actor_id",
      as: "actor_films"
    }
  },
  { $unwind:"$actor_films" },
  {
    $lookup: {
      from: "film_category",
      localField: "actor_films.film_id",
      foreignField: "film_id",
      as: "film_cat"
    }
  },
  { $unwind:"$film_cat" },
  {
    $lookup: {
      from: "category",
      localField: "film_cat.category_id",
      foreignField: "_id",
      as: "categories"
    }
  },
  {
    $match: {
      "categories.name": "Comedy"
    }
  },
  {
    $group: {
      _id: {
        actor_id: "$_id",
        first_name: "$first_name",
        last_name: "$last_name"
      },
      comedy_film_count: {
        $sum: {
          $cond: {
            if: {
              $in: ["Comedy", "$categories.name"]
            },
            then: 1,
            else: 0
          }
        }
      }
    }
  },
  {
    $project: {
      _id: 0,
      first_name: "$_id.first_name",
      last_name: "$_id.last_name",
      comedy_film_count: 1
    }
  },

  {
    $sort: {
      comedy_film_count: -1
    }
  },
  {
    $limit: 10
  }
  ])
