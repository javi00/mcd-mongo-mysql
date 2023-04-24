
db.film.aggregate([
    {$lookup: {
        from: "language",
        localField: "language_id",
        foreignField: "_id",
        as: "lan"}},
//       {$group: {_id: "$lan.name", count: { $count: 1 }}}
/*    
{$unwind: "$lan"},
    
    {
       $sort: {
          count: -1
        }
    }
    */
]);