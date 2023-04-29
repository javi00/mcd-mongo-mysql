




var mapF = function() {emit(this.language_id, 1);};
var reduceF = function(key, values) {return Array.sum(values);};
db.film.mapReduce(
    mapF,
    reduceF,
    {out: "counts"});

db.counts.find({}, {_id:1, value:1}).sort({value:-1})


