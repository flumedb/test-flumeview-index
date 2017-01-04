
var tape = require('tape')

var data = [
  {key: Math.random(), value: {foo: true, bar: Date.now()}},
  {key: Math.random(), value: {foo: true, bar: Date.now()}},
  {key: Math.random(), value: {foo: true, bar: Date.now()}}
]

module.exports = function (db) {

  tape('simple', function (t) {
    db.append(data, function (err, m) {
      if(err) throw err
      db.index.get(data[0].key, function (err, value) {
        if(err) throw err
        t.deepEqual(value, data[0])
        db.index.get(data[1].key, function (err, value) {
          if(err) throw err
          t.deepEqual(value, data[1])
          db.index.get(data[2].key, function (err, value) {
            if(err) throw err
            t.deepEqual(value, data[2])
            t.end()
          })
        })
      })
    })
  })

}







