
var tape = require('tape')

var data = [
  {key: Math.random(), value: {foo: true, bar: Date.now()}},
  {key: Math.random(), value: {foo: true, bar: Date.now()}},
  {key: Math.random(), value: {foo: true, bar: Date.now()}}
]

module.exports = function (create) {

  var seed = Date.now()
  var db = create(seed)

  tape('simple', function (t) {
    db.append(data, function (err, m) {
      if(err) throw err
      t.end()
    })
  })

  function test (t) {
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
  }

  tape('test', test)

  tape('reload', function (t) {
    var db = create(seed)
    t.end()
  })

  tape('retest', test)

}











