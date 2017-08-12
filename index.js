
var tape = require('tape')

var data = [
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}},
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}},
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}}
]

module.exports = function (create) {

  var seed = Date.now()
  var filename = '/tmp/test-flumeview-index_'+seed+'/'
  var db = create(filename, seed)

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

  tape('close', function (t) {
    db.close(t.end)
  })

  tape('reload', function (t) {
    db = create(filename, seed)
    t.end()
  })

  tape('retest', test)

}


