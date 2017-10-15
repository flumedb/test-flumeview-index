var pull = require('pull-stream')

var tape = require('tape')


module.exports = function (create, N) {
  N = N || 10
  var data = []
  for(var i = 0; i < N; i++)
    data.push({key: '#'+Math.random(), value: {foo: true, bar: Date.now(), i: i}})

  var seed = Date.now()
  var filename = '/tmp/test-flumeview-index_'+seed+'/'
  var db = create(filename, seed)

  tape('simple', function (t) {
    db.append(data, function (err, m) {
      if(err) throw err
      t.end()
    })
  })

  function all (opts, cb) {
    pull(
      db.index.read(opts),
      pull.collect(cb)
    )
  }

  function test (t) {
    all({}, function (err, ary) {
      all({keys: true, values: false}, function (err, _ary) {
        t.deepEqual(_ary, ary.map(function (e) { return {key: e.key, seq: e.seq} }))
        all({keys: false, values: true}, function (err, _ary) {
          t.deepEqual(_ary, ary.map(function (e) { return {seq: e.seq, value: e.value} }))
          all({values: true, seqs: false, keys: false}, function (err, _ary) {
            t.deepEqual(_ary, ary.map(function (e) { return e.value }))
            t.end()
          })
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


