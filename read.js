var pull = require('pull-stream')
var compare = require('typewise').compare
var tape = require('tape')


module.exports = function (create, N) {
  N = N || 10
  var data = []
  for(var i = 0; i < N; i++)
    data.push({key: [~~(Math.random()*10), '#'+Math.random()], value: {foo: true, bar: Date.now(), i: i}})

  var seed = Date.now()
  var filename = '/tmp/test-flumeview-index_'+seed+'/'
  var db = create(filename, seed)

  var sorted = data.slice().sort(function (a, b) {
    return compare(a.key, b.key)
  })


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
    all({old: true, live: false}, function (err, ary) {
      t.deepEqual(ary.map(function (e) { return {key:e.value.key, value:e.value.value}}), sorted)
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

  tape('empty close 1', function (t) {
    var db = create(filename+2, seed)
    pull(
      db.index.read({}),
      pull.collect(function (err, ary) {
        //ignore err. might be an error or not,
        //depending if read() was called before empty log loaded.
        t.deepEqual(ary, [])
        t.end()
      })
    )
    setTimeout(function () {
      db.close(function () {

      })
    })
  })

  tape('empty close 2', function (t) {
    var db = create(filename+2, seed)
    pull(
      db.index.read({}),
      pull.collect(function (_, ary) {
        //ignore err. might be an error or not,
        //depending if read() was called before empty log loaded.
        t.deepEqual(ary, [])
        t.end()
      })
    )
    setTimeout(function () {
      db.close(function () {})
    })
  })
  tape('empty close 3', function (t) {
    var db = create(filename+3, seed)
    pull(
      db.index.read({}),
      pull.collect(function (err, ary) {
        //should definitely be an error because db.close is called sync
        t.ok(err)
        t.deepEqual(ary, [])
        t.end()
      })
    )
    db.close(function () {})
  })
  tape('empty close 4', function (t) {
    var db = create(filename+4, seed)
    pull(
      db.index.read({}),
      pull.collect(function (err, ary) {
        //should not be an error because db.close not called yet.
        t.notOk(err)
        t.deepEqual(ary, [])
        t.end()
        db.close(function () {})
      })
    )
  })

}




