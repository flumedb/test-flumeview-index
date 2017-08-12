module.exports = function (create) {

  function Timer (name) {
    var start = Date.now()
    return function (ops) {
      var seconds = (Date.now() - start)/1000
      console.log(name, ops, ops/seconds)
    }
  }

  function initialize (db, N, cb) {
    var data = []
    for(var i = 0; i < N; i++)
      data.push({key: '#'+i, value: {
        foo: Math.random(), bar: Date.now()
      }})

    var t = Timer('append')
    db.append(data, function (err, offset) {
      //wait until the view is consistent!
      db.index.since(function (v) {
        if(v < offset) return
        cb(null, N)
      })
    })
  }

  function ordered (db, N, cb) {
    //ordered reads
    var n = 0
    for(var i = 0; i < N; i++) {
      db.index.get('#'+i, next)
    }

    function next (err, v) {
      if(++n == N) cb(null, N)
    }
  }

  function random (db, N, cb) {
    ;(function get(i) {
      if(i >= N) return cb(null, N)

      db.index.get('#'+~~(Math.random()*N), function (err, data) {
        setImmediate(function () { get(i+1) })
      })

    })(0)

  }

  var seed = Date.now()
  var file = '/tmp/test-flumeview-index_'+seed+'/'

  var db = create(file, seed)
  var N = 10000
  var t = Timer('append')
  initialize(db, N, function (err, n) {
    t(n)
    t = Timer('ordered_cached')
    ordered(db, N, function (err, n) {
      t(n)
      t = Timer('random_cached')
      random(db, N, function (err, n) {
        t(n)
        t = Timer('ordered')
        db.close(function () {
          db.close(function () {
            var db = create(file)
            ordered(db, N, function (err, n) {
              t(n)
              t = Timer('random_cached')
              db.close(function () {
                var db = create(file)
                random(db, N, function (err, n) {
                  t(n)
                })
              })
            })
          })
        })
      })
    })
  })

}

