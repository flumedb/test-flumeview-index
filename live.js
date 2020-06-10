var pull = require('pull-stream')
var tape = require('tape')
const os = require('os')
const path = require('path')

var data = [
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}},
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}},
  {key: '#'+Math.random(), value: {foo: true, bar: Date.now()}}
]

module.exports = function (create) {

  var seed = Date.now()
  var filename = path.join(os.tmpdir(), `test-flumeview-index-${seed}`)
  var db = create(filename, seed)

  var live = []

  pull(
    db.index.read({live: true, sync: false}),
    pull.drain(function (data) { live.push(data) })
  )

  tape('simple', function (t) {
    db.append(data, function (err, m) {
      if(err) throw err
      pull(
        db.index.read({}),
        pull.collect(function (err, ary){
          t.deepEqual(live.sort(function (a, b) {
            return a.key < b.key ? -1 : a.key > b.key ? 1 : 0
          }), ary)
          t.end()
        })
      )
    })
  })

  tape('close', function (t) {
    db.close(t.end)
  })

}


