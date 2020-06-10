var Flume = require('flumedb')
var Log = require('flumelog-offset')
var Index = require('flumeview-level')
var codec = require('flumecodec')

require('./')(function (file, seed) {
  return Flume(Log(file + '/log.offset', 1024, codec.json)).use(
    'index',
    Index(1, function (e) {
      console.log(e)
      return [e.key]
    })
  )
})
