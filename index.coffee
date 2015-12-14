'use strict';
util           = require 'util'
{EventEmitter} = require 'events'
debug          = require('debug')('meshblu-mongodb-forwarder')
mongojs        = require 'mongojs'

MESSAGE_SCHEMA = {
  type: 'object'
  properties:
    collection:
      type: 'string'
      required: true
    exampleString:
      type: 'string'
      required: true
}


OPTIONS_SCHEMA = {
  type: 'object'
  properties:
    host:
      type: 'string'
      required: true
    port:
      type: 'string'
      required: true
    username:
      type: 'string'
      required: false
    password:
      type: 'string'
      required: false
    database:
      type: 'string'
      required: true
    collection:
      type: 'string'
      required: true
}
DEFAULT_OPTIONS = {
  host : "127.0.0.1"
  port : "27017"
  database: "octoblu"
  collection: "iotdata"
}

class Plugin extends EventEmitter
  constructor: ->
    @options = DEFAULT_OPTIONS
    @messageSchema = MESSAGE_SCHEMA
    @optionsSchema = OPTIONS_SCHEMA

  onMessage: (message) =>
    console.log "message received", message
    return unless message
    @getConnection((error, collection) =>
      console.log "Connected to database"
      collection.insert message, (dbError, result) =>
        return @emit('error', dbError) if dbError
        console.log "Record inserted", result
        response =
          devices : ["*"]
          topic : "mongo-insert"
          result: result
        @emit 'message', response
    )


  onConfig: (device) =>
    console.log "Device", device
    @setOptions device.options || DEFAULT_OPTIONS
    @getConnection((error, collection)=>
      console.log "There was an error", error if error
      console.log "Successfully Connected to MongoDB"
    )


  setOptions: (options=DEFAULT_OPTIONS) =>
    @options = options
  getConnectionString: ()=>
    connectionString = "mongodb://"
    connectionString += "#{@options.username}:#{@options.password}@" if @options.username? and @options.password?
    connectionString += "#{@options.host}:#{@options.port}" if @options.host? and @options.port?
    connectionString += "/#{@options.database}" if @options.database?

  getConnection:(callback) =>
    connectionString = @getConnectionString()
    console.log "Connection String is", connectionString
    console.log "Options", @options
    if not @db
      @db = mongojs(connectionString)
      @collection = @db.collection @options.collection
    callback null, @collection

module.exports =
  messageSchema: MESSAGE_SCHEMA
  optionsSchema: OPTIONS_SCHEMA
  Plugin: Plugin
