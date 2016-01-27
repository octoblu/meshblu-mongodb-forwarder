'use strict';
util           = require 'util'
{EventEmitter} = require 'events'
debug          = require('debug')('meshblu-mongodb-forwarder')
mongojs        = require 'mongojs'

MESSAGE_SCHEMA = {
  type: 'object'
  properties:
    action:
      type: 'string'
    query:
      type: 'string'
    update:
      type: 'string'
    value:
      type: 'string'
}

ACTION_MAP = [
  {
    'value': 'find'
    'name': 'Find'
  }
  {
    'value': 'findAndModify'
    'name': 'Find and Modify'
  }
  {
    'value': 'insert'
    'name': 'Insert'
  }
]

MESSAGE_FORM_SCHEMA = [
  {
    'key': 'action'
    'type': 'select'
    'titleMap': ACTION_MAP
  }
  {
    'key': 'query'
    'condition': "model.action == 'find' || model.action == 'findAndModify'"
  }
  {
    'key': 'update'
    'condition': "model.action == 'findAndModify'"
  }
  {
    'key': 'value'
    'condition': "model.action == 'insert'"
  }
]

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
    @messageFormSchema = MESSAGE_FORM_SCHEMA
    @optionsSchema = OPTIONS_SCHEMA

  onMessage: (message) =>
    console.log "message received", message
    return unless message
    { collection, action } = message.payload
    @getConnection((error, collection) =>
      console.log "Connected to database"

      switch action
        when "find" then @find(message, collection)
        when "findAndModify" then @findAndModify(message, collection)
        when "insert" then @insert(message, collection)
    )

  find: (message, collection) =>
    self = @
    {query} = message.payload
    query = JSON.parse(query) if typeof query is 'string'

    collection.find query, (err, doc) ->
      return if !doc
      response =
        devices : ["*"]
        topic : "mongo-find"
        result: doc
      self.emit 'message', response

  findAndModify: (message, collection) =>
    self = @
    {query, update} = message.payload
    query = JSON.parse(query) if typeof query is 'string'
    update = JSON.parse(update) if typeof update is 'string'
    collection.findAndModify {
      query: query
      update: update
      new: true
    }, (err, doc, lastErrorObject) ->
      response =
        devices : ["*"]
        topic : "mongo-findAndModify"
        result: doc
      self.emit 'message', response


  insert: (message, collection) =>
    self = @
    {value} = message.payload
    value = JSON.parse(value) if typeof value is 'string'
    collection.insert value, (dbError, result) =>
      return @emit('error', dbError) if dbError
      console.log "Record inserted", result
      response =
        devices : ["*"]
        topic : "mongo-insert"
        result: result
      self.emit 'message', response

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
  messageFormSchema: MESSAGE_FORM_SCHEMA
  optionsSchema: OPTIONS_SCHEMA
  Plugin: Plugin
