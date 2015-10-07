var assign = require('lodash.assign'),
  bind = require('lodash.bind'),
  isEmpty = require('lodash.isempty'),
  jsondiffpatch = require('jsondiffpatch'),

  COMMANDS = require('./commands'),
  utils = require('./utils'),
  Server;

Server = function(adapter, transport, diffOptions) {
  if (!(adapter && transport)) {
    throw new Error('Need to specify an adapter and a transport');
  }
  if (!diffOptions) {
    diffOptions = {};
  }

  this.adapter = adapter;
  this.transport = transport;
  this.data = {};
  this.requests = {};
  this.saveRequests = {};
  this.saveQueue = {};

  // bind functions
  this.trackConnection = bind(this.trackConnection, this);

  // set up the jsondiffpatch options
  // see here for options: https://github.com/benjamine/jsondiffpatch#options
  diffOptions = assign({
    objectHash: function(obj) {
      return obj.id || obj._id || JSON.stringify(obj);
    }
  }, diffOptions);

  this.jsondiffpatch = jsondiffpatch.create(diffOptions);

  this.transport.on('connection', this.trackConnection);
};

/**
 * Registers the correct event listeners
 * @param  {Connection} connection The connection that should get tracked
 */
Server.prototype.trackConnection = function(connection) {
  connection.on(COMMANDS.join, this.joinConnection.bind(this, connection));
  connection.on(COMMANDS.syncWithServer, this.receiveEdit.bind(this, connection));
};

/**
 * Joins a connection to a room and send the initial data
 * @param  {Connection} connection
 * @param  {String} room             room identifier
 * @param  {Function} initializeClient Callback that is being used for initialization of the client
 */
Server.prototype.joinConnection = function(connection, room, initializeClient) {
  this.getData(room, connection.userid, function(error, data) {
    // connect to the room
    connection.join(room);

    // set up the client version for this socket
    // each connection has a backup and a shadow
    // and a set of edits
    data.clientVersions[connection.id] = {
      backup: {
        doc: utils.deepCopy(data.serverCopy),
        serverVersion: 0
      },
      shadow: {
        doc: utils.deepCopy(data.serverCopy),
        serverVersion: 0,
        localVersion: 0
      },
      edits: []
    };

    // send the current server version
    initializeClient(data.serverCopy);
  });
};

/**
 * Reset all queues / cached data
 * @param  {Function} callback The function that should be called when complete
 */
Server.prototype.reset = function(callback) {
  var waitForDiffSyncServer = function() {
    var pending = false;
    for (room in this.saveRequests) {
      if (this.saveRequests[room] === true) {
        pending = true;
      }
    }
    if (pending) {
      setTimeout(waitForDiffSyncServer, 1000);
    } else {
      this.data = {};
      this.requests = {};
      this.saveRequests = {};
      this.saveQueue = {};
      callback();
    }
  }.bind(this);
  waitForDiffSyncServer();
};

/**
 * Gets data for a room from the internal cache or from the adapter
 * @param  {String}   room     room identifier
 * @param  {String}   userid   The id of the user who made the requests
 * @param  {Function} callback notifier-callback
 */
Server.prototype.getData = function(room, userid, callback) {
  var cachedVersion = this.data[room],
    cache = this.data,
    requests = this.requests;

  if (cachedVersion) {
    callback(null, cachedVersion);
  } else {
    // if there is no request for this room
    // ask the adapter for the data
    // do nothing in the else case because this operation
    // should only happen once
    if (!requests[room]) {
      requests[room] = true;
      this.adapter.getData(room, userid, function(error, data) {
        cache[room] = {
          registeredSockets: [],
          clientVersions: {},
          serverCopy: data
        };

        requests[room] = false;
        callback(null, cache[room]);
      });
    } else {
      requests[room] = true;
    }
  }
};

/**
 * Applies the sent edits to the shadow and the server copy, notifies all connected sockets and saves a snapshot
 * @param  {Object} connection   The connection that sent the edits
 * @param  {Object} editMessage  The message containing all edits
 * @param  {Function} sendToClient The callback that sends the server changes back to the client
 */
Server.prototype.receiveEdit = function(connection, editMessage, sendToClient) {
  this.getData(editMessage.room, connection.userid, function(err, doc) {
    if (doc) {
      this.adapter.checkDiffs(editMessage, doc, function(err, res) {
        if (res) {
          // -1) The algorithm actually says we should use a checksum here, I don't think that's necessary
          // 0) get the relevant doc
          // 0.a) get the client versions
          var clientDoc = doc.clientVersions[connection.id];

          // no client doc could be found, client needs to re-auth
          if (err || !clientDoc) {
            connection.emit(COMMANDS.error, 'Need to re-connect!');
            return;
          }

          // when the versions match, remove old edits stack
          if (editMessage.serverVersion === clientDoc.shadow.serverVersion) {
            clientDoc.edits = [];
          }

          // 1) iterate over all edits
          editMessage.edits.forEach(function(edit) {
            // 2) check the version numbers
            if (edit.serverVersion === clientDoc.shadow.serverVersion &&
              edit.localVersion === clientDoc.shadow.localVersion) {
              // versions match
              // backup! TODO: is this the right place to do that?
              clientDoc.backup.doc = utils.deepCopy(clientDoc.shadow.doc);

              // 3) patch the shadow
              // var snapshot = utils.deepCopy(clientDoc.shadow.doc);
              this.jsondiffpatch.patch(clientDoc.shadow.doc, utils.deepCopy(edit.diff));
              // clientDoc.shadow.doc = snapshot;

              // apply the patch to the server's document
              // snapshot = utils.deepCopy(doc.serverCopy);
              this.jsondiffpatch.patch(doc.serverCopy, utils.deepCopy(edit.diff));
              // doc.serverCopy = snapshot;

              // 3.a) increase the version number for the shadow if diff not empty
              if (!isEmpty(edit.diff)) {
                clientDoc.shadow.localVersion++;
              }
            } else {
              // TODO: implement backup workflow
              // has a low priority since `packets are not lost` - but don't quote me on that :P
              console.log('error', 'patch rejected!!', edit.serverVersion, '->', clientDoc.shadow.serverVersion, ':',
                edit.localVersion, '->', clientDoc.shadow.localVersion);
            }
          }.bind(this));

          // 4) save a snapshot of the document
          this.saveSnapshot(editMessage.room, editMessage.edits, connection.userid);

          // notify all sockets about the update, if not empty
          if (editMessage.edits.length > 0) {
            this.transport.to(editMessage.room).emit(COMMANDS.remoteUpdateIncoming, connection.id);
          }

          this.sendServerChanges(doc, clientDoc, sendToClient);
        } else {
          //Do nothing - something in those diffs isn't allowed.
        }
      }.bind(this));
    }
  }.bind(this));
};

Server.prototype.saveSnapshot = function(room, edits, userid) {
  var noRequestInProgress = !this.saveRequests[room],
    checkQueueAndSaveAgain = function() {
      // if another save request is in the queue, save again
      var anotherRequestScheduled = this.saveQueue[room] === true;
      this.saveRequests[room] = false;
      if (anotherRequestScheduled) {
        this.saveQueue[room] = false;
        this.saveSnapshot(room, edits, userid);
      }
    }.bind(this);

  // only save if no save going on at the moment
  if (noRequestInProgress) {
    this.saveRequests[room] = true;
    // get data for saving
    this.getData(room, userid, function(err, data) {
      // store data
      if (!err && data) {
        this.adapter.storeData(room, userid, data.serverCopy, edits, checkQueueAndSaveAgain);
      } else {
        checkQueueAndSaveAgain();
      }
    }.bind(this));
  } else {
    // schedule a new save request
    this.saveQueue[room] = true;
  }
};

Server.prototype.sendServerChanges = function(doc, clientDoc, send) {
  // create a diff from the current server version to the client's shadow
  var diff = this.jsondiffpatch.diff(clientDoc.shadow.doc, doc.serverCopy);
  var basedOnServerVersion = clientDoc.shadow.serverVersion;

  // add the difference to the server's edit stack
  if (!isEmpty(diff)) {
    clientDoc.edits.push({
      serverVersion: basedOnServerVersion,
      localVersion: clientDoc.shadow.localVersion,
      diff: diff
    });
    // update the server version
    clientDoc.shadow.serverVersion++;

    // apply the patch to the server shadow
    this.jsondiffpatch.patch(clientDoc.shadow.doc, utils.deepCopy(diff));
  }

  // we explicitly want empty diffs to get sent as well
  send({
    localVersion: clientDoc.shadow.localVersion,
    serverVersion: basedOnServerVersion,
    edits: clientDoc.edits
  });
};

module.exports = Server;