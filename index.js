
/**
 * Module dependencies.
 */

var uid2 = require('uid2');
var redis = require('ioredis');
var msgpack = require('msgpack-js');
var Adapter = require('socket.io-adapter');
var debug = require('debug')('socket.io-ioredis');

/**
 * Module exports.
 */

module.exports = adapter;

/**
 * Returns a redis Adapter class.
 *
 * @param {String} optional, redis uri
 * @return {RedisAdapter} adapter
 * @api public
 */

function adapter(uri, opts){
  opts = opts || {};

  // handle options only
  if ('object' == typeof uri) {
    opts = uri;
    uri = null;
  }

  // handle uri string
  if (uri) {
    uri = uri.split(':');
    opts.host = uri[0];
    opts.port = uri[1];
  }

  // opts
  var host = opts.host || '127.0.0.1';
  var port = Number(opts.port || 6379);
  var pub = opts.pubClient;
  var sub = opts.subClient;
  var prefix = opts.key || 'socket.io';

  // init clients if needed
  if (!pub) pub = new redis(port, host);
  if (!sub) sub = new redis(port, host);

  // this server's key
  var uid = uid2(6);

  /**
   * Adapter constructor.
   *
   * @param {String} namespace name
   * @api public
   */

  function Redis(){
    Adapter.apply(this, arguments);

    this.uid = uid;
    this.prefix = prefix;

    this.setupRedis();
  }

  Redis.prototype.setupRedis = function() {
    sub.subscribe(this.getChannelName());
    sub.on('messageBuffer', this.onmessage.bind(this));
  };

  Redis.prototype.getChannelName = function() {
    var args = Array.prototype.slice.call(arguments);
    args.unshift(prefix, this.nsp.name);
    return args.join('#') + '#';
  };

  /**
   * Inherits from `Adapter`.
   */

  Redis.prototype.__proto__ = Adapter.prototype;

  /**
   * Called with a subscription message
   *
   * @api private
   */

  Redis.prototype.onmessage = function(channel, msg){
    var args = msgpack.decode(msg);
    var packet, opts;

    if (uid == args.shift()) {
      return debug('ignore same uid');
    }

    packet = args[0];
    opts = args[1];

    if (packet && packet.nsp === undefined) {
      packet.nsp = '/';
    }

    if (!packet || packet.nsp != this.nsp.name) {
      return debug('ignore different namespace');
    }

    Adapter.prototype.broadcast.call(this, packet, opts);
  };

  /**
   * Broadcasts a packet.
   *
   * @param {Object} packet to emit
   * @param {Object} options
   * @param {Boolean} whether the packet came from another node
   * @api public
   */

  Redis.prototype.broadcast = function(packet, opts){
    Adapter.prototype.broadcast.call(this, packet, opts);
    var msg = msgpack.encode([uid, packet, opts]);
    if (opts.rooms) {
      var self = this;
      opts.rooms.forEach(function(room) {
        runCommand('publish', self.getChannelName(room), msg);
      });
    } else {
      runCommand('publish', this.getChannelName(), msg);
    }
  };

  function runCommand(type) {
    var redisArgs = Array.prototype.slice.call(arguments, 1);
    debug('running redis command', type, redisArgs);
    if (type === 'publish') {
      pub.publish.apply(pub, redisArgs);
    } else {
      sub[type].apply(sub, redisArgs);
    }
  }

  /**
   * Subscribe client to room messages.
   *
   * @param {String} client id
   * @param {String} room
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.add = function(id, room, fn){
    debug('adding %s to %s ', id, room);
    Adapter.prototype.add.call(this, id, room);
    runCommand('subscribe', this.getChannelName(room), fn);
  };

  /**
   * Unsubscribe client from room messages.
   *
   * @param {String} session id
   * @param {String} room id
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.del = function(id, room, fn){
    debug('removing %s from %s', id, room);
    Adapter.prototype.del.call(this, id, room);
    if (!this.rooms[room]) {
      runCommand('unsubscribe', this.getChannelName(room), fn);
    } else if (fn) {
      process.nextTick(fn.bind(null, null));
    }
  };

  /**
   * Unsubscribe client completely.
   *
   * @param {String} client id
   * @param {Function} callback (optional)
   * @api public
   */

  Redis.prototype.delAll = function(id, fn){
    var rooms = this.sids[id];

    debug('removing %s from all rooms', id );

    Adapter.prototype.delAll.call(this, id);

    if (!rooms) {
      return process.nextTick(fn.bind(null, null));
    } else {
        debug('removing %s from rooms %s', id , Object.keys(rooms));
      var self = this;
      runCommand('unsubscribe', Object.keys(rooms).reduce(function(memo, room) {
        if (!self.rooms[room]) {
          memo.push(self.getChannelName(room));
        }
        return memo;
      }, []), fn);
    }
  };

  Redis.uid = uid;
  Redis.pubClient = pub;
  Redis.subClient = sub;
  Redis.prefix = prefix;

  return Redis;

}
