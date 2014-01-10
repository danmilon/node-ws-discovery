var dgram = require('dgram')
  , uuid = require('node-uuid')
  , et = require('elementtree')
  , EventEmitter = require('events').EventEmitter
  , util = require('util')
  , bunyan = require('bunyan')

var PROBE_ACTION = {
  '1.1': 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Probe',
  '2006': 'http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe'
}

var RESOLVE_ACTION = {
  '1.1': 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Resolve',
  '2006': 'http://schemas.xmlsoap.org/ws/2005/04/discovery/Resolve'
}

var DISCOVERY_BODY = {
  '1.1':            ['<?xml version="1.0" encoding="UTF-8"?>' +
                    '<s12:Envelope xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01">' +
                    '<s12:Header>' +
                    '    <wsa:Action>http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Probe</wsa:Action>' +
                    '    <wsa:MessageID>',
                    '</wsa:MessageID>' +
                    '    <wsa:To>urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01</wsa:To>' +
                    '</s12:Header>' +
                    '<s12:Body>' +
                    '    <wsd:Probe/>' +
                    '</s12:Body>' +
                    '</s12:Envelope>'],
  '2006':           ['<?xml version="1.0" encoding="UTF-8"?>' +
                     '<s12:Envelope xmlns:wsdp="http://schemas.xmlsoap.org/ws/2006/02/devprof" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsd="http://schemas.xmlsoap.org/ws/2005/04/discovery">' +
                     '<s12:Header>' +
                     '    <wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Probe</wsa:Action>' +
                     '   <wsa:MessageID>',
                     '</wsa:MessageID>' +
                     '    <wsa:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</wsa:To>' +
                     '</s12:Header>' +
                     '<s12:Body>' +
                     '    <wsd:Probe/>' +
                     '</s12:Body>' +
                     '</s12:Envelope>']
}

var RESOLVE_BODY =
  ['<?xml version="1.0" encoding="UTF-8"?>' +
  '<s12:Envelope xmlns:wsdp="http://schemas.xmlsoap.org/ws/2006/02/devprof" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsd="http://schemas.xmlsoap.org/ws/2005/04/discovery">' +
  '<s12:Header>' +
      '<wsa:Action>http://schemas.xmlsoap.org/ws/2005/04/discovery/Resolve</wsa:Action>' +
      '<wsa:MessageID>',
      '</wsa:MessageID>' +
      '<wsa:To>urn:schemas-xmlsoap-org:ws:2005:04:discovery</wsa:To>' +
  '</s12:Header>' +
  '<s12:Body>' +
      '<wsd:Resolve>' +
          '<wsa:EndpointReference>' +
              '<wsa:Address>',
              '</wsa:Address>' +
          '</wsa:EndpointReference>' +
      '</wsd:Resolve>' +
  '</s12:Body>' +
  '</s12:Envelope>']

var PROBE_MATCH_BODY =
  ['<?xml version="1.0" encoding="UTF-8"?>' +
  '<s12:Envelope xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01">' +
  '<s12:Header>' +
      '<wsa:Action>http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ProbeMatches</wsa:Action>' +
      '<wsa:MessageID>',
      '</wsa:MessageID>' +
      '<wsa:RelatesTo>',
      '</wsa:RelatesTo>' +
      '<wsa:To>http://www.w3.org/2005/08/addressing/anonymous</wsa:To>' +
      '<wsd:AppSequence InstanceId="',
      '" MessageNumber="',
      '"/>' +
  '</s12:Header>' +
  '<s12:Body>' +
      '<wsd:ProbeMatches>' +
          '<wsd:ProbeMatch>' +
              '<wsa:EndpointReference>' +
                  '<wsa:Address>',
                  '</wsa:Address>' +
              '</wsa:EndpointReference>' +
              '<wsd:Types>',
              '</wsd:Types>' +
              '<wsd:XAddrs>',
              '</wsd:XAddrs>' +
              '<wsd:MetadataVersion>',
              '</wsd:MetadataVersion>' +
          '</wsd:ProbeMatch>' +
      '</wsd:ProbeMatches>' +
  '</s12:Body>' +
  '</s12:Envelope>']

var RESOLVE_MATCH_BODY =
  ['<?xml version="1.0" encoding="UTF-8"?>' +
  '<s12:Envelope xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01">' +
  '<s12:Header>' +
      '<wsa:Action>http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ResolveMatches</wsa:Action>' +
      '<wsa:MessageID>',
      '</wsa:MessageID>' +
      '<wsa:RelatesTo>',
      '</wsa:RelatesTo>' +
      '<wsa:To>http://www.w3.org/2005/08/addressing/anonymous</wsa:To>' +
      '<wsd:AppSequence InstanceId="',
      '" MessageNumber="',
      '"/>' +
  '</s12:Header>' +
  '<s12:Body>' +
      '<wsd:ResolveMatches>' +
          '<wsd:ResolveMatch>' +
              '<wsa:EndpointReference>' +
                  '<wsa:Address>',
                  '</wsa:Address>' +
              '</wsa:EndpointReference>' +
              '<wsd:Types>',
              '</wsd:Types>' +
              '<wsd:XAddrs>',
              '</wsd:XAddrs>' +
              '<wsd:MetadataVersion>',
              '</wsd:MetadataVersion>' +
          '</wsd:ResolveMatch>' +
      '</wsd:ResolveMatches>' +
  '</s12:Body>' +
  '</s12:Envelope>']

var HELLO_BODY =
  ['<?xml version="1.0" encoding="UTF-8"?>' +
  '<s12:Envelope xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01">' +
  '<s12:Header>' +
      '<wsa:Action>http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Hello</wsa:Action>' +
      '<wsa:MessageID>',
      '</wsa:MessageID>' +
      '<wsa:To>urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01</wsa:To>' +
      '<wsd:AppSequence InstanceId="',
      '" MessageNumber="',
      '"/>' +
  '</s12:Header>' +
  '<s12:Body>' +
      '<wsd:Hello>' +
          '<wsa:EndpointReference>' +
              '<wsa:Address>',
              '</wsa:Address>' +
          '</wsa:EndpointReference>' +
          '<wsd:Types>',
          '</wsd:Types>' +
          '<wsd:XAddrs>',
          '</wsd:XAddrs>' +
          '<wsd:MetadataVersion>',
          '</wsd:MetadataVersion>' +
      '</wsd:Hello>' +
  '</s12:Body>' +
  '</s12:Envelope>']

var BYE_BODY =
  ['<?xml version="1.0" encoding="UTF-8"?>' +
  '<s12:Envelope xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01">' +
  '<s12:Header>' +
      '<wsa:Action>http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Bye</wsa:Action>' +
      '<wsa:MessageID>',
      '</wsa:MessageID>' +
      '<wsa:To>urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01</wsa:To>' +
      '<wsd:AppSequence InstanceId="',
      '" MessageNumber="',
      '"/>' +
  '</s12:Header>' +
  '<s12:Body>' +
      '<wsd:Bye>' +
          '<wsa:EndpointReference>' +
              '<wsa:Address>',
              '</wsa:Address>' +
          '</wsa:EndpointReference>' +
          '<wsd:Types>',
          '</wsd:Types>' +
          '<wsd:XAddrs>',
          '</wsd:XAddrs>' +
          '<wsd:MetadataVersion>',
          '</wsd:MetadataVersion>' +
      '</wsd:Bye>' +
  '</s12:Body>' +
  '</s12:Envelope>']

function emptyCb() {}



function makeByeBody(msgId, instanceId, msgNum, device) {
  var body = BYE_BODY

  return new Buffer(
    body[0] + msgId + body[1] +
    instanceId + body[2] +
    msgNum + body[3] +
    device.address +
    body[4] +
    device.types +
    body[5] +
    device.xaddrs +
    body[6] +
    device.metadataVersion +
    body[7])
}

function makeHelloBody(msgId, instanceId, msgNum, device) {
  var body = HELLO_BODY

  return new Buffer(
    body[0] + msgId + body[1] +
    instanceId + body[2] +
    msgNum + body[3] +
    device.address +
    body[4] +
    device.types +
    body[5] +
    device.xaddrs +
    body[6] +
    device.metadataVersion +
    body[7])
}

function makeResolveMatchBody(msgId, relatesTo, instanceId, msgNum, device) {
  var body = RESOLVE_MATCH_BODY

  return new Buffer(
    body[0] + msgId + body[1] +
    relatesTo + body[2] +
    instanceId + body[3] +
    msgNum + body[4] +
    device.address +
    body[5] +
    device.types +
    body[6] +
    device.xaddrs +
    body[7] +
    device.metadataVersion +
    body[8])
}

function makeProbeMatchBody(msgId, relatesTo, instanceId, msgNum, device) {
  var body = PROBE_MATCH_BODY

  return new Buffer(
    body[0] + msgId + body[1] +
    relatesTo + body[2] +
    instanceId + body[3] +
    msgNum + body[4] +
    device.address +
    body[5] +
    device.types +
    body[6] +
    device.xaddrs +
    body[7] +
    device.metadataVersion +
    body[8])
}

function makeDiscoveryBody(ver, msgId) {
  var body = DISCOVERY_BODY[ver]

  return new Buffer(body[0] + msgId + body[1])
}

function makeResolveBody(msgId, address) {
  var body = RESOLVE_BODY

  return new Buffer(body[0] + msgId + body[1] + address + body[2])
}

function genMessageId() {
  return 'urn:uuid:' + uuid.v4()
}

function setTimeoutWithRandomDelay(fn, max) {
  setTimeout(fn, Math.floor(Math.random() * max))
}

function WSDiscovery(opts) {
  if (!(this instanceof WSDiscovery)) {
    return new WSDiscovery(opts)
  }
  
  this.opts = opts = opts || {}
  
  this.device = opts.device

  opts.version = opts.version || '1.1'
  this.maxDelay = opts.maxDelay || 500

  this.resetInstanceId()
  this.msgNum = 1

  opts.log = opts.log || false

  if (opts.log) {
    this.log = bunyan.createLogger({ name: 'ws-discovery' })
  }

  var isValidDPWSVersion = ['1.1', '2006', 'all'].indexOf(opts.version) !== -1
  if (!isValidDPWSVersion) {
    throw new Error('dpwsVersion must be 1.1, 2006 or all')
  }

  if (!this.device && opts.hello) {
    throw new Error('asked to transmit hello messages but no device provided')
  }

  if (this.device && typeof opts.hello === 'undefined') {
    opts.hello = true
  }

  this.socket = dgram.createSocket('udp4')
  this.socket.on('error', function (err) {
    throw err
  })
}

util.inherits(WSDiscovery, EventEmitter)

WSDiscovery.prototype.bind = function (cb) {
  var self = this

  this.socket.bind(3702, function () {
    if (self.log) {
      self.log.info('udp socket listening', self.socket.address())
    }
    
    self.socket.addMembership('239.255.255.250')
    
    if (self.log) {
      self.log.info('joined multicast group 239.255.255.250')
    }
  })

  if (this.device) {
    this.socket.on('message', function (msg, rinfo) {
      var tree = et.parse(msg.toString())

      var action = tree.findtext('*/wsa:Action')
      if (action === PROBE_ACTION['1.1']) {
        self.replyToProbe(tree, rinfo)
      }
      else if (action === RESOLVE_ACTION['1.1']) {
        self.replyToResolve(tree, rinfo)
      }
    })
  }

  this.socket.on('listening', function () {
    cb()
  })

  if (this.opts.hello) {
    this.hello()
  }
}

WSDiscovery.prototype.close = function () {
  this.socket.once('close', this.emit.bind(this, 'close'))
  this.socket.close()
}

WSDiscovery.prototype.hello = function (cb) {
  var msgId = genMessageId()

  var body = makeHelloBody(
    msgId,
    this.instanceId,
    this.msgNum,
    this.device)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, 3702, '239.255.255.250', cb), this.maxDelay)
  this.msgNum += 1
}

WSDiscovery.prototype.bye = function (cb) {
  var msgId = genMessageId()

  var body = makeByeBody(
    msgId,
    this.instanceId,
    this.msgNum,
    this.device)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, 3702, '239.255.255.250', cb), this.maxDelay)
  this.msgNum += 1
}

WSDiscovery.prototype.resetInstanceId = function () {
  this.instanceId = Date.now()
}

WSDiscovery.prototype.replyToResolve = function (tree, rinfo) {
  if (tree instanceof Buffer) {
    tree = tree.toString()
  }

  if (typeof tree === 'string') {
    tree = et.parse(tree)
  }

  var msgId = genMessageId()
    , resolveMsgId = tree.findtext('*/wsa:MessageID')

  var body = makeResolveMatchBody(
    msgId,
    resolveMsgId,
    this.instanceId,
    this.msgNum,
    this.device)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, rinfo.port, rinfo.address), this.maxDelay)
  this.msgNum += 1
}

WSDiscovery.prototype.replyToProbe = function (tree, rinfo) {
  if (tree instanceof Buffer) {
    tree = et.parse(tree.toString())
  }
  else if (typeof tree === 'string') {
    tree = et.parse(tree)
  }

  var msgId = genMessageId()
    , probeMsgId = tree.findtext('*/wsa:MessageID')

  var body = makeProbeMatchBody(
    msgId,
    probeMsgId,
    this.instanceId,
    this.msgNum,
    this.device)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, rinfo.port, rinfo.address), this.maxDelay)
  this.msgNum += 1
}

WSDiscovery.prototype.probe = function (opts, cb) {
  var self = this

  if (typeof opts === 'function') {
    cb = opts
  }

  cb = cb || emptyCb
  opts = opts || {}


  opts.timeout = opts.timeout || 5000
  opts.resolve = opts.resolve || false
  
  var messageId = genMessageId()
    , body = makeDiscoveryBody(this.opts.version, messageId)

  this.socket.on('message', listener)

  setTimeout(function () {
    self.socket.removeListener('message', listener)
    cb()
  }, opts.timeout)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, 3702, '239.255.255.250'), this.maxDelay)

  function listener(msg) {
    var tree = et.parse(msg.toString())

    var relatesTo = tree.findtext('*/wsa:RelatesTo')
    if (relatesTo === messageId) {
      var matches = tree.findall('*/*/wsd:ProbeMatch')
      
      matches.forEach(function (match) {
        var device = {
          address: match.findtext('*/wsa:Address'),
          types: match.findtext('wsd:Types'),
          metadataVersion: match.findtext('wsd:MetadataVersion'),
          xaddrs: match.findtext('wsd:XAddrs')
        }

        if (opts.resolve) {
          self.resolve(device.address, function (err, resolved) {
            if (err) {
              throw err
            }

            device.xaddrs = resolved.xaddrs

            self.emit('device', device)
          })
        }
        else {
          self.emit('device', device)
        }
      })
    }
  }
}

WSDiscovery.prototype.resolve = function (opts, cb) {
  var self = this

  if (typeof opts === 'string') {
    opts = { address: opts }
  }

  opts.timeout = opts.timeout || 5000

  var msgId = genMessageId()
  var body = makeResolveBody(msgId, opts.address)

  self.socket.on('message', listener)
  
  setTimeout(function () {
    self.socket.removeListener('message', listener)
  }, opts.timeout)

  setTimeoutWithRandomDelay(this.socket.send.bind(this.socket, body, 0, body.length, 3702, '239.255.255.250'), this.maxDelay)

  function listener(msg) {
    var tree = et.parse(msg.toString())

    var relatesTo = tree.findtext('*/wsa:RelatesTo')
    if (relatesTo === msgId) {
      self.socket.removeListener('message', listener)

      var match = tree.find('*/*/wsd:ResolveMatch')

      if (match.findtext('*/wsa:Address') !== opts.address) {
        return cb(new Error('resolved address differs from request!'))
      }

      cb(null, {
        xaddrs: match.findtext('wsd:XAddrs'),
        metadataVersion: match.findtext('wsd:MetadataVersion')
      })
    }
  }
}

module.exports = WSDiscovery
