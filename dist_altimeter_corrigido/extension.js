"use strict"; var vr = Object.create; var At = Object.defineProperty; var br = Object.getOwnPropertyDescriptor; var gr = Object.getOwnPropertyNames; var yr = Object.getPrototypeOf, wr = Object.prototype.hasOwnProperty; var g = (t, e) => () => (e || t((e = { exports: {} }).exports, e), e.exports), On = (t, e) => { for (var a in e) At(t, a, { get: e[a], enumerable: !0 }) }, jn = (t, e, a, n) => { if (e && typeof e == "object" || typeof e == "function") for (let i of gr(e)) !wr.call(t, i) && i !== a && At(t, i, { get: () => e[i], enumerable: !(n = br(e, i)) || n.enumerable }); return t }; var L = (t, e, a) => (a = t != null ? vr(yr(t)) : {}, jn(e || !t || !t.__esModule ? At(a, "default", { value: t, enumerable: !0 }) : a, t)), Sr = t => jn(At({}, "__esModule", { value: !0 }), t); var Vn = g((Gu, Wn) => { var Gn = require("stream").Stream, cc = require("util"); Wn.exports = oe; function oe() { this.source = null, this.dataSize = 0, this.maxDataSize = 1024 * 1024, this.pauseStream = !0, this._maxDataSizeExceeded = !1, this._released = !1, this._bufferedEvents = [] } cc.inherits(oe, Gn); oe.create = function (t, e) { var a = new this; e = e || {}; for (var n in e) a[n] = e[n]; a.source = t; var i = t.emit; return t.emit = function () { return a._handleEmit(arguments), i.apply(t, arguments) }, t.on("error", function () { }), a.pauseStream && t.pause(), a }; Object.defineProperty(oe.prototype, "readable", { configurable: !0, enumerable: !0, get: function () { return this.source.readable } }); oe.prototype.setEncoding = function () { return this.source.setEncoding.apply(this.source, arguments) }; oe.prototype.resume = function () { this._released || this.release(), this.source.resume() }; oe.prototype.pause = function () { this.source.pause() }; oe.prototype.release = function () { this._released = !0, this._bufferedEvents.forEach(function (t) { this.emit.apply(this, t) }.bind(this)), this._bufferedEvents = [] }; oe.prototype.pipe = function () { var t = Gn.prototype.pipe.apply(this, arguments); return this.resume(), t }; oe.prototype._handleEmit = function (t) { if (this._released) { this.emit.apply(this, t); return } t[0] === "data" && (this.dataSize += t[1].length, this._checkIfMaxDataSizeExceeded()), this._bufferedEvents.push(t) }; oe.prototype._checkIfMaxDataSizeExceeded = function () { if (!this._maxDataSizeExceeded && !(this.dataSize <= this.maxDataSize)) { this._maxDataSizeExceeded = !0; var t = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded."; this.emit("error", new Error(t)) } } }); var Xn = g((Wu, Yn) => { var pc = require("util"), Kn = require("stream").Stream, Jn = Vn(); Yn.exports = I; function I() { this.writable = !1, this.readable = !0, this.dataSize = 0, this.maxDataSize = 2 * 1024 * 1024, this.pauseStreams = !0, this._released = !1, this._streams = [], this._currentStream = null, this._insideLoop = !1, this._pendingNext = !1 } pc.inherits(I, Kn); I.create = function (t) { var e = new this; t = t || {}; for (var a in t) e[a] = t[a]; return e }; I.isStreamLike = function (t) { return typeof t != "function" && typeof t != "string" && typeof t != "boolean" && typeof t != "number" && !Buffer.isBuffer(t) }; I.prototype.append = function (t) { var e = I.isStreamLike(t); if (e) { if (!(t instanceof Jn)) { var a = Jn.create(t, { maxDataSize: 1 / 0, pauseStream: this.pauseStreams }); t.on("data", this._checkDataSize.bind(this)), t = a } this._handleErrors(t), this.pauseStreams && t.pause() } return this._streams.push(t), this }; I.prototype.pipe = function (t, e) { return Kn.prototype.pipe.call(this, t, e), this.resume(), t }; I.prototype._getNext = function () { if (this._currentStream = null, this._insideLoop) { this._pendingNext = !0; return } this._insideLoop = !0; try { do this._pendingNext = !1, this._realGetNext(); while (this._pendingNext) } finally { this._insideLoop = !1 } }; I.prototype._realGetNext = function () { var t = this._streams.shift(); if (typeof t > "u") { this.end(); return } if (typeof t != "function") { this._pipeNext(t); return } var e = t; e(function (a) { var n = I.isStreamLike(a); n && (a.on("data", this._checkDataSize.bind(this)), this._handleErrors(a)), this._pipeNext(a) }.bind(this)) }; I.prototype._pipeNext = function (t) { this._currentStream = t; var e = I.isStreamLike(t); if (e) { t.on("end", this._getNext.bind(this)), t.pipe(this, { end: !1 }); return } var a = t; this.write(a), this._getNext() }; I.prototype._handleErrors = function (t) { var e = this; t.on("error", function (a) { e._emitError(a) }) }; I.prototype.write = function (t) { this.emit("data", t) }; I.prototype.pause = function () { this.pauseStreams && (this.pauseStreams && this._currentStream && typeof this._currentStream.pause == "function" && this._currentStream.pause(), this.emit("pause")) }; I.prototype.resume = function () { this._released || (this._released = !0, this.writable = !0, this._getNext()), this.pauseStreams && this._currentStream && typeof this._currentStream.resume == "function" && this._currentStream.resume(), this.emit("resume") }; I.prototype.end = function () { this._reset(), this.emit("end") }; I.prototype.destroy = function () { this._reset(), this.emit("close") }; I.prototype._reset = function () { this.writable = !1, this._streams = [], this._currentStream = null }; I.prototype._checkDataSize = function () { if (this._updateDataSize(), !(this.dataSize <= this.maxDataSize)) { var t = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded."; this._emitError(new Error(t)) } }; I.prototype._updateDataSize = function () { this.dataSize = 0; var t = this; this._streams.forEach(function (e) { e.dataSize && (t.dataSize += e.dataSize) }), this._currentStream && this._currentStream.dataSize && (this.dataSize += this._currentStream.dataSize) }; I.prototype._emitError = function (t) { this._reset(), this.emit("error", t) } }); var Qn = g((Vu, lc) => { lc.exports = { "application/1d-interleaved-parityfec": { source: "iana" }, "application/3gpdash-qoe-report+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/3gpp-ims+xml": { source: "iana", compressible: !0 }, "application/3gpphal+json": { source: "iana", compressible: !0 }, "application/3gpphalforms+json": { source: "iana", compressible: !0 }, "application/a2l": { source: "iana" }, "application/ace+cbor": { source: "iana" }, "application/activemessage": { source: "iana" }, "application/activity+json": { source: "iana", compressible: !0 }, "application/alto-costmap+json": { source: "iana", compressible: !0 }, "application/alto-costmapfilter+json": { source: "iana", compressible: !0 }, "application/alto-directory+json": { source: "iana", compressible: !0 }, "application/alto-endpointcost+json": { source: "iana", compressible: !0 }, "application/alto-endpointcostparams+json": { source: "iana", compressible: !0 }, "application/alto-endpointprop+json": { source: "iana", compressible: !0 }, "application/alto-endpointpropparams+json": { source: "iana", compressible: !0 }, "application/alto-error+json": { source: "iana", compressible: !0 }, "application/alto-networkmap+json": { source: "iana", compressible: !0 }, "application/alto-networkmapfilter+json": { source: "iana", compressible: !0 }, "application/alto-updatestreamcontrol+json": { source: "iana", compressible: !0 }, "application/alto-updatestreamparams+json": { source: "iana", compressible: !0 }, "application/aml": { source: "iana" }, "application/andrew-inset": { source: "iana", extensions: ["ez"] }, "application/applefile": { source: "iana" }, "application/applixware": { source: "apache", extensions: ["aw"] }, "application/at+jwt": { source: "iana" }, "application/atf": { source: "iana" }, "application/atfx": { source: "iana" }, "application/atom+xml": { source: "iana", compressible: !0, extensions: ["atom"] }, "application/atomcat+xml": { source: "iana", compressible: !0, extensions: ["atomcat"] }, "application/atomdeleted+xml": { source: "iana", compressible: !0, extensions: ["atomdeleted"] }, "application/atomicmail": { source: "iana" }, "application/atomsvc+xml": { source: "iana", compressible: !0, extensions: ["atomsvc"] }, "application/atsc-dwd+xml": { source: "iana", compressible: !0, extensions: ["dwd"] }, "application/atsc-dynamic-event-message": { source: "iana" }, "application/atsc-held+xml": { source: "iana", compressible: !0, extensions: ["held"] }, "application/atsc-rdt+json": { source: "iana", compressible: !0 }, "application/atsc-rsat+xml": { source: "iana", compressible: !0, extensions: ["rsat"] }, "application/atxml": { source: "iana" }, "application/auth-policy+xml": { source: "iana", compressible: !0 }, "application/bacnet-xdd+zip": { source: "iana", compressible: !1 }, "application/batch-smtp": { source: "iana" }, "application/bdoc": { compressible: !1, extensions: ["bdoc"] }, "application/beep+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/calendar+json": { source: "iana", compressible: !0 }, "application/calendar+xml": { source: "iana", compressible: !0, extensions: ["xcs"] }, "application/call-completion": { source: "iana" }, "application/cals-1840": { source: "iana" }, "application/captive+json": { source: "iana", compressible: !0 }, "application/cbor": { source: "iana" }, "application/cbor-seq": { source: "iana" }, "application/cccex": { source: "iana" }, "application/ccmp+xml": { source: "iana", compressible: !0 }, "application/ccxml+xml": { source: "iana", compressible: !0, extensions: ["ccxml"] }, "application/cdfx+xml": { source: "iana", compressible: !0, extensions: ["cdfx"] }, "application/cdmi-capability": { source: "iana", extensions: ["cdmia"] }, "application/cdmi-container": { source: "iana", extensions: ["cdmic"] }, "application/cdmi-domain": { source: "iana", extensions: ["cdmid"] }, "application/cdmi-object": { source: "iana", extensions: ["cdmio"] }, "application/cdmi-queue": { source: "iana", extensions: ["cdmiq"] }, "application/cdni": { source: "iana" }, "application/cea": { source: "iana" }, "application/cea-2018+xml": { source: "iana", compressible: !0 }, "application/cellml+xml": { source: "iana", compressible: !0 }, "application/cfw": { source: "iana" }, "application/city+json": { source: "iana", compressible: !0 }, "application/clr": { source: "iana" }, "application/clue+xml": { source: "iana", compressible: !0 }, "application/clue_info+xml": { source: "iana", compressible: !0 }, "application/cms": { source: "iana" }, "application/cnrp+xml": { source: "iana", compressible: !0 }, "application/coap-group+json": { source: "iana", compressible: !0 }, "application/coap-payload": { source: "iana" }, "application/commonground": { source: "iana" }, "application/conference-info+xml": { source: "iana", compressible: !0 }, "application/cose": { source: "iana" }, "application/cose-key": { source: "iana" }, "application/cose-key-set": { source: "iana" }, "application/cpl+xml": { source: "iana", compressible: !0, extensions: ["cpl"] }, "application/csrattrs": { source: "iana" }, "application/csta+xml": { source: "iana", compressible: !0 }, "application/cstadata+xml": { source: "iana", compressible: !0 }, "application/csvm+json": { source: "iana", compressible: !0 }, "application/cu-seeme": { source: "apache", extensions: ["cu"] }, "application/cwt": { source: "iana" }, "application/cybercash": { source: "iana" }, "application/dart": { compressible: !0 }, "application/dash+xml": { source: "iana", compressible: !0, extensions: ["mpd"] }, "application/dash-patch+xml": { source: "iana", compressible: !0, extensions: ["mpp"] }, "application/dashdelta": { source: "iana" }, "application/davmount+xml": { source: "iana", compressible: !0, extensions: ["davmount"] }, "application/dca-rft": { source: "iana" }, "application/dcd": { source: "iana" }, "application/dec-dx": { source: "iana" }, "application/dialog-info+xml": { source: "iana", compressible: !0 }, "application/dicom": { source: "iana" }, "application/dicom+json": { source: "iana", compressible: !0 }, "application/dicom+xml": { source: "iana", compressible: !0 }, "application/dii": { source: "iana" }, "application/dit": { source: "iana" }, "application/dns": { source: "iana" }, "application/dns+json": { source: "iana", compressible: !0 }, "application/dns-message": { source: "iana" }, "application/docbook+xml": { source: "apache", compressible: !0, extensions: ["dbk"] }, "application/dots+cbor": { source: "iana" }, "application/dskpp+xml": { source: "iana", compressible: !0 }, "application/dssc+der": { source: "iana", extensions: ["dssc"] }, "application/dssc+xml": { source: "iana", compressible: !0, extensions: ["xdssc"] }, "application/dvcs": { source: "iana" }, "application/ecmascript": { source: "iana", compressible: !0, extensions: ["es", "ecma"] }, "application/edi-consent": { source: "iana" }, "application/edi-x12": { source: "iana", compressible: !1 }, "application/edifact": { source: "iana", compressible: !1 }, "application/efi": { source: "iana" }, "application/elm+json": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/elm+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.cap+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/emergencycalldata.comment+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.control+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.deviceinfo+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.ecall.msd": { source: "iana" }, "application/emergencycalldata.providerinfo+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.serviceinfo+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.subscriberinfo+xml": { source: "iana", compressible: !0 }, "application/emergencycalldata.veds+xml": { source: "iana", compressible: !0 }, "application/emma+xml": { source: "iana", compressible: !0, extensions: ["emma"] }, "application/emotionml+xml": { source: "iana", compressible: !0, extensions: ["emotionml"] }, "application/encaprtp": { source: "iana" }, "application/epp+xml": { source: "iana", compressible: !0 }, "application/epub+zip": { source: "iana", compressible: !1, extensions: ["epub"] }, "application/eshop": { source: "iana" }, "application/exi": { source: "iana", extensions: ["exi"] }, "application/expect-ct-report+json": { source: "iana", compressible: !0 }, "application/express": { source: "iana", extensions: ["exp"] }, "application/fastinfoset": { source: "iana" }, "application/fastsoap": { source: "iana" }, "application/fdt+xml": { source: "iana", compressible: !0, extensions: ["fdt"] }, "application/fhir+json": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/fhir+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/fido.trusted-apps+json": { compressible: !0 }, "application/fits": { source: "iana" }, "application/flexfec": { source: "iana" }, "application/font-sfnt": { source: "iana" }, "application/font-tdpfr": { source: "iana", extensions: ["pfr"] }, "application/font-woff": { source: "iana", compressible: !1 }, "application/framework-attributes+xml": { source: "iana", compressible: !0 }, "application/geo+json": { source: "iana", compressible: !0, extensions: ["geojson"] }, "application/geo+json-seq": { source: "iana" }, "application/geopackage+sqlite3": { source: "iana" }, "application/geoxacml+xml": { source: "iana", compressible: !0 }, "application/gltf-buffer": { source: "iana" }, "application/gml+xml": { source: "iana", compressible: !0, extensions: ["gml"] }, "application/gpx+xml": { source: "apache", compressible: !0, extensions: ["gpx"] }, "application/gxf": { source: "apache", extensions: ["gxf"] }, "application/gzip": { source: "iana", compressible: !1, extensions: ["gz"] }, "application/h224": { source: "iana" }, "application/held+xml": { source: "iana", compressible: !0 }, "application/hjson": { extensions: ["hjson"] }, "application/http": { source: "iana" }, "application/hyperstudio": { source: "iana", extensions: ["stk"] }, "application/ibe-key-request+xml": { source: "iana", compressible: !0 }, "application/ibe-pkg-reply+xml": { source: "iana", compressible: !0 }, "application/ibe-pp-data": { source: "iana" }, "application/iges": { source: "iana" }, "application/im-iscomposing+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/index": { source: "iana" }, "application/index.cmd": { source: "iana" }, "application/index.obj": { source: "iana" }, "application/index.response": { source: "iana" }, "application/index.vnd": { source: "iana" }, "application/inkml+xml": { source: "iana", compressible: !0, extensions: ["ink", "inkml"] }, "application/iotp": { source: "iana" }, "application/ipfix": { source: "iana", extensions: ["ipfix"] }, "application/ipp": { source: "iana" }, "application/isup": { source: "iana" }, "application/its+xml": { source: "iana", compressible: !0, extensions: ["its"] }, "application/java-archive": { source: "apache", compressible: !1, extensions: ["jar", "war", "ear"] }, "application/java-serialized-object": { source: "apache", compressible: !1, extensions: ["ser"] }, "application/java-vm": { source: "apache", compressible: !1, extensions: ["class"] }, "application/javascript": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["js", "mjs"] }, "application/jf2feed+json": { source: "iana", compressible: !0 }, "application/jose": { source: "iana" }, "application/jose+json": { source: "iana", compressible: !0 }, "application/jrd+json": { source: "iana", compressible: !0 }, "application/jscalendar+json": { source: "iana", compressible: !0 }, "application/json": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["json", "map"] }, "application/json-patch+json": { source: "iana", compressible: !0 }, "application/json-seq": { source: "iana" }, "application/json5": { extensions: ["json5"] }, "application/jsonml+json": { source: "apache", compressible: !0, extensions: ["jsonml"] }, "application/jwk+json": { source: "iana", compressible: !0 }, "application/jwk-set+json": { source: "iana", compressible: !0 }, "application/jwt": { source: "iana" }, "application/kpml-request+xml": { source: "iana", compressible: !0 }, "application/kpml-response+xml": { source: "iana", compressible: !0 }, "application/ld+json": { source: "iana", compressible: !0, extensions: ["jsonld"] }, "application/lgr+xml": { source: "iana", compressible: !0, extensions: ["lgr"] }, "application/link-format": { source: "iana" }, "application/load-control+xml": { source: "iana", compressible: !0 }, "application/lost+xml": { source: "iana", compressible: !0, extensions: ["lostxml"] }, "application/lostsync+xml": { source: "iana", compressible: !0 }, "application/lpf+zip": { source: "iana", compressible: !1 }, "application/lxf": { source: "iana" }, "application/mac-binhex40": { source: "iana", extensions: ["hqx"] }, "application/mac-compactpro": { source: "apache", extensions: ["cpt"] }, "application/macwriteii": { source: "iana" }, "application/mads+xml": { source: "iana", compressible: !0, extensions: ["mads"] }, "application/manifest+json": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["webmanifest"] }, "application/marc": { source: "iana", extensions: ["mrc"] }, "application/marcxml+xml": { source: "iana", compressible: !0, extensions: ["mrcx"] }, "application/mathematica": { source: "iana", extensions: ["ma", "nb", "mb"] }, "application/mathml+xml": { source: "iana", compressible: !0, extensions: ["mathml"] }, "application/mathml-content+xml": { source: "iana", compressible: !0 }, "application/mathml-presentation+xml": { source: "iana", compressible: !0 }, "application/mbms-associated-procedure-description+xml": { source: "iana", compressible: !0 }, "application/mbms-deregister+xml": { source: "iana", compressible: !0 }, "application/mbms-envelope+xml": { source: "iana", compressible: !0 }, "application/mbms-msk+xml": { source: "iana", compressible: !0 }, "application/mbms-msk-response+xml": { source: "iana", compressible: !0 }, "application/mbms-protection-description+xml": { source: "iana", compressible: !0 }, "application/mbms-reception-report+xml": { source: "iana", compressible: !0 }, "application/mbms-register+xml": { source: "iana", compressible: !0 }, "application/mbms-register-response+xml": { source: "iana", compressible: !0 }, "application/mbms-schedule+xml": { source: "iana", compressible: !0 }, "application/mbms-user-service-description+xml": { source: "iana", compressible: !0 }, "application/mbox": { source: "iana", extensions: ["mbox"] }, "application/media-policy-dataset+xml": { source: "iana", compressible: !0, extensions: ["mpf"] }, "application/media_control+xml": { source: "iana", compressible: !0 }, "application/mediaservercontrol+xml": { source: "iana", compressible: !0, extensions: ["mscml"] }, "application/merge-patch+json": { source: "iana", compressible: !0 }, "application/metalink+xml": { source: "apache", compressible: !0, extensions: ["metalink"] }, "application/metalink4+xml": { source: "iana", compressible: !0, extensions: ["meta4"] }, "application/mets+xml": { source: "iana", compressible: !0, extensions: ["mets"] }, "application/mf4": { source: "iana" }, "application/mikey": { source: "iana" }, "application/mipc": { source: "iana" }, "application/missing-blocks+cbor-seq": { source: "iana" }, "application/mmt-aei+xml": { source: "iana", compressible: !0, extensions: ["maei"] }, "application/mmt-usd+xml": { source: "iana", compressible: !0, extensions: ["musd"] }, "application/mods+xml": { source: "iana", compressible: !0, extensions: ["mods"] }, "application/moss-keys": { source: "iana" }, "application/moss-signature": { source: "iana" }, "application/mosskey-data": { source: "iana" }, "application/mosskey-request": { source: "iana" }, "application/mp21": { source: "iana", extensions: ["m21", "mp21"] }, "application/mp4": { source: "iana", extensions: ["mp4s", "m4p"] }, "application/mpeg4-generic": { source: "iana" }, "application/mpeg4-iod": { source: "iana" }, "application/mpeg4-iod-xmt": { source: "iana" }, "application/mrb-consumer+xml": { source: "iana", compressible: !0 }, "application/mrb-publish+xml": { source: "iana", compressible: !0 }, "application/msc-ivr+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/msc-mixer+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/msword": { source: "iana", compressible: !1, extensions: ["doc", "dot"] }, "application/mud+json": { source: "iana", compressible: !0 }, "application/multipart-core": { source: "iana" }, "application/mxf": { source: "iana", extensions: ["mxf"] }, "application/n-quads": { source: "iana", extensions: ["nq"] }, "application/n-triples": { source: "iana", extensions: ["nt"] }, "application/nasdata": { source: "iana" }, "application/news-checkgroups": { source: "iana", charset: "US-ASCII" }, "application/news-groupinfo": { source: "iana", charset: "US-ASCII" }, "application/news-transmission": { source: "iana" }, "application/nlsml+xml": { source: "iana", compressible: !0 }, "application/node": { source: "iana", extensions: ["cjs"] }, "application/nss": { source: "iana" }, "application/oauth-authz-req+jwt": { source: "iana" }, "application/oblivious-dns-message": { source: "iana" }, "application/ocsp-request": { source: "iana" }, "application/ocsp-response": { source: "iana" }, "application/octet-stream": { source: "iana", compressible: !1, extensions: ["bin", "dms", "lrf", "mar", "so", "dist", "distz", "pkg", "bpk", "dump", "elc", "deploy", "exe", "dll", "deb", "dmg", "iso", "img", "msi", "msp", "msm", "buffer"] }, "application/oda": { source: "iana", extensions: ["oda"] }, "application/odm+xml": { source: "iana", compressible: !0 }, "application/odx": { source: "iana" }, "application/oebps-package+xml": { source: "iana", compressible: !0, extensions: ["opf"] }, "application/ogg": { source: "iana", compressible: !1, extensions: ["ogx"] }, "application/omdoc+xml": { source: "apache", compressible: !0, extensions: ["omdoc"] }, "application/onenote": { source: "apache", extensions: ["onetoc", "onetoc2", "onetmp", "onepkg"] }, "application/opc-nodeset+xml": { source: "iana", compressible: !0 }, "application/oscore": { source: "iana" }, "application/oxps": { source: "iana", extensions: ["oxps"] }, "application/p21": { source: "iana" }, "application/p21+zip": { source: "iana", compressible: !1 }, "application/p2p-overlay+xml": { source: "iana", compressible: !0, extensions: ["relo"] }, "application/parityfec": { source: "iana" }, "application/passport": { source: "iana" }, "application/patch-ops-error+xml": { source: "iana", compressible: !0, extensions: ["xer"] }, "application/pdf": { source: "iana", compressible: !1, extensions: ["pdf"] }, "application/pdx": { source: "iana" }, "application/pem-certificate-chain": { source: "iana" }, "application/pgp-encrypted": { source: "iana", compressible: !1, extensions: ["pgp"] }, "application/pgp-keys": { source: "iana", extensions: ["asc"] }, "application/pgp-signature": { source: "iana", extensions: ["asc", "sig"] }, "application/pics-rules": { source: "apache", extensions: ["prf"] }, "application/pidf+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/pidf-diff+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/pkcs10": { source: "iana", extensions: ["p10"] }, "application/pkcs12": { source: "iana" }, "application/pkcs7-mime": { source: "iana", extensions: ["p7m", "p7c"] }, "application/pkcs7-signature": { source: "iana", extensions: ["p7s"] }, "application/pkcs8": { source: "iana", extensions: ["p8"] }, "application/pkcs8-encrypted": { source: "iana" }, "application/pkix-attr-cert": { source: "iana", extensions: ["ac"] }, "application/pkix-cert": { source: "iana", extensions: ["cer"] }, "application/pkix-crl": { source: "iana", extensions: ["crl"] }, "application/pkix-pkipath": { source: "iana", extensions: ["pkipath"] }, "application/pkixcmp": { source: "iana", extensions: ["pki"] }, "application/pls+xml": { source: "iana", compressible: !0, extensions: ["pls"] }, "application/poc-settings+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/postscript": { source: "iana", compressible: !0, extensions: ["ai", "eps", "ps"] }, "application/ppsp-tracker+json": { source: "iana", compressible: !0 }, "application/problem+json": { source: "iana", compressible: !0 }, "application/problem+xml": { source: "iana", compressible: !0 }, "application/provenance+xml": { source: "iana", compressible: !0, extensions: ["provx"] }, "application/prs.alvestrand.titrax-sheet": { source: "iana" }, "application/prs.cww": { source: "iana", extensions: ["cww"] }, "application/prs.cyn": { source: "iana", charset: "7-BIT" }, "application/prs.hpub+zip": { source: "iana", compressible: !1 }, "application/prs.nprend": { source: "iana" }, "application/prs.plucker": { source: "iana" }, "application/prs.rdf-xml-crypt": { source: "iana" }, "application/prs.xsf+xml": { source: "iana", compressible: !0 }, "application/pskc+xml": { source: "iana", compressible: !0, extensions: ["pskcxml"] }, "application/pvd+json": { source: "iana", compressible: !0 }, "application/qsig": { source: "iana" }, "application/raml+yaml": { compressible: !0, extensions: ["raml"] }, "application/raptorfec": { source: "iana" }, "application/rdap+json": { source: "iana", compressible: !0 }, "application/rdf+xml": { source: "iana", compressible: !0, extensions: ["rdf", "owl"] }, "application/reginfo+xml": { source: "iana", compressible: !0, extensions: ["rif"] }, "application/relax-ng-compact-syntax": { source: "iana", extensions: ["rnc"] }, "application/remote-printing": { source: "iana" }, "application/reputon+json": { source: "iana", compressible: !0 }, "application/resource-lists+xml": { source: "iana", compressible: !0, extensions: ["rl"] }, "application/resource-lists-diff+xml": { source: "iana", compressible: !0, extensions: ["rld"] }, "application/rfc+xml": { source: "iana", compressible: !0 }, "application/riscos": { source: "iana" }, "application/rlmi+xml": { source: "iana", compressible: !0 }, "application/rls-services+xml": { source: "iana", compressible: !0, extensions: ["rs"] }, "application/route-apd+xml": { source: "iana", compressible: !0, extensions: ["rapd"] }, "application/route-s-tsid+xml": { source: "iana", compressible: !0, extensions: ["sls"] }, "application/route-usd+xml": { source: "iana", compressible: !0, extensions: ["rusd"] }, "application/rpki-ghostbusters": { source: "iana", extensions: ["gbr"] }, "application/rpki-manifest": { source: "iana", extensions: ["mft"] }, "application/rpki-publication": { source: "iana" }, "application/rpki-roa": { source: "iana", extensions: ["roa"] }, "application/rpki-updown": { source: "iana" }, "application/rsd+xml": { source: "apache", compressible: !0, extensions: ["rsd"] }, "application/rss+xml": { source: "apache", compressible: !0, extensions: ["rss"] }, "application/rtf": { source: "iana", compressible: !0, extensions: ["rtf"] }, "application/rtploopback": { source: "iana" }, "application/rtx": { source: "iana" }, "application/samlassertion+xml": { source: "iana", compressible: !0 }, "application/samlmetadata+xml": { source: "iana", compressible: !0 }, "application/sarif+json": { source: "iana", compressible: !0 }, "application/sarif-external-properties+json": { source: "iana", compressible: !0 }, "application/sbe": { source: "iana" }, "application/sbml+xml": { source: "iana", compressible: !0, extensions: ["sbml"] }, "application/scaip+xml": { source: "iana", compressible: !0 }, "application/scim+json": { source: "iana", compressible: !0 }, "application/scvp-cv-request": { source: "iana", extensions: ["scq"] }, "application/scvp-cv-response": { source: "iana", extensions: ["scs"] }, "application/scvp-vp-request": { source: "iana", extensions: ["spq"] }, "application/scvp-vp-response": { source: "iana", extensions: ["spp"] }, "application/sdp": { source: "iana", extensions: ["sdp"] }, "application/secevent+jwt": { source: "iana" }, "application/senml+cbor": { source: "iana" }, "application/senml+json": { source: "iana", compressible: !0 }, "application/senml+xml": { source: "iana", compressible: !0, extensions: ["senmlx"] }, "application/senml-etch+cbor": { source: "iana" }, "application/senml-etch+json": { source: "iana", compressible: !0 }, "application/senml-exi": { source: "iana" }, "application/sensml+cbor": { source: "iana" }, "application/sensml+json": { source: "iana", compressible: !0 }, "application/sensml+xml": { source: "iana", compressible: !0, extensions: ["sensmlx"] }, "application/sensml-exi": { source: "iana" }, "application/sep+xml": { source: "iana", compressible: !0 }, "application/sep-exi": { source: "iana" }, "application/session-info": { source: "iana" }, "application/set-payment": { source: "iana" }, "application/set-payment-initiation": { source: "iana", extensions: ["setpay"] }, "application/set-registration": { source: "iana" }, "application/set-registration-initiation": { source: "iana", extensions: ["setreg"] }, "application/sgml": { source: "iana" }, "application/sgml-open-catalog": { source: "iana" }, "application/shf+xml": { source: "iana", compressible: !0, extensions: ["shf"] }, "application/sieve": { source: "iana", extensions: ["siv", "sieve"] }, "application/simple-filter+xml": { source: "iana", compressible: !0 }, "application/simple-message-summary": { source: "iana" }, "application/simplesymbolcontainer": { source: "iana" }, "application/sipc": { source: "iana" }, "application/slate": { source: "iana" }, "application/smil": { source: "iana" }, "application/smil+xml": { source: "iana", compressible: !0, extensions: ["smi", "smil"] }, "application/smpte336m": { source: "iana" }, "application/soap+fastinfoset": { source: "iana" }, "application/soap+xml": { source: "iana", compressible: !0 }, "application/sparql-query": { source: "iana", extensions: ["rq"] }, "application/sparql-results+xml": { source: "iana", compressible: !0, extensions: ["srx"] }, "application/spdx+json": { source: "iana", compressible: !0 }, "application/spirits-event+xml": { source: "iana", compressible: !0 }, "application/sql": { source: "iana" }, "application/srgs": { source: "iana", extensions: ["gram"] }, "application/srgs+xml": { source: "iana", compressible: !0, extensions: ["grxml"] }, "application/sru+xml": { source: "iana", compressible: !0, extensions: ["sru"] }, "application/ssdl+xml": { source: "apache", compressible: !0, extensions: ["ssdl"] }, "application/ssml+xml": { source: "iana", compressible: !0, extensions: ["ssml"] }, "application/stix+json": { source: "iana", compressible: !0 }, "application/swid+xml": { source: "iana", compressible: !0, extensions: ["swidtag"] }, "application/tamp-apex-update": { source: "iana" }, "application/tamp-apex-update-confirm": { source: "iana" }, "application/tamp-community-update": { source: "iana" }, "application/tamp-community-update-confirm": { source: "iana" }, "application/tamp-error": { source: "iana" }, "application/tamp-sequence-adjust": { source: "iana" }, "application/tamp-sequence-adjust-confirm": { source: "iana" }, "application/tamp-status-query": { source: "iana" }, "application/tamp-status-response": { source: "iana" }, "application/tamp-update": { source: "iana" }, "application/tamp-update-confirm": { source: "iana" }, "application/tar": { compressible: !0 }, "application/taxii+json": { source: "iana", compressible: !0 }, "application/td+json": { source: "iana", compressible: !0 }, "application/tei+xml": { source: "iana", compressible: !0, extensions: ["tei", "teicorpus"] }, "application/tetra_isi": { source: "iana" }, "application/thraud+xml": { source: "iana", compressible: !0, extensions: ["tfi"] }, "application/timestamp-query": { source: "iana" }, "application/timestamp-reply": { source: "iana" }, "application/timestamped-data": { source: "iana", extensions: ["tsd"] }, "application/tlsrpt+gzip": { source: "iana" }, "application/tlsrpt+json": { source: "iana", compressible: !0 }, "application/tnauthlist": { source: "iana" }, "application/token-introspection+jwt": { source: "iana" }, "application/toml": { compressible: !0, extensions: ["toml"] }, "application/trickle-ice-sdpfrag": { source: "iana" }, "application/trig": { source: "iana", extensions: ["trig"] }, "application/ttml+xml": { source: "iana", compressible: !0, extensions: ["ttml"] }, "application/tve-trigger": { source: "iana" }, "application/tzif": { source: "iana" }, "application/tzif-leap": { source: "iana" }, "application/ubjson": { compressible: !1, extensions: ["ubj"] }, "application/ulpfec": { source: "iana" }, "application/urc-grpsheet+xml": { source: "iana", compressible: !0 }, "application/urc-ressheet+xml": { source: "iana", compressible: !0, extensions: ["rsheet"] }, "application/urc-targetdesc+xml": { source: "iana", compressible: !0, extensions: ["td"] }, "application/urc-uisocketdesc+xml": { source: "iana", compressible: !0 }, "application/vcard+json": { source: "iana", compressible: !0 }, "application/vcard+xml": { source: "iana", compressible: !0 }, "application/vemmi": { source: "iana" }, "application/vividence.scriptfile": { source: "apache" }, "application/vnd.1000minds.decision-model+xml": { source: "iana", compressible: !0, extensions: ["1km"] }, "application/vnd.3gpp-prose+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp-prose-pc3ch+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp-v2x-local-service-information": { source: "iana" }, "application/vnd.3gpp.5gnas": { source: "iana" }, "application/vnd.3gpp.access-transfer-events+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.bsf+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.gmop+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.gtpc": { source: "iana" }, "application/vnd.3gpp.interworking-data": { source: "iana" }, "application/vnd.3gpp.lpp": { source: "iana" }, "application/vnd.3gpp.mc-signalling-ear": { source: "iana" }, "application/vnd.3gpp.mcdata-affiliation-command+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcdata-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcdata-payload": { source: "iana" }, "application/vnd.3gpp.mcdata-service-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcdata-signalling": { source: "iana" }, "application/vnd.3gpp.mcdata-ue-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcdata-user-profile+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-affiliation-command+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-floor-request+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-location-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-mbms-usage-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-service-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-signed+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-ue-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-ue-init-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcptt-user-profile+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-affiliation-command+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-affiliation-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-location-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-mbms-usage-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-service-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-transmission-request+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-ue-config+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mcvideo-user-profile+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.mid-call+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.ngap": { source: "iana" }, "application/vnd.3gpp.pfcp": { source: "iana" }, "application/vnd.3gpp.pic-bw-large": { source: "iana", extensions: ["plb"] }, "application/vnd.3gpp.pic-bw-small": { source: "iana", extensions: ["psb"] }, "application/vnd.3gpp.pic-bw-var": { source: "iana", extensions: ["pvb"] }, "application/vnd.3gpp.s1ap": { source: "iana" }, "application/vnd.3gpp.sms": { source: "iana" }, "application/vnd.3gpp.sms+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.srvcc-ext+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.srvcc-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.state-and-event-info+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp.ussd+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp2.bcmcsinfo+xml": { source: "iana", compressible: !0 }, "application/vnd.3gpp2.sms": { source: "iana" }, "application/vnd.3gpp2.tcap": { source: "iana", extensions: ["tcap"] }, "application/vnd.3lightssoftware.imagescal": { source: "iana" }, "application/vnd.3m.post-it-notes": { source: "iana", extensions: ["pwn"] }, "application/vnd.accpac.simply.aso": { source: "iana", extensions: ["aso"] }, "application/vnd.accpac.simply.imp": { source: "iana", extensions: ["imp"] }, "application/vnd.acucobol": { source: "iana", extensions: ["acu"] }, "application/vnd.acucorp": { source: "iana", extensions: ["atc", "acutc"] }, "application/vnd.adobe.air-application-installer-package+zip": { source: "apache", compressible: !1, extensions: ["air"] }, "application/vnd.adobe.flash.movie": { source: "iana" }, "application/vnd.adobe.formscentral.fcdt": { source: "iana", extensions: ["fcdt"] }, "application/vnd.adobe.fxp": { source: "iana", extensions: ["fxp", "fxpl"] }, "application/vnd.adobe.partial-upload": { source: "iana" }, "application/vnd.adobe.xdp+xml": { source: "iana", compressible: !0, extensions: ["xdp"] }, "application/vnd.adobe.xfdf": { source: "iana", extensions: ["xfdf"] }, "application/vnd.aether.imp": { source: "iana" }, "application/vnd.afpc.afplinedata": { source: "iana" }, "application/vnd.afpc.afplinedata-pagedef": { source: "iana" }, "application/vnd.afpc.cmoca-cmresource": { source: "iana" }, "application/vnd.afpc.foca-charset": { source: "iana" }, "application/vnd.afpc.foca-codedfont": { source: "iana" }, "application/vnd.afpc.foca-codepage": { source: "iana" }, "application/vnd.afpc.modca": { source: "iana" }, "application/vnd.afpc.modca-cmtable": { source: "iana" }, "application/vnd.afpc.modca-formdef": { source: "iana" }, "application/vnd.afpc.modca-mediummap": { source: "iana" }, "application/vnd.afpc.modca-objectcontainer": { source: "iana" }, "application/vnd.afpc.modca-overlay": { source: "iana" }, "application/vnd.afpc.modca-pagesegment": { source: "iana" }, "application/vnd.age": { source: "iana", extensions: ["age"] }, "application/vnd.ah-barcode": { source: "iana" }, "application/vnd.ahead.space": { source: "iana", extensions: ["ahead"] }, "application/vnd.airzip.filesecure.azf": { source: "iana", extensions: ["azf"] }, "application/vnd.airzip.filesecure.azs": { source: "iana", extensions: ["azs"] }, "application/vnd.amadeus+json": { source: "iana", compressible: !0 }, "application/vnd.amazon.ebook": { source: "apache", extensions: ["azw"] }, "application/vnd.amazon.mobi8-ebook": { source: "iana" }, "application/vnd.americandynamics.acc": { source: "iana", extensions: ["acc"] }, "application/vnd.amiga.ami": { source: "iana", extensions: ["ami"] }, "application/vnd.amundsen.maze+xml": { source: "iana", compressible: !0 }, "application/vnd.android.ota": { source: "iana" }, "application/vnd.android.package-archive": { source: "apache", compressible: !1, extensions: ["apk"] }, "application/vnd.anki": { source: "iana" }, "application/vnd.anser-web-certificate-issue-initiation": { source: "iana", extensions: ["cii"] }, "application/vnd.anser-web-funds-transfer-initiation": { source: "apache", extensions: ["fti"] }, "application/vnd.antix.game-component": { source: "iana", extensions: ["atx"] }, "application/vnd.apache.arrow.file": { source: "iana" }, "application/vnd.apache.arrow.stream": { source: "iana" }, "application/vnd.apache.thrift.binary": { source: "iana" }, "application/vnd.apache.thrift.compact": { source: "iana" }, "application/vnd.apache.thrift.json": { source: "iana" }, "application/vnd.api+json": { source: "iana", compressible: !0 }, "application/vnd.aplextor.warrp+json": { source: "iana", compressible: !0 }, "application/vnd.apothekende.reservation+json": { source: "iana", compressible: !0 }, "application/vnd.apple.installer+xml": { source: "iana", compressible: !0, extensions: ["mpkg"] }, "application/vnd.apple.keynote": { source: "iana", extensions: ["key"] }, "application/vnd.apple.mpegurl": { source: "iana", extensions: ["m3u8"] }, "application/vnd.apple.numbers": { source: "iana", extensions: ["numbers"] }, "application/vnd.apple.pages": { source: "iana", extensions: ["pages"] }, "application/vnd.apple.pkpass": { compressible: !1, extensions: ["pkpass"] }, "application/vnd.arastra.swi": { source: "iana" }, "application/vnd.aristanetworks.swi": { source: "iana", extensions: ["swi"] }, "application/vnd.artisan+json": { source: "iana", compressible: !0 }, "application/vnd.artsquare": { source: "iana" }, "application/vnd.astraea-software.iota": { source: "iana", extensions: ["iota"] }, "application/vnd.audiograph": { source: "iana", extensions: ["aep"] }, "application/vnd.autopackage": { source: "iana" }, "application/vnd.avalon+json": { source: "iana", compressible: !0 }, "application/vnd.avistar+xml": { source: "iana", compressible: !0 }, "application/vnd.balsamiq.bmml+xml": { source: "iana", compressible: !0, extensions: ["bmml"] }, "application/vnd.balsamiq.bmpr": { source: "iana" }, "application/vnd.banana-accounting": { source: "iana" }, "application/vnd.bbf.usp.error": { source: "iana" }, "application/vnd.bbf.usp.msg": { source: "iana" }, "application/vnd.bbf.usp.msg+json": { source: "iana", compressible: !0 }, "application/vnd.bekitzur-stech+json": { source: "iana", compressible: !0 }, "application/vnd.bint.med-content": { source: "iana" }, "application/vnd.biopax.rdf+xml": { source: "iana", compressible: !0 }, "application/vnd.blink-idb-value-wrapper": { source: "iana" }, "application/vnd.blueice.multipass": { source: "iana", extensions: ["mpm"] }, "application/vnd.bluetooth.ep.oob": { source: "iana" }, "application/vnd.bluetooth.le.oob": { source: "iana" }, "application/vnd.bmi": { source: "iana", extensions: ["bmi"] }, "application/vnd.bpf": { source: "iana" }, "application/vnd.bpf3": { source: "iana" }, "application/vnd.businessobjects": { source: "iana", extensions: ["rep"] }, "application/vnd.byu.uapi+json": { source: "iana", compressible: !0 }, "application/vnd.cab-jscript": { source: "iana" }, "application/vnd.canon-cpdl": { source: "iana" }, "application/vnd.canon-lips": { source: "iana" }, "application/vnd.capasystems-pg+json": { source: "iana", compressible: !0 }, "application/vnd.cendio.thinlinc.clientconf": { source: "iana" }, "application/vnd.century-systems.tcp_stream": { source: "iana" }, "application/vnd.chemdraw+xml": { source: "iana", compressible: !0, extensions: ["cdxml"] }, "application/vnd.chess-pgn": { source: "iana" }, "application/vnd.chipnuts.karaoke-mmd": { source: "iana", extensions: ["mmd"] }, "application/vnd.ciedi": { source: "iana" }, "application/vnd.cinderella": { source: "iana", extensions: ["cdy"] }, "application/vnd.cirpack.isdn-ext": { source: "iana" }, "application/vnd.citationstyles.style+xml": { source: "iana", compressible: !0, extensions: ["csl"] }, "application/vnd.claymore": { source: "iana", extensions: ["cla"] }, "application/vnd.cloanto.rp9": { source: "iana", extensions: ["rp9"] }, "application/vnd.clonk.c4group": { source: "iana", extensions: ["c4g", "c4d", "c4f", "c4p", "c4u"] }, "application/vnd.cluetrust.cartomobile-config": { source: "iana", extensions: ["c11amc"] }, "application/vnd.cluetrust.cartomobile-config-pkg": { source: "iana", extensions: ["c11amz"] }, "application/vnd.coffeescript": { source: "iana" }, "application/vnd.collabio.xodocuments.document": { source: "iana" }, "application/vnd.collabio.xodocuments.document-template": { source: "iana" }, "application/vnd.collabio.xodocuments.presentation": { source: "iana" }, "application/vnd.collabio.xodocuments.presentation-template": { source: "iana" }, "application/vnd.collabio.xodocuments.spreadsheet": { source: "iana" }, "application/vnd.collabio.xodocuments.spreadsheet-template": { source: "iana" }, "application/vnd.collection+json": { source: "iana", compressible: !0 }, "application/vnd.collection.doc+json": { source: "iana", compressible: !0 }, "application/vnd.collection.next+json": { source: "iana", compressible: !0 }, "application/vnd.comicbook+zip": { source: "iana", compressible: !1 }, "application/vnd.comicbook-rar": { source: "iana" }, "application/vnd.commerce-battelle": { source: "iana" }, "application/vnd.commonspace": { source: "iana", extensions: ["csp"] }, "application/vnd.contact.cmsg": { source: "iana", extensions: ["cdbcmsg"] }, "application/vnd.coreos.ignition+json": { source: "iana", compressible: !0 }, "application/vnd.cosmocaller": { source: "iana", extensions: ["cmc"] }, "application/vnd.crick.clicker": { source: "iana", extensions: ["clkx"] }, "application/vnd.crick.clicker.keyboard": { source: "iana", extensions: ["clkk"] }, "application/vnd.crick.clicker.palette": { source: "iana", extensions: ["clkp"] }, "application/vnd.crick.clicker.template": { source: "iana", extensions: ["clkt"] }, "application/vnd.crick.clicker.wordbank": { source: "iana", extensions: ["clkw"] }, "application/vnd.criticaltools.wbs+xml": { source: "iana", compressible: !0, extensions: ["wbs"] }, "application/vnd.cryptii.pipe+json": { source: "iana", compressible: !0 }, "application/vnd.crypto-shade-file": { source: "iana" }, "application/vnd.cryptomator.encrypted": { source: "iana" }, "application/vnd.cryptomator.vault": { source: "iana" }, "application/vnd.ctc-posml": { source: "iana", extensions: ["pml"] }, "application/vnd.ctct.ws+xml": { source: "iana", compressible: !0 }, "application/vnd.cups-pdf": { source: "iana" }, "application/vnd.cups-postscript": { source: "iana" }, "application/vnd.cups-ppd": { source: "iana", extensions: ["ppd"] }, "application/vnd.cups-raster": { source: "iana" }, "application/vnd.cups-raw": { source: "iana" }, "application/vnd.curl": { source: "iana" }, "application/vnd.curl.car": { source: "apache", extensions: ["car"] }, "application/vnd.curl.pcurl": { source: "apache", extensions: ["pcurl"] }, "application/vnd.cyan.dean.root+xml": { source: "iana", compressible: !0 }, "application/vnd.cybank": { source: "iana" }, "application/vnd.cyclonedx+json": { source: "iana", compressible: !0 }, "application/vnd.cyclonedx+xml": { source: "iana", compressible: !0 }, "application/vnd.d2l.coursepackage1p0+zip": { source: "iana", compressible: !1 }, "application/vnd.d3m-dataset": { source: "iana" }, "application/vnd.d3m-problem": { source: "iana" }, "application/vnd.dart": { source: "iana", compressible: !0, extensions: ["dart"] }, "application/vnd.data-vision.rdz": { source: "iana", extensions: ["rdz"] }, "application/vnd.datapackage+json": { source: "iana", compressible: !0 }, "application/vnd.dataresource+json": { source: "iana", compressible: !0 }, "application/vnd.dbf": { source: "iana", extensions: ["dbf"] }, "application/vnd.debian.binary-package": { source: "iana" }, "application/vnd.dece.data": { source: "iana", extensions: ["uvf", "uvvf", "uvd", "uvvd"] }, "application/vnd.dece.ttml+xml": { source: "iana", compressible: !0, extensions: ["uvt", "uvvt"] }, "application/vnd.dece.unspecified": { source: "iana", extensions: ["uvx", "uvvx"] }, "application/vnd.dece.zip": { source: "iana", extensions: ["uvz", "uvvz"] }, "application/vnd.denovo.fcselayout-link": { source: "iana", extensions: ["fe_launch"] }, "application/vnd.desmume.movie": { source: "iana" }, "application/vnd.dir-bi.plate-dl-nosuffix": { source: "iana" }, "application/vnd.dm.delegation+xml": { source: "iana", compressible: !0 }, "application/vnd.dna": { source: "iana", extensions: ["dna"] }, "application/vnd.document+json": { source: "iana", compressible: !0 }, "application/vnd.dolby.mlp": { source: "apache", extensions: ["mlp"] }, "application/vnd.dolby.mobile.1": { source: "iana" }, "application/vnd.dolby.mobile.2": { source: "iana" }, "application/vnd.doremir.scorecloud-binary-document": { source: "iana" }, "application/vnd.dpgraph": { source: "iana", extensions: ["dpg"] }, "application/vnd.dreamfactory": { source: "iana", extensions: ["dfac"] }, "application/vnd.drive+json": { source: "iana", compressible: !0 }, "application/vnd.ds-keypoint": { source: "apache", extensions: ["kpxx"] }, "application/vnd.dtg.local": { source: "iana" }, "application/vnd.dtg.local.flash": { source: "iana" }, "application/vnd.dtg.local.html": { source: "iana" }, "application/vnd.dvb.ait": { source: "iana", extensions: ["ait"] }, "application/vnd.dvb.dvbisl+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.dvbj": { source: "iana" }, "application/vnd.dvb.esgcontainer": { source: "iana" }, "application/vnd.dvb.ipdcdftnotifaccess": { source: "iana" }, "application/vnd.dvb.ipdcesgaccess": { source: "iana" }, "application/vnd.dvb.ipdcesgaccess2": { source: "iana" }, "application/vnd.dvb.ipdcesgpdd": { source: "iana" }, "application/vnd.dvb.ipdcroaming": { source: "iana" }, "application/vnd.dvb.iptv.alfec-base": { source: "iana" }, "application/vnd.dvb.iptv.alfec-enhancement": { source: "iana" }, "application/vnd.dvb.notif-aggregate-root+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-container+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-generic+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-ia-msglist+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-ia-registration-request+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-ia-registration-response+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.notif-init+xml": { source: "iana", compressible: !0 }, "application/vnd.dvb.pfr": { source: "iana" }, "application/vnd.dvb.service": { source: "iana", extensions: ["svc"] }, "application/vnd.dxr": { source: "iana" }, "application/vnd.dynageo": { source: "iana", extensions: ["geo"] }, "application/vnd.dzr": { source: "iana" }, "application/vnd.easykaraoke.cdgdownload": { source: "iana" }, "application/vnd.ecdis-update": { source: "iana" }, "application/vnd.ecip.rlp": { source: "iana" }, "application/vnd.eclipse.ditto+json": { source: "iana", compressible: !0 }, "application/vnd.ecowin.chart": { source: "iana", extensions: ["mag"] }, "application/vnd.ecowin.filerequest": { source: "iana" }, "application/vnd.ecowin.fileupdate": { source: "iana" }, "application/vnd.ecowin.series": { source: "iana" }, "application/vnd.ecowin.seriesrequest": { source: "iana" }, "application/vnd.ecowin.seriesupdate": { source: "iana" }, "application/vnd.efi.img": { source: "iana" }, "application/vnd.efi.iso": { source: "iana" }, "application/vnd.emclient.accessrequest+xml": { source: "iana", compressible: !0 }, "application/vnd.enliven": { source: "iana", extensions: ["nml"] }, "application/vnd.enphase.envoy": { source: "iana" }, "application/vnd.eprints.data+xml": { source: "iana", compressible: !0 }, "application/vnd.epson.esf": { source: "iana", extensions: ["esf"] }, "application/vnd.epson.msf": { source: "iana", extensions: ["msf"] }, "application/vnd.epson.quickanime": { source: "iana", extensions: ["qam"] }, "application/vnd.epson.salt": { source: "iana", extensions: ["slt"] }, "application/vnd.epson.ssf": { source: "iana", extensions: ["ssf"] }, "application/vnd.ericsson.quickcall": { source: "iana" }, "application/vnd.espass-espass+zip": { source: "iana", compressible: !1 }, "application/vnd.eszigno3+xml": { source: "iana", compressible: !0, extensions: ["es3", "et3"] }, "application/vnd.etsi.aoc+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.asic-e+zip": { source: "iana", compressible: !1 }, "application/vnd.etsi.asic-s+zip": { source: "iana", compressible: !1 }, "application/vnd.etsi.cug+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvcommand+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvdiscovery+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvprofile+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvsad-bc+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvsad-cod+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvsad-npvr+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvservice+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvsync+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.iptvueprofile+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.mcid+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.mheg5": { source: "iana" }, "application/vnd.etsi.overload-control-policy-dataset+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.pstn+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.sci+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.simservs+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.timestamp-token": { source: "iana" }, "application/vnd.etsi.tsl+xml": { source: "iana", compressible: !0 }, "application/vnd.etsi.tsl.der": { source: "iana" }, "application/vnd.eu.kasparian.car+json": { source: "iana", compressible: !0 }, "application/vnd.eudora.data": { source: "iana" }, "application/vnd.evolv.ecig.profile": { source: "iana" }, "application/vnd.evolv.ecig.settings": { source: "iana" }, "application/vnd.evolv.ecig.theme": { source: "iana" }, "application/vnd.exstream-empower+zip": { source: "iana", compressible: !1 }, "application/vnd.exstream-package": { source: "iana" }, "application/vnd.ezpix-album": { source: "iana", extensions: ["ez2"] }, "application/vnd.ezpix-package": { source: "iana", extensions: ["ez3"] }, "application/vnd.f-secure.mobile": { source: "iana" }, "application/vnd.familysearch.gedcom+zip": { source: "iana", compressible: !1 }, "application/vnd.fastcopy-disk-image": { source: "iana" }, "application/vnd.fdf": { source: "iana", extensions: ["fdf"] }, "application/vnd.fdsn.mseed": { source: "iana", extensions: ["mseed"] }, "application/vnd.fdsn.seed": { source: "iana", extensions: ["seed", "dataless"] }, "application/vnd.ffsns": { source: "iana" }, "application/vnd.ficlab.flb+zip": { source: "iana", compressible: !1 }, "application/vnd.filmit.zfc": { source: "iana" }, "application/vnd.fints": { source: "iana" }, "application/vnd.firemonkeys.cloudcell": { source: "iana" }, "application/vnd.flographit": { source: "iana", extensions: ["gph"] }, "application/vnd.fluxtime.clip": { source: "iana", extensions: ["ftc"] }, "application/vnd.font-fontforge-sfd": { source: "iana" }, "application/vnd.framemaker": { source: "iana", extensions: ["fm", "frame", "maker", "book"] }, "application/vnd.frogans.fnc": { source: "iana", extensions: ["fnc"] }, "application/vnd.frogans.ltf": { source: "iana", extensions: ["ltf"] }, "application/vnd.fsc.weblaunch": { source: "iana", extensions: ["fsc"] }, "application/vnd.fujifilm.fb.docuworks": { source: "iana" }, "application/vnd.fujifilm.fb.docuworks.binder": { source: "iana" }, "application/vnd.fujifilm.fb.docuworks.container": { source: "iana" }, "application/vnd.fujifilm.fb.jfi+xml": { source: "iana", compressible: !0 }, "application/vnd.fujitsu.oasys": { source: "iana", extensions: ["oas"] }, "application/vnd.fujitsu.oasys2": { source: "iana", extensions: ["oa2"] }, "application/vnd.fujitsu.oasys3": { source: "iana", extensions: ["oa3"] }, "application/vnd.fujitsu.oasysgp": { source: "iana", extensions: ["fg5"] }, "application/vnd.fujitsu.oasysprs": { source: "iana", extensions: ["bh2"] }, "application/vnd.fujixerox.art-ex": { source: "iana" }, "application/vnd.fujixerox.art4": { source: "iana" }, "application/vnd.fujixerox.ddd": { source: "iana", extensions: ["ddd"] }, "application/vnd.fujixerox.docuworks": { source: "iana", extensions: ["xdw"] }, "application/vnd.fujixerox.docuworks.binder": { source: "iana", extensions: ["xbd"] }, "application/vnd.fujixerox.docuworks.container": { source: "iana" }, "application/vnd.fujixerox.hbpl": { source: "iana" }, "application/vnd.fut-misnet": { source: "iana" }, "application/vnd.futoin+cbor": { source: "iana" }, "application/vnd.futoin+json": { source: "iana", compressible: !0 }, "application/vnd.fuzzysheet": { source: "iana", extensions: ["fzs"] }, "application/vnd.genomatix.tuxedo": { source: "iana", extensions: ["txd"] }, "application/vnd.gentics.grd+json": { source: "iana", compressible: !0 }, "application/vnd.geo+json": { source: "iana", compressible: !0 }, "application/vnd.geocube+xml": { source: "iana", compressible: !0 }, "application/vnd.geogebra.file": { source: "iana", extensions: ["ggb"] }, "application/vnd.geogebra.slides": { source: "iana" }, "application/vnd.geogebra.tool": { source: "iana", extensions: ["ggt"] }, "application/vnd.geometry-explorer": { source: "iana", extensions: ["gex", "gre"] }, "application/vnd.geonext": { source: "iana", extensions: ["gxt"] }, "application/vnd.geoplan": { source: "iana", extensions: ["g2w"] }, "application/vnd.geospace": { source: "iana", extensions: ["g3w"] }, "application/vnd.gerber": { source: "iana" }, "application/vnd.globalplatform.card-content-mgt": { source: "iana" }, "application/vnd.globalplatform.card-content-mgt-response": { source: "iana" }, "application/vnd.gmx": { source: "iana", extensions: ["gmx"] }, "application/vnd.google-apps.document": { compressible: !1, extensions: ["gdoc"] }, "application/vnd.google-apps.presentation": { compressible: !1, extensions: ["gslides"] }, "application/vnd.google-apps.spreadsheet": { compressible: !1, extensions: ["gsheet"] }, "application/vnd.google-earth.kml+xml": { source: "iana", compressible: !0, extensions: ["kml"] }, "application/vnd.google-earth.kmz": { source: "iana", compressible: !1, extensions: ["kmz"] }, "application/vnd.gov.sk.e-form+xml": { source: "iana", compressible: !0 }, "application/vnd.gov.sk.e-form+zip": { source: "iana", compressible: !1 }, "application/vnd.gov.sk.xmldatacontainer+xml": { source: "iana", compressible: !0 }, "application/vnd.grafeq": { source: "iana", extensions: ["gqf", "gqs"] }, "application/vnd.gridmp": { source: "iana" }, "application/vnd.groove-account": { source: "iana", extensions: ["gac"] }, "application/vnd.groove-help": { source: "iana", extensions: ["ghf"] }, "application/vnd.groove-identity-message": { source: "iana", extensions: ["gim"] }, "application/vnd.groove-injector": { source: "iana", extensions: ["grv"] }, "application/vnd.groove-tool-message": { source: "iana", extensions: ["gtm"] }, "application/vnd.groove-tool-template": { source: "iana", extensions: ["tpl"] }, "application/vnd.groove-vcard": { source: "iana", extensions: ["vcg"] }, "application/vnd.hal+json": { source: "iana", compressible: !0 }, "application/vnd.hal+xml": { source: "iana", compressible: !0, extensions: ["hal"] }, "application/vnd.handheld-entertainment+xml": { source: "iana", compressible: !0, extensions: ["zmm"] }, "application/vnd.hbci": { source: "iana", extensions: ["hbci"] }, "application/vnd.hc+json": { source: "iana", compressible: !0 }, "application/vnd.hcl-bireports": { source: "iana" }, "application/vnd.hdt": { source: "iana" }, "application/vnd.heroku+json": { source: "iana", compressible: !0 }, "application/vnd.hhe.lesson-player": { source: "iana", extensions: ["les"] }, "application/vnd.hl7cda+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.hl7v2+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.hp-hpgl": { source: "iana", extensions: ["hpgl"] }, "application/vnd.hp-hpid": { source: "iana", extensions: ["hpid"] }, "application/vnd.hp-hps": { source: "iana", extensions: ["hps"] }, "application/vnd.hp-jlyt": { source: "iana", extensions: ["jlt"] }, "application/vnd.hp-pcl": { source: "iana", extensions: ["pcl"] }, "application/vnd.hp-pclxl": { source: "iana", extensions: ["pclxl"] }, "application/vnd.httphone": { source: "iana" }, "application/vnd.hydrostatix.sof-data": { source: "iana", extensions: ["sfd-hdstx"] }, "application/vnd.hyper+json": { source: "iana", compressible: !0 }, "application/vnd.hyper-item+json": { source: "iana", compressible: !0 }, "application/vnd.hyperdrive+json": { source: "iana", compressible: !0 }, "application/vnd.hzn-3d-crossword": { source: "iana" }, "application/vnd.ibm.afplinedata": { source: "iana" }, "application/vnd.ibm.electronic-media": { source: "iana" }, "application/vnd.ibm.minipay": { source: "iana", extensions: ["mpy"] }, "application/vnd.ibm.modcap": { source: "iana", extensions: ["afp", "listafp", "list3820"] }, "application/vnd.ibm.rights-management": { source: "iana", extensions: ["irm"] }, "application/vnd.ibm.secure-container": { source: "iana", extensions: ["sc"] }, "application/vnd.iccprofile": { source: "iana", extensions: ["icc", "icm"] }, "application/vnd.ieee.1905": { source: "iana" }, "application/vnd.igloader": { source: "iana", extensions: ["igl"] }, "application/vnd.imagemeter.folder+zip": { source: "iana", compressible: !1 }, "application/vnd.imagemeter.image+zip": { source: "iana", compressible: !1 }, "application/vnd.immervision-ivp": { source: "iana", extensions: ["ivp"] }, "application/vnd.immervision-ivu": { source: "iana", extensions: ["ivu"] }, "application/vnd.ims.imsccv1p1": { source: "iana" }, "application/vnd.ims.imsccv1p2": { source: "iana" }, "application/vnd.ims.imsccv1p3": { source: "iana" }, "application/vnd.ims.lis.v2.result+json": { source: "iana", compressible: !0 }, "application/vnd.ims.lti.v2.toolconsumerprofile+json": { source: "iana", compressible: !0 }, "application/vnd.ims.lti.v2.toolproxy+json": { source: "iana", compressible: !0 }, "application/vnd.ims.lti.v2.toolproxy.id+json": { source: "iana", compressible: !0 }, "application/vnd.ims.lti.v2.toolsettings+json": { source: "iana", compressible: !0 }, "application/vnd.ims.lti.v2.toolsettings.simple+json": { source: "iana", compressible: !0 }, "application/vnd.informedcontrol.rms+xml": { source: "iana", compressible: !0 }, "application/vnd.informix-visionary": { source: "iana" }, "application/vnd.infotech.project": { source: "iana" }, "application/vnd.infotech.project+xml": { source: "iana", compressible: !0 }, "application/vnd.innopath.wamp.notification": { source: "iana" }, "application/vnd.insors.igm": { source: "iana", extensions: ["igm"] }, "application/vnd.intercon.formnet": { source: "iana", extensions: ["xpw", "xpx"] }, "application/vnd.intergeo": { source: "iana", extensions: ["i2g"] }, "application/vnd.intertrust.digibox": { source: "iana" }, "application/vnd.intertrust.nncp": { source: "iana" }, "application/vnd.intu.qbo": { source: "iana", extensions: ["qbo"] }, "application/vnd.intu.qfx": { source: "iana", extensions: ["qfx"] }, "application/vnd.iptc.g2.catalogitem+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.conceptitem+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.knowledgeitem+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.newsitem+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.newsmessage+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.packageitem+xml": { source: "iana", compressible: !0 }, "application/vnd.iptc.g2.planningitem+xml": { source: "iana", compressible: !0 }, "application/vnd.ipunplugged.rcprofile": { source: "iana", extensions: ["rcprofile"] }, "application/vnd.irepository.package+xml": { source: "iana", compressible: !0, extensions: ["irp"] }, "application/vnd.is-xpr": { source: "iana", extensions: ["xpr"] }, "application/vnd.isac.fcs": { source: "iana", extensions: ["fcs"] }, "application/vnd.iso11783-10+zip": { source: "iana", compressible: !1 }, "application/vnd.jam": { source: "iana", extensions: ["jam"] }, "application/vnd.japannet-directory-service": { source: "iana" }, "application/vnd.japannet-jpnstore-wakeup": { source: "iana" }, "application/vnd.japannet-payment-wakeup": { source: "iana" }, "application/vnd.japannet-registration": { source: "iana" }, "application/vnd.japannet-registration-wakeup": { source: "iana" }, "application/vnd.japannet-setstore-wakeup": { source: "iana" }, "application/vnd.japannet-verification": { source: "iana" }, "application/vnd.japannet-verification-wakeup": { source: "iana" }, "application/vnd.jcp.javame.midlet-rms": { source: "iana", extensions: ["rms"] }, "application/vnd.jisp": { source: "iana", extensions: ["jisp"] }, "application/vnd.joost.joda-archive": { source: "iana", extensions: ["joda"] }, "application/vnd.jsk.isdn-ngn": { source: "iana" }, "application/vnd.kahootz": { source: "iana", extensions: ["ktz", "ktr"] }, "application/vnd.kde.karbon": { source: "iana", extensions: ["karbon"] }, "application/vnd.kde.kchart": { source: "iana", extensions: ["chrt"] }, "application/vnd.kde.kformula": { source: "iana", extensions: ["kfo"] }, "application/vnd.kde.kivio": { source: "iana", extensions: ["flw"] }, "application/vnd.kde.kontour": { source: "iana", extensions: ["kon"] }, "application/vnd.kde.kpresenter": { source: "iana", extensions: ["kpr", "kpt"] }, "application/vnd.kde.kspread": { source: "iana", extensions: ["ksp"] }, "application/vnd.kde.kword": { source: "iana", extensions: ["kwd", "kwt"] }, "application/vnd.kenameaapp": { source: "iana", extensions: ["htke"] }, "application/vnd.kidspiration": { source: "iana", extensions: ["kia"] }, "application/vnd.kinar": { source: "iana", extensions: ["kne", "knp"] }, "application/vnd.koan": { source: "iana", extensions: ["skp", "skd", "skt", "skm"] }, "application/vnd.kodak-descriptor": { source: "iana", extensions: ["sse"] }, "application/vnd.las": { source: "iana" }, "application/vnd.las.las+json": { source: "iana", compressible: !0 }, "application/vnd.las.las+xml": { source: "iana", compressible: !0, extensions: ["lasxml"] }, "application/vnd.laszip": { source: "iana" }, "application/vnd.leap+json": { source: "iana", compressible: !0 }, "application/vnd.liberty-request+xml": { source: "iana", compressible: !0 }, "application/vnd.llamagraphics.life-balance.desktop": { source: "iana", extensions: ["lbd"] }, "application/vnd.llamagraphics.life-balance.exchange+xml": { source: "iana", compressible: !0, extensions: ["lbe"] }, "application/vnd.logipipe.circuit+zip": { source: "iana", compressible: !1 }, "application/vnd.loom": { source: "iana" }, "application/vnd.lotus-1-2-3": { source: "iana", extensions: ["123"] }, "application/vnd.lotus-approach": { source: "iana", extensions: ["apr"] }, "application/vnd.lotus-freelance": { source: "iana", extensions: ["pre"] }, "application/vnd.lotus-notes": { source: "iana", extensions: ["nsf"] }, "application/vnd.lotus-organizer": { source: "iana", extensions: ["org"] }, "application/vnd.lotus-screencam": { source: "iana", extensions: ["scm"] }, "application/vnd.lotus-wordpro": { source: "iana", extensions: ["lwp"] }, "application/vnd.macports.portpkg": { source: "iana", extensions: ["portpkg"] }, "application/vnd.mapbox-vector-tile": { source: "iana", extensions: ["mvt"] }, "application/vnd.marlin.drm.actiontoken+xml": { source: "iana", compressible: !0 }, "application/vnd.marlin.drm.conftoken+xml": { source: "iana", compressible: !0 }, "application/vnd.marlin.drm.license+xml": { source: "iana", compressible: !0 }, "application/vnd.marlin.drm.mdcf": { source: "iana" }, "application/vnd.mason+json": { source: "iana", compressible: !0 }, "application/vnd.maxar.archive.3tz+zip": { source: "iana", compressible: !1 }, "application/vnd.maxmind.maxmind-db": { source: "iana" }, "application/vnd.mcd": { source: "iana", extensions: ["mcd"] }, "application/vnd.medcalcdata": { source: "iana", extensions: ["mc1"] }, "application/vnd.mediastation.cdkey": { source: "iana", extensions: ["cdkey"] }, "application/vnd.meridian-slingshot": { source: "iana" }, "application/vnd.mfer": { source: "iana", extensions: ["mwf"] }, "application/vnd.mfmp": { source: "iana", extensions: ["mfm"] }, "application/vnd.micro+json": { source: "iana", compressible: !0 }, "application/vnd.micrografx.flo": { source: "iana", extensions: ["flo"] }, "application/vnd.micrografx.igx": { source: "iana", extensions: ["igx"] }, "application/vnd.microsoft.portable-executable": { source: "iana" }, "application/vnd.microsoft.windows.thumbnail-cache": { source: "iana" }, "application/vnd.miele+json": { source: "iana", compressible: !0 }, "application/vnd.mif": { source: "iana", extensions: ["mif"] }, "application/vnd.minisoft-hp3000-save": { source: "iana" }, "application/vnd.mitsubishi.misty-guard.trustweb": { source: "iana" }, "application/vnd.mobius.daf": { source: "iana", extensions: ["daf"] }, "application/vnd.mobius.dis": { source: "iana", extensions: ["dis"] }, "application/vnd.mobius.mbk": { source: "iana", extensions: ["mbk"] }, "application/vnd.mobius.mqy": { source: "iana", extensions: ["mqy"] }, "application/vnd.mobius.msl": { source: "iana", extensions: ["msl"] }, "application/vnd.mobius.plc": { source: "iana", extensions: ["plc"] }, "application/vnd.mobius.txf": { source: "iana", extensions: ["txf"] }, "application/vnd.mophun.application": { source: "iana", extensions: ["mpn"] }, "application/vnd.mophun.certificate": { source: "iana", extensions: ["mpc"] }, "application/vnd.motorola.flexsuite": { source: "iana" }, "application/vnd.motorola.flexsuite.adsi": { source: "iana" }, "application/vnd.motorola.flexsuite.fis": { source: "iana" }, "application/vnd.motorola.flexsuite.gotap": { source: "iana" }, "application/vnd.motorola.flexsuite.kmr": { source: "iana" }, "application/vnd.motorola.flexsuite.ttc": { source: "iana" }, "application/vnd.motorola.flexsuite.wem": { source: "iana" }, "application/vnd.motorola.iprm": { source: "iana" }, "application/vnd.mozilla.xul+xml": { source: "iana", compressible: !0, extensions: ["xul"] }, "application/vnd.ms-3mfdocument": { source: "iana" }, "application/vnd.ms-artgalry": { source: "iana", extensions: ["cil"] }, "application/vnd.ms-asf": { source: "iana" }, "application/vnd.ms-cab-compressed": { source: "iana", extensions: ["cab"] }, "application/vnd.ms-color.iccprofile": { source: "apache" }, "application/vnd.ms-excel": { source: "iana", compressible: !1, extensions: ["xls", "xlm", "xla", "xlc", "xlt", "xlw"] }, "application/vnd.ms-excel.addin.macroenabled.12": { source: "iana", extensions: ["xlam"] }, "application/vnd.ms-excel.sheet.binary.macroenabled.12": { source: "iana", extensions: ["xlsb"] }, "application/vnd.ms-excel.sheet.macroenabled.12": { source: "iana", extensions: ["xlsm"] }, "application/vnd.ms-excel.template.macroenabled.12": { source: "iana", extensions: ["xltm"] }, "application/vnd.ms-fontobject": { source: "iana", compressible: !0, extensions: ["eot"] }, "application/vnd.ms-htmlhelp": { source: "iana", extensions: ["chm"] }, "application/vnd.ms-ims": { source: "iana", extensions: ["ims"] }, "application/vnd.ms-lrm": { source: "iana", extensions: ["lrm"] }, "application/vnd.ms-office.activex+xml": { source: "iana", compressible: !0 }, "application/vnd.ms-officetheme": { source: "iana", extensions: ["thmx"] }, "application/vnd.ms-opentype": { source: "apache", compressible: !0 }, "application/vnd.ms-outlook": { compressible: !1, extensions: ["msg"] }, "application/vnd.ms-package.obfuscated-opentype": { source: "apache" }, "application/vnd.ms-pki.seccat": { source: "apache", extensions: ["cat"] }, "application/vnd.ms-pki.stl": { source: "apache", extensions: ["stl"] }, "application/vnd.ms-playready.initiator+xml": { source: "iana", compressible: !0 }, "application/vnd.ms-powerpoint": { source: "iana", compressible: !1, extensions: ["ppt", "pps", "pot"] }, "application/vnd.ms-powerpoint.addin.macroenabled.12": { source: "iana", extensions: ["ppam"] }, "application/vnd.ms-powerpoint.presentation.macroenabled.12": { source: "iana", extensions: ["pptm"] }, "application/vnd.ms-powerpoint.slide.macroenabled.12": { source: "iana", extensions: ["sldm"] }, "application/vnd.ms-powerpoint.slideshow.macroenabled.12": { source: "iana", extensions: ["ppsm"] }, "application/vnd.ms-powerpoint.template.macroenabled.12": { source: "iana", extensions: ["potm"] }, "application/vnd.ms-printdevicecapabilities+xml": { source: "iana", compressible: !0 }, "application/vnd.ms-printing.printticket+xml": { source: "apache", compressible: !0 }, "application/vnd.ms-printschematicket+xml": { source: "iana", compressible: !0 }, "application/vnd.ms-project": { source: "iana", extensions: ["mpp", "mpt"] }, "application/vnd.ms-tnef": { source: "iana" }, "application/vnd.ms-windows.devicepairing": { source: "iana" }, "application/vnd.ms-windows.nwprinting.oob": { source: "iana" }, "application/vnd.ms-windows.printerpairing": { source: "iana" }, "application/vnd.ms-windows.wsd.oob": { source: "iana" }, "application/vnd.ms-wmdrm.lic-chlg-req": { source: "iana" }, "application/vnd.ms-wmdrm.lic-resp": { source: "iana" }, "application/vnd.ms-wmdrm.meter-chlg-req": { source: "iana" }, "application/vnd.ms-wmdrm.meter-resp": { source: "iana" }, "application/vnd.ms-word.document.macroenabled.12": { source: "iana", extensions: ["docm"] }, "application/vnd.ms-word.template.macroenabled.12": { source: "iana", extensions: ["dotm"] }, "application/vnd.ms-works": { source: "iana", extensions: ["wps", "wks", "wcm", "wdb"] }, "application/vnd.ms-wpl": { source: "iana", extensions: ["wpl"] }, "application/vnd.ms-xpsdocument": { source: "iana", compressible: !1, extensions: ["xps"] }, "application/vnd.msa-disk-image": { source: "iana" }, "application/vnd.mseq": { source: "iana", extensions: ["mseq"] }, "application/vnd.msign": { source: "iana" }, "application/vnd.multiad.creator": { source: "iana" }, "application/vnd.multiad.creator.cif": { source: "iana" }, "application/vnd.music-niff": { source: "iana" }, "application/vnd.musician": { source: "iana", extensions: ["mus"] }, "application/vnd.muvee.style": { source: "iana", extensions: ["msty"] }, "application/vnd.mynfc": { source: "iana", extensions: ["taglet"] }, "application/vnd.nacamar.ybrid+json": { source: "iana", compressible: !0 }, "application/vnd.ncd.control": { source: "iana" }, "application/vnd.ncd.reference": { source: "iana" }, "application/vnd.nearst.inv+json": { source: "iana", compressible: !0 }, "application/vnd.nebumind.line": { source: "iana" }, "application/vnd.nervana": { source: "iana" }, "application/vnd.netfpx": { source: "iana" }, "application/vnd.neurolanguage.nlu": { source: "iana", extensions: ["nlu"] }, "application/vnd.nimn": { source: "iana" }, "application/vnd.nintendo.nitro.rom": { source: "iana" }, "application/vnd.nintendo.snes.rom": { source: "iana" }, "application/vnd.nitf": { source: "iana", extensions: ["ntf", "nitf"] }, "application/vnd.noblenet-directory": { source: "iana", extensions: ["nnd"] }, "application/vnd.noblenet-sealer": { source: "iana", extensions: ["nns"] }, "application/vnd.noblenet-web": { source: "iana", extensions: ["nnw"] }, "application/vnd.nokia.catalogs": { source: "iana" }, "application/vnd.nokia.conml+wbxml": { source: "iana" }, "application/vnd.nokia.conml+xml": { source: "iana", compressible: !0 }, "application/vnd.nokia.iptv.config+xml": { source: "iana", compressible: !0 }, "application/vnd.nokia.isds-radio-presets": { source: "iana" }, "application/vnd.nokia.landmark+wbxml": { source: "iana" }, "application/vnd.nokia.landmark+xml": { source: "iana", compressible: !0 }, "application/vnd.nokia.landmarkcollection+xml": { source: "iana", compressible: !0 }, "application/vnd.nokia.n-gage.ac+xml": { source: "iana", compressible: !0, extensions: ["ac"] }, "application/vnd.nokia.n-gage.data": { source: "iana", extensions: ["ngdat"] }, "application/vnd.nokia.n-gage.symbian.install": { source: "iana", extensions: ["n-gage"] }, "application/vnd.nokia.ncd": { source: "iana" }, "application/vnd.nokia.pcd+wbxml": { source: "iana" }, "application/vnd.nokia.pcd+xml": { source: "iana", compressible: !0 }, "application/vnd.nokia.radio-preset": { source: "iana", extensions: ["rpst"] }, "application/vnd.nokia.radio-presets": { source: "iana", extensions: ["rpss"] }, "application/vnd.novadigm.edm": { source: "iana", extensions: ["edm"] }, "application/vnd.novadigm.edx": { source: "iana", extensions: ["edx"] }, "application/vnd.novadigm.ext": { source: "iana", extensions: ["ext"] }, "application/vnd.ntt-local.content-share": { source: "iana" }, "application/vnd.ntt-local.file-transfer": { source: "iana" }, "application/vnd.ntt-local.ogw_remote-access": { source: "iana" }, "application/vnd.ntt-local.sip-ta_remote": { source: "iana" }, "application/vnd.ntt-local.sip-ta_tcp_stream": { source: "iana" }, "application/vnd.oasis.opendocument.chart": { source: "iana", extensions: ["odc"] }, "application/vnd.oasis.opendocument.chart-template": { source: "iana", extensions: ["otc"] }, "application/vnd.oasis.opendocument.database": { source: "iana", extensions: ["odb"] }, "application/vnd.oasis.opendocument.formula": { source: "iana", extensions: ["odf"] }, "application/vnd.oasis.opendocument.formula-template": { source: "iana", extensions: ["odft"] }, "application/vnd.oasis.opendocument.graphics": { source: "iana", compressible: !1, extensions: ["odg"] }, "application/vnd.oasis.opendocument.graphics-template": { source: "iana", extensions: ["otg"] }, "application/vnd.oasis.opendocument.image": { source: "iana", extensions: ["odi"] }, "application/vnd.oasis.opendocument.image-template": { source: "iana", extensions: ["oti"] }, "application/vnd.oasis.opendocument.presentation": { source: "iana", compressible: !1, extensions: ["odp"] }, "application/vnd.oasis.opendocument.presentation-template": { source: "iana", extensions: ["otp"] }, "application/vnd.oasis.opendocument.spreadsheet": { source: "iana", compressible: !1, extensions: ["ods"] }, "application/vnd.oasis.opendocument.spreadsheet-template": { source: "iana", extensions: ["ots"] }, "application/vnd.oasis.opendocument.text": { source: "iana", compressible: !1, extensions: ["odt"] }, "application/vnd.oasis.opendocument.text-master": { source: "iana", extensions: ["odm"] }, "application/vnd.oasis.opendocument.text-template": { source: "iana", extensions: ["ott"] }, "application/vnd.oasis.opendocument.text-web": { source: "iana", extensions: ["oth"] }, "application/vnd.obn": { source: "iana" }, "application/vnd.ocf+cbor": { source: "iana" }, "application/vnd.oci.image.manifest.v1+json": { source: "iana", compressible: !0 }, "application/vnd.oftn.l10n+json": { source: "iana", compressible: !0 }, "application/vnd.oipf.contentaccessdownload+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.contentaccessstreaming+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.cspg-hexbinary": { source: "iana" }, "application/vnd.oipf.dae.svg+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.dae.xhtml+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.mippvcontrolmessage+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.pae.gem": { source: "iana" }, "application/vnd.oipf.spdiscovery+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.spdlist+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.ueprofile+xml": { source: "iana", compressible: !0 }, "application/vnd.oipf.userprofile+xml": { source: "iana", compressible: !0 }, "application/vnd.olpc-sugar": { source: "iana", extensions: ["xo"] }, "application/vnd.oma-scws-config": { source: "iana" }, "application/vnd.oma-scws-http-request": { source: "iana" }, "application/vnd.oma-scws-http-response": { source: "iana" }, "application/vnd.oma.bcast.associated-procedure-parameter+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.drm-trigger+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.imd+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.ltkm": { source: "iana" }, "application/vnd.oma.bcast.notification+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.provisioningtrigger": { source: "iana" }, "application/vnd.oma.bcast.sgboot": { source: "iana" }, "application/vnd.oma.bcast.sgdd+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.sgdu": { source: "iana" }, "application/vnd.oma.bcast.simple-symbol-container": { source: "iana" }, "application/vnd.oma.bcast.smartcard-trigger+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.sprov+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.bcast.stkm": { source: "iana" }, "application/vnd.oma.cab-address-book+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.cab-feature-handler+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.cab-pcc+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.cab-subs-invite+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.cab-user-prefs+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.dcd": { source: "iana" }, "application/vnd.oma.dcdc": { source: "iana" }, "application/vnd.oma.dd2+xml": { source: "iana", compressible: !0, extensions: ["dd2"] }, "application/vnd.oma.drm.risd+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.group-usage-list+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.lwm2m+cbor": { source: "iana" }, "application/vnd.oma.lwm2m+json": { source: "iana", compressible: !0 }, "application/vnd.oma.lwm2m+tlv": { source: "iana" }, "application/vnd.oma.pal+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.poc.detailed-progress-report+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.poc.final-report+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.poc.groups+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.poc.invocation-descriptor+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.poc.optimized-progress-report+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.push": { source: "iana" }, "application/vnd.oma.scidm.messages+xml": { source: "iana", compressible: !0 }, "application/vnd.oma.xcap-directory+xml": { source: "iana", compressible: !0 }, "application/vnd.omads-email+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.omads-file+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.omads-folder+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.omaloc-supl-init": { source: "iana" }, "application/vnd.onepager": { source: "iana" }, "application/vnd.onepagertamp": { source: "iana" }, "application/vnd.onepagertamx": { source: "iana" }, "application/vnd.onepagertat": { source: "iana" }, "application/vnd.onepagertatp": { source: "iana" }, "application/vnd.onepagertatx": { source: "iana" }, "application/vnd.openblox.game+xml": { source: "iana", compressible: !0, extensions: ["obgx"] }, "application/vnd.openblox.game-binary": { source: "iana" }, "application/vnd.openeye.oeb": { source: "iana" }, "application/vnd.openofficeorg.extension": { source: "apache", extensions: ["oxt"] }, "application/vnd.openstreetmap.data+xml": { source: "iana", compressible: !0, extensions: ["osm"] }, "application/vnd.opentimestamps.ots": { source: "iana" }, "application/vnd.openxmlformats-officedocument.custom-properties+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.customxmlproperties+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawing+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.chart+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.chartshapes+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.diagramcolors+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.diagramdata+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.diagramlayout+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.drawingml.diagramstyle+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.extended-properties+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.commentauthors+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.comments+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.handoutmaster+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.notesmaster+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.notesslide+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.presentation": { source: "iana", compressible: !1, extensions: ["pptx"] }, "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.presprops+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.slide": { source: "iana", extensions: ["sldx"] }, "application/vnd.openxmlformats-officedocument.presentationml.slide+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.slidelayout+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.slidemaster+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.slideshow": { source: "iana", extensions: ["ppsx"] }, "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.slideupdateinfo+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.tablestyles+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.tags+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.template": { source: "iana", extensions: ["potx"] }, "application/vnd.openxmlformats-officedocument.presentationml.template.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.presentationml.viewprops+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.calcchain+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.connections+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.externallink+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcachedefinition+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcacherecords+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.pivottable+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.querytable+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionheaders+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionlog+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedstrings+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": { source: "iana", compressible: !1, extensions: ["xlsx"] }, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetmetadata+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.tablesinglecells+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.template": { source: "iana", extensions: ["xltx"] }, "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.usernames+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.volatiledependencies+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.theme+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.themeoverride+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.vmldrawing": { source: "iana" }, "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.document": { source: "iana", compressible: !1, extensions: ["docx"] }, "application/vnd.openxmlformats-officedocument.wordprocessingml.document.glossary+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.fonttable+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.template": { source: "iana", extensions: ["dotx"] }, "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-officedocument.wordprocessingml.websettings+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-package.core-properties+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-package.digital-signature-xmlsignature+xml": { source: "iana", compressible: !0 }, "application/vnd.openxmlformats-package.relationships+xml": { source: "iana", compressible: !0 }, "application/vnd.oracle.resource+json": { source: "iana", compressible: !0 }, "application/vnd.orange.indata": { source: "iana" }, "application/vnd.osa.netdeploy": { source: "iana" }, "application/vnd.osgeo.mapguide.package": { source: "iana", extensions: ["mgp"] }, "application/vnd.osgi.bundle": { source: "iana" }, "application/vnd.osgi.dp": { source: "iana", extensions: ["dp"] }, "application/vnd.osgi.subsystem": { source: "iana", extensions: ["esa"] }, "application/vnd.otps.ct-kip+xml": { source: "iana", compressible: !0 }, "application/vnd.oxli.countgraph": { source: "iana" }, "application/vnd.pagerduty+json": { source: "iana", compressible: !0 }, "application/vnd.palm": { source: "iana", extensions: ["pdb", "pqa", "oprc"] }, "application/vnd.panoply": { source: "iana" }, "application/vnd.paos.xml": { source: "iana" }, "application/vnd.patentdive": { source: "iana" }, "application/vnd.patientecommsdoc": { source: "iana" }, "application/vnd.pawaafile": { source: "iana", extensions: ["paw"] }, "application/vnd.pcos": { source: "iana" }, "application/vnd.pg.format": { source: "iana", extensions: ["str"] }, "application/vnd.pg.osasli": { source: "iana", extensions: ["ei6"] }, "application/vnd.piaccess.application-licence": { source: "iana" }, "application/vnd.picsel": { source: "iana", extensions: ["efif"] }, "application/vnd.pmi.widget": { source: "iana", extensions: ["wg"] }, "application/vnd.poc.group-advertisement+xml": { source: "iana", compressible: !0 }, "application/vnd.pocketlearn": { source: "iana", extensions: ["plf"] }, "application/vnd.powerbuilder6": { source: "iana", extensions: ["pbd"] }, "application/vnd.powerbuilder6-s": { source: "iana" }, "application/vnd.powerbuilder7": { source: "iana" }, "application/vnd.powerbuilder7-s": { source: "iana" }, "application/vnd.powerbuilder75": { source: "iana" }, "application/vnd.powerbuilder75-s": { source: "iana" }, "application/vnd.preminet": { source: "iana" }, "application/vnd.previewsystems.box": { source: "iana", extensions: ["box"] }, "application/vnd.proteus.magazine": { source: "iana", extensions: ["mgz"] }, "application/vnd.psfs": { source: "iana" }, "application/vnd.publishare-delta-tree": { source: "iana", extensions: ["qps"] }, "application/vnd.pvi.ptid1": { source: "iana", extensions: ["ptid"] }, "application/vnd.pwg-multiplexed": { source: "iana" }, "application/vnd.pwg-xhtml-print+xml": { source: "iana", compressible: !0 }, "application/vnd.qualcomm.brew-app-res": { source: "iana" }, "application/vnd.quarantainenet": { source: "iana" }, "application/vnd.quark.quarkxpress": { source: "iana", extensions: ["qxd", "qxt", "qwd", "qwt", "qxl", "qxb"] }, "application/vnd.quobject-quoxdocument": { source: "iana" }, "application/vnd.radisys.moml+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-audit+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-audit-conf+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-audit-conn+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-audit-dialog+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-audit-stream+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-conf+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-base+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-fax-detect+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-fax-sendrecv+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-group+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-speech+xml": { source: "iana", compressible: !0 }, "application/vnd.radisys.msml-dialog-transform+xml": { source: "iana", compressible: !0 }, "application/vnd.rainstor.data": { source: "iana" }, "application/vnd.rapid": { source: "iana" }, "application/vnd.rar": { source: "iana", extensions: ["rar"] }, "application/vnd.realvnc.bed": { source: "iana", extensions: ["bed"] }, "application/vnd.recordare.musicxml": { source: "iana", extensions: ["mxl"] }, "application/vnd.recordare.musicxml+xml": { source: "iana", compressible: !0, extensions: ["musicxml"] }, "application/vnd.renlearn.rlprint": { source: "iana" }, "application/vnd.resilient.logic": { source: "iana" }, "application/vnd.restful+json": { source: "iana", compressible: !0 }, "application/vnd.rig.cryptonote": { source: "iana", extensions: ["cryptonote"] }, "application/vnd.rim.cod": { source: "apache", extensions: ["cod"] }, "application/vnd.rn-realmedia": { source: "apache", extensions: ["rm"] }, "application/vnd.rn-realmedia-vbr": { source: "apache", extensions: ["rmvb"] }, "application/vnd.route66.link66+xml": { source: "iana", compressible: !0, extensions: ["link66"] }, "application/vnd.rs-274x": { source: "iana" }, "application/vnd.ruckus.download": { source: "iana" }, "application/vnd.s3sms": { source: "iana" }, "application/vnd.sailingtracker.track": { source: "iana", extensions: ["st"] }, "application/vnd.sar": { source: "iana" }, "application/vnd.sbm.cid": { source: "iana" }, "application/vnd.sbm.mid2": { source: "iana" }, "application/vnd.scribus": { source: "iana" }, "application/vnd.sealed.3df": { source: "iana" }, "application/vnd.sealed.csf": { source: "iana" }, "application/vnd.sealed.doc": { source: "iana" }, "application/vnd.sealed.eml": { source: "iana" }, "application/vnd.sealed.mht": { source: "iana" }, "application/vnd.sealed.net": { source: "iana" }, "application/vnd.sealed.ppt": { source: "iana" }, "application/vnd.sealed.tiff": { source: "iana" }, "application/vnd.sealed.xls": { source: "iana" }, "application/vnd.sealedmedia.softseal.html": { source: "iana" }, "application/vnd.sealedmedia.softseal.pdf": { source: "iana" }, "application/vnd.seemail": { source: "iana", extensions: ["see"] }, "application/vnd.seis+json": { source: "iana", compressible: !0 }, "application/vnd.sema": { source: "iana", extensions: ["sema"] }, "application/vnd.semd": { source: "iana", extensions: ["semd"] }, "application/vnd.semf": { source: "iana", extensions: ["semf"] }, "application/vnd.shade-save-file": { source: "iana" }, "application/vnd.shana.informed.formdata": { source: "iana", extensions: ["ifm"] }, "application/vnd.shana.informed.formtemplate": { source: "iana", extensions: ["itp"] }, "application/vnd.shana.informed.interchange": { source: "iana", extensions: ["iif"] }, "application/vnd.shana.informed.package": { source: "iana", extensions: ["ipk"] }, "application/vnd.shootproof+json": { source: "iana", compressible: !0 }, "application/vnd.shopkick+json": { source: "iana", compressible: !0 }, "application/vnd.shp": { source: "iana" }, "application/vnd.shx": { source: "iana" }, "application/vnd.sigrok.session": { source: "iana" }, "application/vnd.simtech-mindmapper": { source: "iana", extensions: ["twd", "twds"] }, "application/vnd.siren+json": { source: "iana", compressible: !0 }, "application/vnd.smaf": { source: "iana", extensions: ["mmf"] }, "application/vnd.smart.notebook": { source: "iana" }, "application/vnd.smart.teacher": { source: "iana", extensions: ["teacher"] }, "application/vnd.snesdev-page-table": { source: "iana" }, "application/vnd.software602.filler.form+xml": { source: "iana", compressible: !0, extensions: ["fo"] }, "application/vnd.software602.filler.form-xml-zip": { source: "iana" }, "application/vnd.solent.sdkm+xml": { source: "iana", compressible: !0, extensions: ["sdkm", "sdkd"] }, "application/vnd.spotfire.dxp": { source: "iana", extensions: ["dxp"] }, "application/vnd.spotfire.sfs": { source: "iana", extensions: ["sfs"] }, "application/vnd.sqlite3": { source: "iana" }, "application/vnd.sss-cod": { source: "iana" }, "application/vnd.sss-dtf": { source: "iana" }, "application/vnd.sss-ntf": { source: "iana" }, "application/vnd.stardivision.calc": { source: "apache", extensions: ["sdc"] }, "application/vnd.stardivision.draw": { source: "apache", extensions: ["sda"] }, "application/vnd.stardivision.impress": { source: "apache", extensions: ["sdd"] }, "application/vnd.stardivision.math": { source: "apache", extensions: ["smf"] }, "application/vnd.stardivision.writer": { source: "apache", extensions: ["sdw", "vor"] }, "application/vnd.stardivision.writer-global": { source: "apache", extensions: ["sgl"] }, "application/vnd.stepmania.package": { source: "iana", extensions: ["smzip"] }, "application/vnd.stepmania.stepchart": { source: "iana", extensions: ["sm"] }, "application/vnd.street-stream": { source: "iana" }, "application/vnd.sun.wadl+xml": { source: "iana", compressible: !0, extensions: ["wadl"] }, "application/vnd.sun.xml.calc": { source: "apache", extensions: ["sxc"] }, "application/vnd.sun.xml.calc.template": { source: "apache", extensions: ["stc"] }, "application/vnd.sun.xml.draw": { source: "apache", extensions: ["sxd"] }, "application/vnd.sun.xml.draw.template": { source: "apache", extensions: ["std"] }, "application/vnd.sun.xml.impress": { source: "apache", extensions: ["sxi"] }, "application/vnd.sun.xml.impress.template": { source: "apache", extensions: ["sti"] }, "application/vnd.sun.xml.math": { source: "apache", extensions: ["sxm"] }, "application/vnd.sun.xml.writer": { source: "apache", extensions: ["sxw"] }, "application/vnd.sun.xml.writer.global": { source: "apache", extensions: ["sxg"] }, "application/vnd.sun.xml.writer.template": { source: "apache", extensions: ["stw"] }, "application/vnd.sus-calendar": { source: "iana", extensions: ["sus", "susp"] }, "application/vnd.svd": { source: "iana", extensions: ["svd"] }, "application/vnd.swiftview-ics": { source: "iana" }, "application/vnd.sycle+xml": { source: "iana", compressible: !0 }, "application/vnd.syft+json": { source: "iana", compressible: !0 }, "application/vnd.symbian.install": { source: "apache", extensions: ["sis", "sisx"] }, "application/vnd.syncml+xml": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["xsm"] }, "application/vnd.syncml.dm+wbxml": { source: "iana", charset: "UTF-8", extensions: ["bdm"] }, "application/vnd.syncml.dm+xml": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["xdm"] }, "application/vnd.syncml.dm.notification": { source: "iana" }, "application/vnd.syncml.dmddf+wbxml": { source: "iana" }, "application/vnd.syncml.dmddf+xml": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["ddf"] }, "application/vnd.syncml.dmtnds+wbxml": { source: "iana" }, "application/vnd.syncml.dmtnds+xml": { source: "iana", charset: "UTF-8", compressible: !0 }, "application/vnd.syncml.ds.notification": { source: "iana" }, "application/vnd.tableschema+json": { source: "iana", compressible: !0 }, "application/vnd.tao.intent-module-archive": { source: "iana", extensions: ["tao"] }, "application/vnd.tcpdump.pcap": { source: "iana", extensions: ["pcap", "cap", "dmp"] }, "application/vnd.think-cell.ppttc+json": { source: "iana", compressible: !0 }, "application/vnd.tmd.mediaflex.api+xml": { source: "iana", compressible: !0 }, "application/vnd.tml": { source: "iana" }, "application/vnd.tmobile-livetv": { source: "iana", extensions: ["tmo"] }, "application/vnd.tri.onesource": { source: "iana" }, "application/vnd.trid.tpt": { source: "iana", extensions: ["tpt"] }, "application/vnd.triscape.mxs": { source: "iana", extensions: ["mxs"] }, "application/vnd.trueapp": { source: "iana", extensions: ["tra"] }, "application/vnd.truedoc": { source: "iana" }, "application/vnd.ubisoft.webplayer": { source: "iana" }, "application/vnd.ufdl": { source: "iana", extensions: ["ufd", "ufdl"] }, "application/vnd.uiq.theme": { source: "iana", extensions: ["utz"] }, "application/vnd.umajin": { source: "iana", extensions: ["umj"] }, "application/vnd.unity": { source: "iana", extensions: ["unityweb"] }, "application/vnd.uoml+xml": { source: "iana", compressible: !0, extensions: ["uoml"] }, "application/vnd.uplanet.alert": { source: "iana" }, "application/vnd.uplanet.alert-wbxml": { source: "iana" }, "application/vnd.uplanet.bearer-choice": { source: "iana" }, "application/vnd.uplanet.bearer-choice-wbxml": { source: "iana" }, "application/vnd.uplanet.cacheop": { source: "iana" }, "application/vnd.uplanet.cacheop-wbxml": { source: "iana" }, "application/vnd.uplanet.channel": { source: "iana" }, "application/vnd.uplanet.channel-wbxml": { source: "iana" }, "application/vnd.uplanet.list": { source: "iana" }, "application/vnd.uplanet.list-wbxml": { source: "iana" }, "application/vnd.uplanet.listcmd": { source: "iana" }, "application/vnd.uplanet.listcmd-wbxml": { source: "iana" }, "application/vnd.uplanet.signal": { source: "iana" }, "application/vnd.uri-map": { source: "iana" }, "application/vnd.valve.source.material": { source: "iana" }, "application/vnd.vcx": { source: "iana", extensions: ["vcx"] }, "application/vnd.vd-study": { source: "iana" }, "application/vnd.vectorworks": { source: "iana" }, "application/vnd.vel+json": { source: "iana", compressible: !0 }, "application/vnd.verimatrix.vcas": { source: "iana" }, "application/vnd.veritone.aion+json": { source: "iana", compressible: !0 }, "application/vnd.veryant.thin": { source: "iana" }, "application/vnd.ves.encrypted": { source: "iana" }, "application/vnd.vidsoft.vidconference": { source: "iana" }, "application/vnd.visio": { source: "iana", extensions: ["vsd", "vst", "vss", "vsw"] }, "application/vnd.visionary": { source: "iana", extensions: ["vis"] }, "application/vnd.vividence.scriptfile": { source: "iana" }, "application/vnd.vsf": { source: "iana", extensions: ["vsf"] }, "application/vnd.wap.sic": { source: "iana" }, "application/vnd.wap.slc": { source: "iana" }, "application/vnd.wap.wbxml": { source: "iana", charset: "UTF-8", extensions: ["wbxml"] }, "application/vnd.wap.wmlc": { source: "iana", extensions: ["wmlc"] }, "application/vnd.wap.wmlscriptc": { source: "iana", extensions: ["wmlsc"] }, "application/vnd.webturbo": { source: "iana", extensions: ["wtb"] }, "application/vnd.wfa.dpp": { source: "iana" }, "application/vnd.wfa.p2p": { source: "iana" }, "application/vnd.wfa.wsc": { source: "iana" }, "application/vnd.windows.devicepairing": { source: "iana" }, "application/vnd.wmc": { source: "iana" }, "application/vnd.wmf.bootstrap": { source: "iana" }, "application/vnd.wolfram.mathematica": { source: "iana" }, "application/vnd.wolfram.mathematica.package": { source: "iana" }, "application/vnd.wolfram.player": { source: "iana", extensions: ["nbp"] }, "application/vnd.wordperfect": { source: "iana", extensions: ["wpd"] }, "application/vnd.wqd": { source: "iana", extensions: ["wqd"] }, "application/vnd.wrq-hp3000-labelled": { source: "iana" }, "application/vnd.wt.stf": { source: "iana", extensions: ["stf"] }, "application/vnd.wv.csp+wbxml": { source: "iana" }, "application/vnd.wv.csp+xml": { source: "iana", compressible: !0 }, "application/vnd.wv.ssp+xml": { source: "iana", compressible: !0 }, "application/vnd.xacml+json": { source: "iana", compressible: !0 }, "application/vnd.xara": { source: "iana", extensions: ["xar"] }, "application/vnd.xfdl": { source: "iana", extensions: ["xfdl"] }, "application/vnd.xfdl.webform": { source: "iana" }, "application/vnd.xmi+xml": { source: "iana", compressible: !0 }, "application/vnd.xmpie.cpkg": { source: "iana" }, "application/vnd.xmpie.dpkg": { source: "iana" }, "application/vnd.xmpie.plan": { source: "iana" }, "application/vnd.xmpie.ppkg": { source: "iana" }, "application/vnd.xmpie.xlim": { source: "iana" }, "application/vnd.yamaha.hv-dic": { source: "iana", extensions: ["hvd"] }, "application/vnd.yamaha.hv-script": { source: "iana", extensions: ["hvs"] }, "application/vnd.yamaha.hv-voice": { source: "iana", extensions: ["hvp"] }, "application/vnd.yamaha.openscoreformat": { source: "iana", extensions: ["osf"] }, "application/vnd.yamaha.openscoreformat.osfpvg+xml": { source: "iana", compressible: !0, extensions: ["osfpvg"] }, "application/vnd.yamaha.remote-setup": { source: "iana" }, "application/vnd.yamaha.smaf-audio": { source: "iana", extensions: ["saf"] }, "application/vnd.yamaha.smaf-phrase": { source: "iana", extensions: ["spf"] }, "application/vnd.yamaha.through-ngn": { source: "iana" }, "application/vnd.yamaha.tunnel-udpencap": { source: "iana" }, "application/vnd.yaoweme": { source: "iana" }, "application/vnd.yellowriver-custom-menu": { source: "iana", extensions: ["cmp"] }, "application/vnd.youtube.yt": { source: "iana" }, "application/vnd.zul": { source: "iana", extensions: ["zir", "zirz"] }, "application/vnd.zzazz.deck+xml": { source: "iana", compressible: !0, extensions: ["zaz"] }, "application/voicexml+xml": { source: "iana", compressible: !0, extensions: ["vxml"] }, "application/voucher-cms+json": { source: "iana", compressible: !0 }, "application/vq-rtcpxr": { source: "iana" }, "application/wasm": { source: "iana", compressible: !0, extensions: ["wasm"] }, "application/watcherinfo+xml": { source: "iana", compressible: !0, extensions: ["wif"] }, "application/webpush-options+json": { source: "iana", compressible: !0 }, "application/whoispp-query": { source: "iana" }, "application/whoispp-response": { source: "iana" }, "application/widget": { source: "iana", extensions: ["wgt"] }, "application/winhlp": { source: "apache", extensions: ["hlp"] }, "application/wita": { source: "iana" }, "application/wordperfect5.1": { source: "iana" }, "application/wsdl+xml": { source: "iana", compressible: !0, extensions: ["wsdl"] }, "application/wspolicy+xml": { source: "iana", compressible: !0, extensions: ["wspolicy"] }, "application/x-7z-compressed": { source: "apache", compressible: !1, extensions: ["7z"] }, "application/x-abiword": { source: "apache", extensions: ["abw"] }, "application/x-ace-compressed": { source: "apache", extensions: ["ace"] }, "application/x-amf": { source: "apache" }, "application/x-apple-diskimage": { source: "apache", extensions: ["dmg"] }, "application/x-arj": { compressible: !1, extensions: ["arj"] }, "application/x-authorware-bin": { source: "apache", extensions: ["aab", "x32", "u32", "vox"] }, "application/x-authorware-map": { source: "apache", extensions: ["aam"] }, "application/x-authorware-seg": { source: "apache", extensions: ["aas"] }, "application/x-bcpio": { source: "apache", extensions: ["bcpio"] }, "application/x-bdoc": { compressible: !1, extensions: ["bdoc"] }, "application/x-bittorrent": { source: "apache", extensions: ["torrent"] }, "application/x-blorb": { source: "apache", extensions: ["blb", "blorb"] }, "application/x-bzip": { source: "apache", compressible: !1, extensions: ["bz"] }, "application/x-bzip2": { source: "apache", compressible: !1, extensions: ["bz2", "boz"] }, "application/x-cbr": { source: "apache", extensions: ["cbr", "cba", "cbt", "cbz", "cb7"] }, "application/x-cdlink": { source: "apache", extensions: ["vcd"] }, "application/x-cfs-compressed": { source: "apache", extensions: ["cfs"] }, "application/x-chat": { source: "apache", extensions: ["chat"] }, "application/x-chess-pgn": { source: "apache", extensions: ["pgn"] }, "application/x-chrome-extension": { extensions: ["crx"] }, "application/x-cocoa": { source: "nginx", extensions: ["cco"] }, "application/x-compress": { source: "apache" }, "application/x-conference": { source: "apache", extensions: ["nsc"] }, "application/x-cpio": { source: "apache", extensions: ["cpio"] }, "application/x-csh": { source: "apache", extensions: ["csh"] }, "application/x-deb": { compressible: !1 }, "application/x-debian-package": { source: "apache", extensions: ["deb", "udeb"] }, "application/x-dgc-compressed": { source: "apache", extensions: ["dgc"] }, "application/x-director": { source: "apache", extensions: ["dir", "dcr", "dxr", "cst", "cct", "cxt", "w3d", "fgd", "swa"] }, "application/x-doom": { source: "apache", extensions: ["wad"] }, "application/x-dtbncx+xml": { source: "apache", compressible: !0, extensions: ["ncx"] }, "application/x-dtbook+xml": { source: "apache", compressible: !0, extensions: ["dtb"] }, "application/x-dtbresource+xml": { source: "apache", compressible: !0, extensions: ["res"] }, "application/x-dvi": { source: "apache", compressible: !1, extensions: ["dvi"] }, "application/x-envoy": { source: "apache", extensions: ["evy"] }, "application/x-eva": { source: "apache", extensions: ["eva"] }, "application/x-font-bdf": { source: "apache", extensions: ["bdf"] }, "application/x-font-dos": { source: "apache" }, "application/x-font-framemaker": { source: "apache" }, "application/x-font-ghostscript": { source: "apache", extensions: ["gsf"] }, "application/x-font-libgrx": { source: "apache" }, "application/x-font-linux-psf": { source: "apache", extensions: ["psf"] }, "application/x-font-pcf": { source: "apache", extensions: ["pcf"] }, "application/x-font-snf": { source: "apache", extensions: ["snf"] }, "application/x-font-speedo": { source: "apache" }, "application/x-font-sunos-news": { source: "apache" }, "application/x-font-type1": { source: "apache", extensions: ["pfa", "pfb", "pfm", "afm"] }, "application/x-font-vfont": { source: "apache" }, "application/x-freearc": { source: "apache", extensions: ["arc"] }, "application/x-futuresplash": { source: "apache", extensions: ["spl"] }, "application/x-gca-compressed": { source: "apache", extensions: ["gca"] }, "application/x-glulx": { source: "apache", extensions: ["ulx"] }, "application/x-gnumeric": { source: "apache", extensions: ["gnumeric"] }, "application/x-gramps-xml": { source: "apache", extensions: ["gramps"] }, "application/x-gtar": { source: "apache", extensions: ["gtar"] }, "application/x-gzip": { source: "apache" }, "application/x-hdf": { source: "apache", extensions: ["hdf"] }, "application/x-httpd-php": { compressible: !0, extensions: ["php"] }, "application/x-install-instructions": { source: "apache", extensions: ["install"] }, "application/x-iso9660-image": { source: "apache", extensions: ["iso"] }, "application/x-iwork-keynote-sffkey": { extensions: ["key"] }, "application/x-iwork-numbers-sffnumbers": { extensions: ["numbers"] }, "application/x-iwork-pages-sffpages": { extensions: ["pages"] }, "application/x-java-archive-diff": { source: "nginx", extensions: ["jardiff"] }, "application/x-java-jnlp-file": { source: "apache", compressible: !1, extensions: ["jnlp"] }, "application/x-javascript": { compressible: !0 }, "application/x-keepass2": { extensions: ["kdbx"] }, "application/x-latex": { source: "apache", compressible: !1, extensions: ["latex"] }, "application/x-lua-bytecode": { extensions: ["luac"] }, "application/x-lzh-compressed": { source: "apache", extensions: ["lzh", "lha"] }, "application/x-makeself": { source: "nginx", extensions: ["run"] }, "application/x-mie": { source: "apache", extensions: ["mie"] }, "application/x-mobipocket-ebook": { source: "apache", extensions: ["prc", "mobi"] }, "application/x-mpegurl": { compressible: !1 }, "application/x-ms-application": { source: "apache", extensions: ["application"] }, "application/x-ms-shortcut": { source: "apache", extensions: ["lnk"] }, "application/x-ms-wmd": { source: "apache", extensions: ["wmd"] }, "application/x-ms-wmz": { source: "apache", extensions: ["wmz"] }, "application/x-ms-xbap": { source: "apache", extensions: ["xbap"] }, "application/x-msaccess": { source: "apache", extensions: ["mdb"] }, "application/x-msbinder": { source: "apache", extensions: ["obd"] }, "application/x-mscardfile": { source: "apache", extensions: ["crd"] }, "application/x-msclip": { source: "apache", extensions: ["clp"] }, "application/x-msdos-program": { extensions: ["exe"] }, "application/x-msdownload": { source: "apache", extensions: ["exe", "dll", "com", "bat", "msi"] }, "application/x-msmediaview": { source: "apache", extensions: ["mvb", "m13", "m14"] }, "application/x-msmetafile": { source: "apache", extensions: ["wmf", "wmz", "emf", "emz"] }, "application/x-msmoney": { source: "apache", extensions: ["mny"] }, "application/x-mspublisher": { source: "apache", extensions: ["pub"] }, "application/x-msschedule": { source: "apache", extensions: ["scd"] }, "application/x-msterminal": { source: "apache", extensions: ["trm"] }, "application/x-mswrite": { source: "apache", extensions: ["wri"] }, "application/x-netcdf": { source: "apache", extensions: ["nc", "cdf"] }, "application/x-ns-proxy-autoconfig": { compressible: !0, extensions: ["pac"] }, "application/x-nzb": { source: "apache", extensions: ["nzb"] }, "application/x-perl": { source: "nginx", extensions: ["pl", "pm"] }, "application/x-pilot": { source: "nginx", extensions: ["prc", "pdb"] }, "application/x-pkcs12": { source: "apache", compressible: !1, extensions: ["p12", "pfx"] }, "application/x-pkcs7-certificates": { source: "apache", extensions: ["p7b", "spc"] }, "application/x-pkcs7-certreqresp": { source: "apache", extensions: ["p7r"] }, "application/x-pki-message": { source: "iana" }, "application/x-rar-compressed": { source: "apache", compressible: !1, extensions: ["rar"] }, "application/x-redhat-package-manager": { source: "nginx", extensions: ["rpm"] }, "application/x-research-info-systems": { source: "apache", extensions: ["ris"] }, "application/x-sea": { source: "nginx", extensions: ["sea"] }, "application/x-sh": { source: "apache", compressible: !0, extensions: ["sh"] }, "application/x-shar": { source: "apache", extensions: ["shar"] }, "application/x-shockwave-flash": { source: "apache", compressible: !1, extensions: ["swf"] }, "application/x-silverlight-app": { source: "apache", extensions: ["xap"] }, "application/x-sql": { source: "apache", extensions: ["sql"] }, "application/x-stuffit": { source: "apache", compressible: !1, extensions: ["sit"] }, "application/x-stuffitx": { source: "apache", extensions: ["sitx"] }, "application/x-subrip": { source: "apache", extensions: ["srt"] }, "application/x-sv4cpio": { source: "apache", extensions: ["sv4cpio"] }, "application/x-sv4crc": { source: "apache", extensions: ["sv4crc"] }, "application/x-t3vm-image": { source: "apache", extensions: ["t3"] }, "application/x-tads": { source: "apache", extensions: ["gam"] }, "application/x-tar": { source: "apache", compressible: !0, extensions: ["tar"] }, "application/x-tcl": { source: "apache", extensions: ["tcl", "tk"] }, "application/x-tex": { source: "apache", extensions: ["tex"] }, "application/x-tex-tfm": { source: "apache", extensions: ["tfm"] }, "application/x-texinfo": { source: "apache", extensions: ["texinfo", "texi"] }, "application/x-tgif": { source: "apache", extensions: ["obj"] }, "application/x-ustar": { source: "apache", extensions: ["ustar"] }, "application/x-virtualbox-hdd": { compressible: !0, extensions: ["hdd"] }, "application/x-virtualbox-ova": { compressible: !0, extensions: ["ova"] }, "application/x-virtualbox-ovf": { compressible: !0, extensions: ["ovf"] }, "application/x-virtualbox-vbox": { compressible: !0, extensions: ["vbox"] }, "application/x-virtualbox-vbox-extpack": { compressible: !1, extensions: ["vbox-extpack"] }, "application/x-virtualbox-vdi": { compressible: !0, extensions: ["vdi"] }, "application/x-virtualbox-vhd": { compressible: !0, extensions: ["vhd"] }, "application/x-virtualbox-vmdk": { compressible: !0, extensions: ["vmdk"] }, "application/x-wais-source": { source: "apache", extensions: ["src"] }, "application/x-web-app-manifest+json": { compressible: !0, extensions: ["webapp"] }, "application/x-www-form-urlencoded": { source: "iana", compressible: !0 }, "application/x-x509-ca-cert": { source: "iana", extensions: ["der", "crt", "pem"] }, "application/x-x509-ca-ra-cert": { source: "iana" }, "application/x-x509-next-ca-cert": { source: "iana" }, "application/x-xfig": { source: "apache", extensions: ["fig"] }, "application/x-xliff+xml": { source: "apache", compressible: !0, extensions: ["xlf"] }, "application/x-xpinstall": { source: "apache", compressible: !1, extensions: ["xpi"] }, "application/x-xz": { source: "apache", extensions: ["xz"] }, "application/x-zmachine": { source: "apache", extensions: ["z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8"] }, "application/x400-bp": { source: "iana" }, "application/xacml+xml": { source: "iana", compressible: !0 }, "application/xaml+xml": { source: "apache", compressible: !0, extensions: ["xaml"] }, "application/xcap-att+xml": { source: "iana", compressible: !0, extensions: ["xav"] }, "application/xcap-caps+xml": { source: "iana", compressible: !0, extensions: ["xca"] }, "application/xcap-diff+xml": { source: "iana", compressible: !0, extensions: ["xdf"] }, "application/xcap-el+xml": { source: "iana", compressible: !0, extensions: ["xel"] }, "application/xcap-error+xml": { source: "iana", compressible: !0 }, "application/xcap-ns+xml": { source: "iana", compressible: !0, extensions: ["xns"] }, "application/xcon-conference-info+xml": { source: "iana", compressible: !0 }, "application/xcon-conference-info-diff+xml": { source: "iana", compressible: !0 }, "application/xenc+xml": { source: "iana", compressible: !0, extensions: ["xenc"] }, "application/xhtml+xml": { source: "iana", compressible: !0, extensions: ["xhtml", "xht"] }, "application/xhtml-voice+xml": { source: "apache", compressible: !0 }, "application/xliff+xml": { source: "iana", compressible: !0, extensions: ["xlf"] }, "application/xml": { source: "iana", compressible: !0, extensions: ["xml", "xsl", "xsd", "rng"] }, "application/xml-dtd": { source: "iana", compressible: !0, extensions: ["dtd"] }, "application/xml-external-parsed-entity": { source: "iana" }, "application/xml-patch+xml": { source: "iana", compressible: !0 }, "application/xmpp+xml": { source: "iana", compressible: !0 }, "application/xop+xml": { source: "iana", compressible: !0, extensions: ["xop"] }, "application/xproc+xml": { source: "apache", compressible: !0, extensions: ["xpl"] }, "application/xslt+xml": { source: "iana", compressible: !0, extensions: ["xsl", "xslt"] }, "application/xspf+xml": { source: "apache", compressible: !0, extensions: ["xspf"] }, "application/xv+xml": { source: "iana", compressible: !0, extensions: ["mxml", "xhvml", "xvml", "xvm"] }, "application/yang": { source: "iana", extensions: ["yang"] }, "application/yang-data+json": { source: "iana", compressible: !0 }, "application/yang-data+xml": { source: "iana", compressible: !0 }, "application/yang-patch+json": { source: "iana", compressible: !0 }, "application/yang-patch+xml": { source: "iana", compressible: !0 }, "application/yin+xml": { source: "iana", compressible: !0, extensions: ["yin"] }, "application/zip": { source: "iana", compressible: !1, extensions: ["zip"] }, "application/zlib": { source: "iana" }, "application/zstd": { source: "iana" }, "audio/1d-interleaved-parityfec": { source: "iana" }, "audio/32kadpcm": { source: "iana" }, "audio/3gpp": { source: "iana", compressible: !1, extensions: ["3gpp"] }, "audio/3gpp2": { source: "iana" }, "audio/aac": { source: "iana" }, "audio/ac3": { source: "iana" }, "audio/adpcm": { source: "apache", extensions: ["adp"] }, "audio/amr": { source: "iana", extensions: ["amr"] }, "audio/amr-wb": { source: "iana" }, "audio/amr-wb+": { source: "iana" }, "audio/aptx": { source: "iana" }, "audio/asc": { source: "iana" }, "audio/atrac-advanced-lossless": { source: "iana" }, "audio/atrac-x": { source: "iana" }, "audio/atrac3": { source: "iana" }, "audio/basic": { source: "iana", compressible: !1, extensions: ["au", "snd"] }, "audio/bv16": { source: "iana" }, "audio/bv32": { source: "iana" }, "audio/clearmode": { source: "iana" }, "audio/cn": { source: "iana" }, "audio/dat12": { source: "iana" }, "audio/dls": { source: "iana" }, "audio/dsr-es201108": { source: "iana" }, "audio/dsr-es202050": { source: "iana" }, "audio/dsr-es202211": { source: "iana" }, "audio/dsr-es202212": { source: "iana" }, "audio/dv": { source: "iana" }, "audio/dvi4": { source: "iana" }, "audio/eac3": { source: "iana" }, "audio/encaprtp": { source: "iana" }, "audio/evrc": { source: "iana" }, "audio/evrc-qcp": { source: "iana" }, "audio/evrc0": { source: "iana" }, "audio/evrc1": { source: "iana" }, "audio/evrcb": { source: "iana" }, "audio/evrcb0": { source: "iana" }, "audio/evrcb1": { source: "iana" }, "audio/evrcnw": { source: "iana" }, "audio/evrcnw0": { source: "iana" }, "audio/evrcnw1": { source: "iana" }, "audio/evrcwb": { source: "iana" }, "audio/evrcwb0": { source: "iana" }, "audio/evrcwb1": { source: "iana" }, "audio/evs": { source: "iana" }, "audio/flexfec": { source: "iana" }, "audio/fwdred": { source: "iana" }, "audio/g711-0": { source: "iana" }, "audio/g719": { source: "iana" }, "audio/g722": { source: "iana" }, "audio/g7221": { source: "iana" }, "audio/g723": { source: "iana" }, "audio/g726-16": { source: "iana" }, "audio/g726-24": { source: "iana" }, "audio/g726-32": { source: "iana" }, "audio/g726-40": { source: "iana" }, "audio/g728": { source: "iana" }, "audio/g729": { source: "iana" }, "audio/g7291": { source: "iana" }, "audio/g729d": { source: "iana" }, "audio/g729e": { source: "iana" }, "audio/gsm": { source: "iana" }, "audio/gsm-efr": { source: "iana" }, "audio/gsm-hr-08": { source: "iana" }, "audio/ilbc": { source: "iana" }, "audio/ip-mr_v2.5": { source: "iana" }, "audio/isac": { source: "apache" }, "audio/l16": { source: "iana" }, "audio/l20": { source: "iana" }, "audio/l24": { source: "iana", compressible: !1 }, "audio/l8": { source: "iana" }, "audio/lpc": { source: "iana" }, "audio/melp": { source: "iana" }, "audio/melp1200": { source: "iana" }, "audio/melp2400": { source: "iana" }, "audio/melp600": { source: "iana" }, "audio/mhas": { source: "iana" }, "audio/midi": { source: "apache", extensions: ["mid", "midi", "kar", "rmi"] }, "audio/mobile-xmf": { source: "iana", extensions: ["mxmf"] }, "audio/mp3": { compressible: !1, extensions: ["mp3"] }, "audio/mp4": { source: "iana", compressible: !1, extensions: ["m4a", "mp4a"] }, "audio/mp4a-latm": { source: "iana" }, "audio/mpa": { source: "iana" }, "audio/mpa-robust": { source: "iana" }, "audio/mpeg": { source: "iana", compressible: !1, extensions: ["mpga", "mp2", "mp2a", "mp3", "m2a", "m3a"] }, "audio/mpeg4-generic": { source: "iana" }, "audio/musepack": { source: "apache" }, "audio/ogg": { source: "iana", compressible: !1, extensions: ["oga", "ogg", "spx", "opus"] }, "audio/opus": { source: "iana" }, "audio/parityfec": { source: "iana" }, "audio/pcma": { source: "iana" }, "audio/pcma-wb": { source: "iana" }, "audio/pcmu": { source: "iana" }, "audio/pcmu-wb": { source: "iana" }, "audio/prs.sid": { source: "iana" }, "audio/qcelp": { source: "iana" }, "audio/raptorfec": { source: "iana" }, "audio/red": { source: "iana" }, "audio/rtp-enc-aescm128": { source: "iana" }, "audio/rtp-midi": { source: "iana" }, "audio/rtploopback": { source: "iana" }, "audio/rtx": { source: "iana" }, "audio/s3m": { source: "apache", extensions: ["s3m"] }, "audio/scip": { source: "iana" }, "audio/silk": { source: "apache", extensions: ["sil"] }, "audio/smv": { source: "iana" }, "audio/smv-qcp": { source: "iana" }, "audio/smv0": { source: "iana" }, "audio/sofa": { source: "iana" }, "audio/sp-midi": { source: "iana" }, "audio/speex": { source: "iana" }, "audio/t140c": { source: "iana" }, "audio/t38": { source: "iana" }, "audio/telephone-event": { source: "iana" }, "audio/tetra_acelp": { source: "iana" }, "audio/tetra_acelp_bb": { source: "iana" }, "audio/tone": { source: "iana" }, "audio/tsvcis": { source: "iana" }, "audio/uemclip": { source: "iana" }, "audio/ulpfec": { source: "iana" }, "audio/usac": { source: "iana" }, "audio/vdvi": { source: "iana" }, "audio/vmr-wb": { source: "iana" }, "audio/vnd.3gpp.iufp": { source: "iana" }, "audio/vnd.4sb": { source: "iana" }, "audio/vnd.audiokoz": { source: "iana" }, "audio/vnd.celp": { source: "iana" }, "audio/vnd.cisco.nse": { source: "iana" }, "audio/vnd.cmles.radio-events": { source: "iana" }, "audio/vnd.cns.anp1": { source: "iana" }, "audio/vnd.cns.inf1": { source: "iana" }, "audio/vnd.dece.audio": { source: "iana", extensions: ["uva", "uvva"] }, "audio/vnd.digital-winds": { source: "iana", extensions: ["eol"] }, "audio/vnd.dlna.adts": { source: "iana" }, "audio/vnd.dolby.heaac.1": { source: "iana" }, "audio/vnd.dolby.heaac.2": { source: "iana" }, "audio/vnd.dolby.mlp": { source: "iana" }, "audio/vnd.dolby.mps": { source: "iana" }, "audio/vnd.dolby.pl2": { source: "iana" }, "audio/vnd.dolby.pl2x": { source: "iana" }, "audio/vnd.dolby.pl2z": { source: "iana" }, "audio/vnd.dolby.pulse.1": { source: "iana" }, "audio/vnd.dra": { source: "iana", extensions: ["dra"] }, "audio/vnd.dts": { source: "iana", extensions: ["dts"] }, "audio/vnd.dts.hd": { source: "iana", extensions: ["dtshd"] }, "audio/vnd.dts.uhd": { source: "iana" }, "audio/vnd.dvb.file": { source: "iana" }, "audio/vnd.everad.plj": { source: "iana" }, "audio/vnd.hns.audio": { source: "iana" }, "audio/vnd.lucent.voice": { source: "iana", extensions: ["lvp"] }, "audio/vnd.ms-playready.media.pya": { source: "iana", extensions: ["pya"] }, "audio/vnd.nokia.mobile-xmf": { source: "iana" }, "audio/vnd.nortel.vbk": { source: "iana" }, "audio/vnd.nuera.ecelp4800": { source: "iana", extensions: ["ecelp4800"] }, "audio/vnd.nuera.ecelp7470": { source: "iana", extensions: ["ecelp7470"] }, "audio/vnd.nuera.ecelp9600": { source: "iana", extensions: ["ecelp9600"] }, "audio/vnd.octel.sbc": { source: "iana" }, "audio/vnd.presonus.multitrack": { source: "iana" }, "audio/vnd.qcelp": { source: "iana" }, "audio/vnd.rhetorex.32kadpcm": { source: "iana" }, "audio/vnd.rip": { source: "iana", extensions: ["rip"] }, "audio/vnd.rn-realaudio": { compressible: !1 }, "audio/vnd.sealedmedia.softseal.mpeg": { source: "iana" }, "audio/vnd.vmx.cvsd": { source: "iana" }, "audio/vnd.wave": { compressible: !1 }, "audio/vorbis": { source: "iana", compressible: !1 }, "audio/vorbis-config": { source: "iana" }, "audio/wav": { compressible: !1, extensions: ["wav"] }, "audio/wave": { compressible: !1, extensions: ["wav"] }, "audio/webm": { source: "apache", compressible: !1, extensions: ["weba"] }, "audio/x-aac": { source: "apache", compressible: !1, extensions: ["aac"] }, "audio/x-aiff": { source: "apache", extensions: ["aif", "aiff", "aifc"] }, "audio/x-caf": { source: "apache", compressible: !1, extensions: ["caf"] }, "audio/x-flac": { source: "apache", extensions: ["flac"] }, "audio/x-m4a": { source: "nginx", extensions: ["m4a"] }, "audio/x-matroska": { source: "apache", extensions: ["mka"] }, "audio/x-mpegurl": { source: "apache", extensions: ["m3u"] }, "audio/x-ms-wax": { source: "apache", extensions: ["wax"] }, "audio/x-ms-wma": { source: "apache", extensions: ["wma"] }, "audio/x-pn-realaudio": { source: "apache", extensions: ["ram", "ra"] }, "audio/x-pn-realaudio-plugin": { source: "apache", extensions: ["rmp"] }, "audio/x-realaudio": { source: "nginx", extensions: ["ra"] }, "audio/x-tta": { source: "apache" }, "audio/x-wav": { source: "apache", extensions: ["wav"] }, "audio/xm": { source: "apache", extensions: ["xm"] }, "chemical/x-cdx": { source: "apache", extensions: ["cdx"] }, "chemical/x-cif": { source: "apache", extensions: ["cif"] }, "chemical/x-cmdf": { source: "apache", extensions: ["cmdf"] }, "chemical/x-cml": { source: "apache", extensions: ["cml"] }, "chemical/x-csml": { source: "apache", extensions: ["csml"] }, "chemical/x-pdb": { source: "apache" }, "chemical/x-xyz": { source: "apache", extensions: ["xyz"] }, "font/collection": { source: "iana", extensions: ["ttc"] }, "font/otf": { source: "iana", compressible: !0, extensions: ["otf"] }, "font/sfnt": { source: "iana" }, "font/ttf": { source: "iana", compressible: !0, extensions: ["ttf"] }, "font/woff": { source: "iana", extensions: ["woff"] }, "font/woff2": { source: "iana", extensions: ["woff2"] }, "image/aces": { source: "iana", extensions: ["exr"] }, "image/apng": { compressible: !1, extensions: ["apng"] }, "image/avci": { source: "iana", extensions: ["avci"] }, "image/avcs": { source: "iana", extensions: ["avcs"] }, "image/avif": { source: "iana", compressible: !1, extensions: ["avif"] }, "image/bmp": { source: "iana", compressible: !0, extensions: ["bmp"] }, "image/cgm": { source: "iana", extensions: ["cgm"] }, "image/dicom-rle": { source: "iana", extensions: ["drle"] }, "image/emf": { source: "iana", extensions: ["emf"] }, "image/fits": { source: "iana", extensions: ["fits"] }, "image/g3fax": { source: "iana", extensions: ["g3"] }, "image/gif": { source: "iana", compressible: !1, extensions: ["gif"] }, "image/heic": { source: "iana", extensions: ["heic"] }, "image/heic-sequence": { source: "iana", extensions: ["heics"] }, "image/heif": { source: "iana", extensions: ["heif"] }, "image/heif-sequence": { source: "iana", extensions: ["heifs"] }, "image/hej2k": { source: "iana", extensions: ["hej2"] }, "image/hsj2": { source: "iana", extensions: ["hsj2"] }, "image/ief": { source: "iana", extensions: ["ief"] }, "image/jls": { source: "iana", extensions: ["jls"] }, "image/jp2": { source: "iana", compressible: !1, extensions: ["jp2", "jpg2"] }, "image/jpeg": { source: "iana", compressible: !1, extensions: ["jpeg", "jpg", "jpe"] }, "image/jph": { source: "iana", extensions: ["jph"] }, "image/jphc": { source: "iana", extensions: ["jhc"] }, "image/jpm": { source: "iana", compressible: !1, extensions: ["jpm"] }, "image/jpx": { source: "iana", compressible: !1, extensions: ["jpx", "jpf"] }, "image/jxr": { source: "iana", extensions: ["jxr"] }, "image/jxra": { source: "iana", extensions: ["jxra"] }, "image/jxrs": { source: "iana", extensions: ["jxrs"] }, "image/jxs": { source: "iana", extensions: ["jxs"] }, "image/jxsc": { source: "iana", extensions: ["jxsc"] }, "image/jxsi": { source: "iana", extensions: ["jxsi"] }, "image/jxss": { source: "iana", extensions: ["jxss"] }, "image/ktx": { source: "iana", extensions: ["ktx"] }, "image/ktx2": { source: "iana", extensions: ["ktx2"] }, "image/naplps": { source: "iana" }, "image/pjpeg": { compressible: !1 }, "image/png": { source: "iana", compressible: !1, extensions: ["png"] }, "image/prs.btif": { source: "iana", extensions: ["btif"] }, "image/prs.pti": { source: "iana", extensions: ["pti"] }, "image/pwg-raster": { source: "iana" }, "image/sgi": { source: "apache", extensions: ["sgi"] }, "image/svg+xml": { source: "iana", compressible: !0, extensions: ["svg", "svgz"] }, "image/t38": { source: "iana", extensions: ["t38"] }, "image/tiff": { source: "iana", compressible: !1, extensions: ["tif", "tiff"] }, "image/tiff-fx": { source: "iana", extensions: ["tfx"] }, "image/vnd.adobe.photoshop": { source: "iana", compressible: !0, extensions: ["psd"] }, "image/vnd.airzip.accelerator.azv": { source: "iana", extensions: ["azv"] }, "image/vnd.cns.inf2": { source: "iana" }, "image/vnd.dece.graphic": { source: "iana", extensions: ["uvi", "uvvi", "uvg", "uvvg"] }, "image/vnd.djvu": { source: "iana", extensions: ["djvu", "djv"] }, "image/vnd.dvb.subtitle": { source: "iana", extensions: ["sub"] }, "image/vnd.dwg": { source: "iana", extensions: ["dwg"] }, "image/vnd.dxf": { source: "iana", extensions: ["dxf"] }, "image/vnd.fastbidsheet": { source: "iana", extensions: ["fbs"] }, "image/vnd.fpx": { source: "iana", extensions: ["fpx"] }, "image/vnd.fst": { source: "iana", extensions: ["fst"] }, "image/vnd.fujixerox.edmics-mmr": { source: "iana", extensions: ["mmr"] }, "image/vnd.fujixerox.edmics-rlc": { source: "iana", extensions: ["rlc"] }, "image/vnd.globalgraphics.pgb": { source: "iana" }, "image/vnd.microsoft.icon": { source: "iana", compressible: !0, extensions: ["ico"] }, "image/vnd.mix": { source: "iana" }, "image/vnd.mozilla.apng": { source: "iana" }, "image/vnd.ms-dds": { compressible: !0, extensions: ["dds"] }, "image/vnd.ms-modi": { source: "iana", extensions: ["mdi"] }, "image/vnd.ms-photo": { source: "apache", extensions: ["wdp"] }, "image/vnd.net-fpx": { source: "iana", extensions: ["npx"] }, "image/vnd.pco.b16": { source: "iana", extensions: ["b16"] }, "image/vnd.radiance": { source: "iana" }, "image/vnd.sealed.png": { source: "iana" }, "image/vnd.sealedmedia.softseal.gif": { source: "iana" }, "image/vnd.sealedmedia.softseal.jpg": { source: "iana" }, "image/vnd.svf": { source: "iana" }, "image/vnd.tencent.tap": { source: "iana", extensions: ["tap"] }, "image/vnd.valve.source.texture": { source: "iana", extensions: ["vtf"] }, "image/vnd.wap.wbmp": { source: "iana", extensions: ["wbmp"] }, "image/vnd.xiff": { source: "iana", extensions: ["xif"] }, "image/vnd.zbrush.pcx": { source: "iana", extensions: ["pcx"] }, "image/webp": { source: "apache", extensions: ["webp"] }, "image/wmf": { source: "iana", extensions: ["wmf"] }, "image/x-3ds": { source: "apache", extensions: ["3ds"] }, "image/x-cmu-raster": { source: "apache", extensions: ["ras"] }, "image/x-cmx": { source: "apache", extensions: ["cmx"] }, "image/x-freehand": { source: "apache", extensions: ["fh", "fhc", "fh4", "fh5", "fh7"] }, "image/x-icon": { source: "apache", compressible: !0, extensions: ["ico"] }, "image/x-jng": { source: "nginx", extensions: ["jng"] }, "image/x-mrsid-image": { source: "apache", extensions: ["sid"] }, "image/x-ms-bmp": { source: "nginx", compressible: !0, extensions: ["bmp"] }, "image/x-pcx": { source: "apache", extensions: ["pcx"] }, "image/x-pict": { source: "apache", extensions: ["pic", "pct"] }, "image/x-portable-anymap": { source: "apache", extensions: ["pnm"] }, "image/x-portable-bitmap": { source: "apache", extensions: ["pbm"] }, "image/x-portable-graymap": { source: "apache", extensions: ["pgm"] }, "image/x-portable-pixmap": { source: "apache", extensions: ["ppm"] }, "image/x-rgb": { source: "apache", extensions: ["rgb"] }, "image/x-tga": { source: "apache", extensions: ["tga"] }, "image/x-xbitmap": { source: "apache", extensions: ["xbm"] }, "image/x-xcf": { compressible: !1 }, "image/x-xpixmap": { source: "apache", extensions: ["xpm"] }, "image/x-xwindowdump": { source: "apache", extensions: ["xwd"] }, "message/cpim": { source: "iana" }, "message/delivery-status": { source: "iana" }, "message/disposition-notification": { source: "iana", extensions: ["disposition-notification"] }, "message/external-body": { source: "iana" }, "message/feedback-report": { source: "iana" }, "message/global": { source: "iana", extensions: ["u8msg"] }, "message/global-delivery-status": { source: "iana", extensions: ["u8dsn"] }, "message/global-disposition-notification": { source: "iana", extensions: ["u8mdn"] }, "message/global-headers": { source: "iana", extensions: ["u8hdr"] }, "message/http": { source: "iana", compressible: !1 }, "message/imdn+xml": { source: "iana", compressible: !0 }, "message/news": { source: "iana" }, "message/partial": { source: "iana", compressible: !1 }, "message/rfc822": { source: "iana", compressible: !0, extensions: ["eml", "mime"] }, "message/s-http": { source: "iana" }, "message/sip": { source: "iana" }, "message/sipfrag": { source: "iana" }, "message/tracking-status": { source: "iana" }, "message/vnd.si.simp": { source: "iana" }, "message/vnd.wfa.wsc": { source: "iana", extensions: ["wsc"] }, "model/3mf": { source: "iana", extensions: ["3mf"] }, "model/e57": { source: "iana" }, "model/gltf+json": { source: "iana", compressible: !0, extensions: ["gltf"] }, "model/gltf-binary": { source: "iana", compressible: !0, extensions: ["glb"] }, "model/iges": { source: "iana", compressible: !1, extensions: ["igs", "iges"] }, "model/mesh": { source: "iana", compressible: !1, extensions: ["msh", "mesh", "silo"] }, "model/mtl": { source: "iana", extensions: ["mtl"] }, "model/obj": { source: "iana", extensions: ["obj"] }, "model/step": { source: "iana" }, "model/step+xml": { source: "iana", compressible: !0, extensions: ["stpx"] }, "model/step+zip": { source: "iana", compressible: !1, extensions: ["stpz"] }, "model/step-xml+zip": { source: "iana", compressible: !1, extensions: ["stpxz"] }, "model/stl": { source: "iana", extensions: ["stl"] }, "model/vnd.collada+xml": { source: "iana", compressible: !0, extensions: ["dae"] }, "model/vnd.dwf": { source: "iana", extensions: ["dwf"] }, "model/vnd.flatland.3dml": { source: "iana" }, "model/vnd.gdl": { source: "iana", extensions: ["gdl"] }, "model/vnd.gs-gdl": { source: "apache" }, "model/vnd.gs.gdl": { source: "iana" }, "model/vnd.gtw": { source: "iana", extensions: ["gtw"] }, "model/vnd.moml+xml": { source: "iana", compressible: !0 }, "model/vnd.mts": { source: "iana", extensions: ["mts"] }, "model/vnd.opengex": { source: "iana", extensions: ["ogex"] }, "model/vnd.parasolid.transmit.binary": { source: "iana", extensions: ["x_b"] }, "model/vnd.parasolid.transmit.text": { source: "iana", extensions: ["x_t"] }, "model/vnd.pytha.pyox": { source: "iana" }, "model/vnd.rosette.annotated-data-model": { source: "iana" }, "model/vnd.sap.vds": { source: "iana", extensions: ["vds"] }, "model/vnd.usdz+zip": { source: "iana", compressible: !1, extensions: ["usdz"] }, "model/vnd.valve.source.compiled-map": { source: "iana", extensions: ["bsp"] }, "model/vnd.vtu": { source: "iana", extensions: ["vtu"] }, "model/vrml": { source: "iana", compressible: !1, extensions: ["wrl", "vrml"] }, "model/x3d+binary": { source: "apache", compressible: !1, extensions: ["x3db", "x3dbz"] }, "model/x3d+fastinfoset": { source: "iana", extensions: ["x3db"] }, "model/x3d+vrml": { source: "apache", compressible: !1, extensions: ["x3dv", "x3dvz"] }, "model/x3d+xml": { source: "iana", compressible: !0, extensions: ["x3d", "x3dz"] }, "model/x3d-vrml": { source: "iana", extensions: ["x3dv"] }, "multipart/alternative": { source: "iana", compressible: !1 }, "multipart/appledouble": { source: "iana" }, "multipart/byteranges": { source: "iana" }, "multipart/digest": { source: "iana" }, "multipart/encrypted": { source: "iana", compressible: !1 }, "multipart/form-data": { source: "iana", compressible: !1 }, "multipart/header-set": { source: "iana" }, "multipart/mixed": { source: "iana" }, "multipart/multilingual": { source: "iana" }, "multipart/parallel": { source: "iana" }, "multipart/related": { source: "iana", compressible: !1 }, "multipart/report": { source: "iana" }, "multipart/signed": { source: "iana", compressible: !1 }, "multipart/vnd.bint.med-plus": { source: "iana" }, "multipart/voice-message": { source: "iana" }, "multipart/x-mixed-replace": { source: "iana" }, "text/1d-interleaved-parityfec": { source: "iana" }, "text/cache-manifest": { source: "iana", compressible: !0, extensions: ["appcache", "manifest"] }, "text/calendar": { source: "iana", extensions: ["ics", "ifb"] }, "text/calender": { compressible: !0 }, "text/cmd": { compressible: !0 }, "text/coffeescript": { extensions: ["coffee", "litcoffee"] }, "text/cql": { source: "iana" }, "text/cql-expression": { source: "iana" }, "text/cql-identifier": { source: "iana" }, "text/css": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["css"] }, "text/csv": { source: "iana", compressible: !0, extensions: ["csv"] }, "text/csv-schema": { source: "iana" }, "text/directory": { source: "iana" }, "text/dns": { source: "iana" }, "text/ecmascript": { source: "iana" }, "text/encaprtp": { source: "iana" }, "text/enriched": { source: "iana" }, "text/fhirpath": { source: "iana" }, "text/flexfec": { source: "iana" }, "text/fwdred": { source: "iana" }, "text/gff3": { source: "iana" }, "text/grammar-ref-list": { source: "iana" }, "text/html": { source: "iana", compressible: !0, extensions: ["html", "htm", "shtml"] }, "text/jade": { extensions: ["jade"] }, "text/javascript": { source: "iana", compressible: !0 }, "text/jcr-cnd": { source: "iana" }, "text/jsx": { compressible: !0, extensions: ["jsx"] }, "text/less": { compressible: !0, extensions: ["less"] }, "text/markdown": { source: "iana", compressible: !0, extensions: ["markdown", "md"] }, "text/mathml": { source: "nginx", extensions: ["mml"] }, "text/mdx": { compressible: !0, extensions: ["mdx"] }, "text/mizar": { source: "iana" }, "text/n3": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["n3"] }, "text/parameters": { source: "iana", charset: "UTF-8" }, "text/parityfec": { source: "iana" }, "text/plain": { source: "iana", compressible: !0, extensions: ["txt", "text", "conf", "def", "list", "log", "in", "ini"] }, "text/provenance-notation": { source: "iana", charset: "UTF-8" }, "text/prs.fallenstein.rst": { source: "iana" }, "text/prs.lines.tag": { source: "iana", extensions: ["dsc"] }, "text/prs.prop.logic": { source: "iana" }, "text/raptorfec": { source: "iana" }, "text/red": { source: "iana" }, "text/rfc822-headers": { source: "iana" }, "text/richtext": { source: "iana", compressible: !0, extensions: ["rtx"] }, "text/rtf": { source: "iana", compressible: !0, extensions: ["rtf"] }, "text/rtp-enc-aescm128": { source: "iana" }, "text/rtploopback": { source: "iana" }, "text/rtx": { source: "iana" }, "text/sgml": { source: "iana", extensions: ["sgml", "sgm"] }, "text/shaclc": { source: "iana" }, "text/shex": { source: "iana", extensions: ["shex"] }, "text/slim": { extensions: ["slim", "slm"] }, "text/spdx": { source: "iana", extensions: ["spdx"] }, "text/strings": { source: "iana" }, "text/stylus": { extensions: ["stylus", "styl"] }, "text/t140": { source: "iana" }, "text/tab-separated-values": { source: "iana", compressible: !0, extensions: ["tsv"] }, "text/troff": { source: "iana", extensions: ["t", "tr", "roff", "man", "me", "ms"] }, "text/turtle": { source: "iana", charset: "UTF-8", extensions: ["ttl"] }, "text/ulpfec": { source: "iana" }, "text/uri-list": { source: "iana", compressible: !0, extensions: ["uri", "uris", "urls"] }, "text/vcard": { source: "iana", compressible: !0, extensions: ["vcard"] }, "text/vnd.a": { source: "iana" }, "text/vnd.abc": { source: "iana" }, "text/vnd.ascii-art": { source: "iana" }, "text/vnd.curl": { source: "iana", extensions: ["curl"] }, "text/vnd.curl.dcurl": { source: "apache", extensions: ["dcurl"] }, "text/vnd.curl.mcurl": { source: "apache", extensions: ["mcurl"] }, "text/vnd.curl.scurl": { source: "apache", extensions: ["scurl"] }, "text/vnd.debian.copyright": { source: "iana", charset: "UTF-8" }, "text/vnd.dmclientscript": { source: "iana" }, "text/vnd.dvb.subtitle": { source: "iana", extensions: ["sub"] }, "text/vnd.esmertec.theme-descriptor": { source: "iana", charset: "UTF-8" }, "text/vnd.familysearch.gedcom": { source: "iana", extensions: ["ged"] }, "text/vnd.ficlab.flt": { source: "iana" }, "text/vnd.fly": { source: "iana", extensions: ["fly"] }, "text/vnd.fmi.flexstor": { source: "iana", extensions: ["flx"] }, "text/vnd.gml": { source: "iana" }, "text/vnd.graphviz": { source: "iana", extensions: ["gv"] }, "text/vnd.hans": { source: "iana" }, "text/vnd.hgl": { source: "iana" }, "text/vnd.in3d.3dml": { source: "iana", extensions: ["3dml"] }, "text/vnd.in3d.spot": { source: "iana", extensions: ["spot"] }, "text/vnd.iptc.newsml": { source: "iana" }, "text/vnd.iptc.nitf": { source: "iana" }, "text/vnd.latex-z": { source: "iana" }, "text/vnd.motorola.reflex": { source: "iana" }, "text/vnd.ms-mediapackage": { source: "iana" }, "text/vnd.net2phone.commcenter.command": { source: "iana" }, "text/vnd.radisys.msml-basic-layout": { source: "iana" }, "text/vnd.senx.warpscript": { source: "iana" }, "text/vnd.si.uricatalogue": { source: "iana" }, "text/vnd.sosi": { source: "iana" }, "text/vnd.sun.j2me.app-descriptor": { source: "iana", charset: "UTF-8", extensions: ["jad"] }, "text/vnd.trolltech.linguist": { source: "iana", charset: "UTF-8" }, "text/vnd.wap.si": { source: "iana" }, "text/vnd.wap.sl": { source: "iana" }, "text/vnd.wap.wml": { source: "iana", extensions: ["wml"] }, "text/vnd.wap.wmlscript": { source: "iana", extensions: ["wmls"] }, "text/vtt": { source: "iana", charset: "UTF-8", compressible: !0, extensions: ["vtt"] }, "text/x-asm": { source: "apache", extensions: ["s", "asm"] }, "text/x-c": { source: "apache", extensions: ["c", "cc", "cxx", "cpp", "h", "hh", "dic"] }, "text/x-component": { source: "nginx", extensions: ["htc"] }, "text/x-fortran": { source: "apache", extensions: ["f", "for", "f77", "f90"] }, "text/x-gwt-rpc": { compressible: !0 }, "text/x-handlebars-template": { extensions: ["hbs"] }, "text/x-java-source": { source: "apache", extensions: ["java"] }, "text/x-jquery-tmpl": { compressible: !0 }, "text/x-lua": { extensions: ["lua"] }, "text/x-markdown": { compressible: !0, extensions: ["mkd"] }, "text/x-nfo": { source: "apache", extensions: ["nfo"] }, "text/x-opml": { source: "apache", extensions: ["opml"] }, "text/x-org": { compressible: !0, extensions: ["org"] }, "text/x-pascal": { source: "apache", extensions: ["p", "pas"] }, "text/x-processing": { compressible: !0, extensions: ["pde"] }, "text/x-sass": { extensions: ["sass"] }, "text/x-scss": { extensions: ["scss"] }, "text/x-setext": { source: "apache", extensions: ["etx"] }, "text/x-sfv": { source: "apache", extensions: ["sfv"] }, "text/x-suse-ymp": { compressible: !0, extensions: ["ymp"] }, "text/x-uuencode": { source: "apache", extensions: ["uu"] }, "text/x-vcalendar": { source: "apache", extensions: ["vcs"] }, "text/x-vcard": { source: "apache", extensions: ["vcf"] }, "text/xml": { source: "iana", compressible: !0, extensions: ["xml"] }, "text/xml-external-parsed-entity": { source: "iana" }, "text/yaml": { compressible: !0, extensions: ["yaml", "yml"] }, "video/1d-interleaved-parityfec": { source: "iana" }, "video/3gpp": { source: "iana", extensions: ["3gp", "3gpp"] }, "video/3gpp-tt": { source: "iana" }, "video/3gpp2": { source: "iana", extensions: ["3g2"] }, "video/av1": { source: "iana" }, "video/bmpeg": { source: "iana" }, "video/bt656": { source: "iana" }, "video/celb": { source: "iana" }, "video/dv": { source: "iana" }, "video/encaprtp": { source: "iana" }, "video/ffv1": { source: "iana" }, "video/flexfec": { source: "iana" }, "video/h261": { source: "iana", extensions: ["h261"] }, "video/h263": { source: "iana", extensions: ["h263"] }, "video/h263-1998": { source: "iana" }, "video/h263-2000": { source: "iana" }, "video/h264": { source: "iana", extensions: ["h264"] }, "video/h264-rcdo": { source: "iana" }, "video/h264-svc": { source: "iana" }, "video/h265": { source: "iana" }, "video/iso.segment": { source: "iana", extensions: ["m4s"] }, "video/jpeg": { source: "iana", extensions: ["jpgv"] }, "video/jpeg2000": { source: "iana" }, "video/jpm": { source: "apache", extensions: ["jpm", "jpgm"] }, "video/jxsv": { source: "iana" }, "video/mj2": { source: "iana", extensions: ["mj2", "mjp2"] }, "video/mp1s": { source: "iana" }, "video/mp2p": { source: "iana" }, "video/mp2t": { source: "iana", extensions: ["ts"] }, "video/mp4": { source: "iana", compressible: !1, extensions: ["mp4", "mp4v", "mpg4"] }, "video/mp4v-es": { source: "iana" }, "video/mpeg": { source: "iana", compressible: !1, extensions: ["mpeg", "mpg", "mpe", "m1v", "m2v"] }, "video/mpeg4-generic": { source: "iana" }, "video/mpv": { source: "iana" }, "video/nv": { source: "iana" }, "video/ogg": { source: "iana", compressible: !1, extensions: ["ogv"] }, "video/parityfec": { source: "iana" }, "video/pointer": { source: "iana" }, "video/quicktime": { source: "iana", compressible: !1, extensions: ["qt", "mov"] }, "video/raptorfec": { source: "iana" }, "video/raw": { source: "iana" }, "video/rtp-enc-aescm128": { source: "iana" }, "video/rtploopback": { source: "iana" }, "video/rtx": { source: "iana" }, "video/scip": { source: "iana" }, "video/smpte291": { source: "iana" }, "video/smpte292m": { source: "iana" }, "video/ulpfec": { source: "iana" }, "video/vc1": { source: "iana" }, "video/vc2": { source: "iana" }, "video/vnd.cctv": { source: "iana" }, "video/vnd.dece.hd": { source: "iana", extensions: ["uvh", "uvvh"] }, "video/vnd.dece.mobile": { source: "iana", extensions: ["uvm", "uvvm"] }, "video/vnd.dece.mp4": { source: "iana" }, "video/vnd.dece.pd": { source: "iana", extensions: ["uvp", "uvvp"] }, "video/vnd.dece.sd": { source: "iana", extensions: ["uvs", "uvvs"] }, "video/vnd.dece.video": { source: "iana", extensions: ["uvv", "uvvv"] }, "video/vnd.directv.mpeg": { source: "iana" }, "video/vnd.directv.mpeg-tts": { source: "iana" }, "video/vnd.dlna.mpeg-tts": { source: "iana" }, "video/vnd.dvb.file": { source: "iana", extensions: ["dvb"] }, "video/vnd.fvt": { source: "iana", extensions: ["fvt"] }, "video/vnd.hns.video": { source: "iana" }, "video/vnd.iptvforum.1dparityfec-1010": { source: "iana" }, "video/vnd.iptvforum.1dparityfec-2005": { source: "iana" }, "video/vnd.iptvforum.2dparityfec-1010": { source: "iana" }, "video/vnd.iptvforum.2dparityfec-2005": { source: "iana" }, "video/vnd.iptvforum.ttsavc": { source: "iana" }, "video/vnd.iptvforum.ttsmpeg2": { source: "iana" }, "video/vnd.motorola.video": { source: "iana" }, "video/vnd.motorola.videop": { source: "iana" }, "video/vnd.mpegurl": { source: "iana", extensions: ["mxu", "m4u"] }, "video/vnd.ms-playready.media.pyv": { source: "iana", extensions: ["pyv"] }, "video/vnd.nokia.interleaved-multimedia": { source: "iana" }, "video/vnd.nokia.mp4vr": { source: "iana" }, "video/vnd.nokia.videovoip": { source: "iana" }, "video/vnd.objectvideo": { source: "iana" }, "video/vnd.radgamettools.bink": { source: "iana" }, "video/vnd.radgamettools.smacker": { source: "iana" }, "video/vnd.sealed.mpeg1": { source: "iana" }, "video/vnd.sealed.mpeg4": { source: "iana" }, "video/vnd.sealed.swf": { source: "iana" }, "video/vnd.sealedmedia.softseal.mov": { source: "iana" }, "video/vnd.uvvu.mp4": { source: "iana", extensions: ["uvu", "uvvu"] }, "video/vnd.vivo": { source: "iana", extensions: ["viv"] }, "video/vnd.youtube.yt": { source: "iana" }, "video/vp8": { source: "iana" }, "video/vp9": { source: "iana" }, "video/webm": { source: "apache", compressible: !1, extensions: ["webm"] }, "video/x-f4v": { source: "apache", extensions: ["f4v"] }, "video/x-fli": { source: "apache", extensions: ["fli"] }, "video/x-flv": { source: "apache", compressible: !1, extensions: ["flv"] }, "video/x-m4v": { source: "apache", extensions: ["m4v"] }, "video/x-matroska": { source: "apache", compressible: !1, extensions: ["mkv", "mk3d", "mks"] }, "video/x-mng": { source: "apache", extensions: ["mng"] }, "video/x-ms-asf": { source: "apache", extensions: ["asf", "asx"] }, "video/x-ms-vob": { source: "apache", extensions: ["vob"] }, "video/x-ms-wm": { source: "apache", extensions: ["wm"] }, "video/x-ms-wmv": { source: "apache", compressible: !1, extensions: ["wmv"] }, "video/x-ms-wmx": { source: "apache", extensions: ["wmx"] }, "video/x-ms-wvx": { source: "apache", extensions: ["wvx"] }, "video/x-msvideo": { source: "apache", extensions: ["avi"] }, "video/x-sgi-movie": { source: "apache", extensions: ["movie"] }, "video/x-smv": { source: "apache", extensions: ["smv"] }, "x-conference/x-cooltalk": { source: "apache", extensions: ["ice"] }, "x-shader/x-fragment": { compressible: !0 }, "x-shader/x-vertex": { compressible: !0 } } }); var ei = g((Ju, Zn) => { Zn.exports = Qn() }); var ni = g(K => { "use strict"; var It = ei(), uc = require("path").extname, ti = /^\s*([^;\s]*)(?:;|\s|$)/, dc = /^text\//i; K.charset = ai; K.charsets = { lookup: ai }; K.contentType = mc; K.extension = fc; K.extensions = Object.create(null); K.lookup = xc; K.types = Object.create(null); hc(K.extensions, K.types); function ai(t) { if (!t || typeof t != "string") return !1; var e = ti.exec(t), a = e && It[e[1].toLowerCase()]; return a && a.charset ? a.charset : e && dc.test(e[1]) ? "UTF-8" : !1 } function mc(t) { if (!t || typeof t != "string") return !1; var e = t.indexOf("/") === -1 ? K.lookup(t) : t; if (!e) return !1; if (e.indexOf("charset") === -1) { var a = K.charset(e); a && (e += "; charset=" + a.toLowerCase()) } return e } function fc(t) { if (!t || typeof t != "string") return !1; var e = ti.exec(t), a = e && K.extensions[e[1].toLowerCase()]; return !a || !a.length ? !1 : a[0] } function xc(t) { if (!t || typeof t != "string") return !1; var e = uc("x." + t).toLowerCase().substr(1); return e && K.types[e] || !1 } function hc(t, e) { var a = ["nginx", "apache", void 0, "iana"]; Object.keys(It).forEach(function (i) { var o = It[i], s = o.extensions; if (!(!s || !s.length)) { t[i] = s; for (var r = 0; r < s.length; r++) { var u = s[r]; if (e[u]) { var l = a.indexOf(It[e[u]].source), c = a.indexOf(o.source); if (e[u] !== "application/octet-stream" && (l > c || l === c && e[u].substr(0, 12) === "application/")) continue } e[u] = i } } }) } }); var oi = g((Yu, ii) => { ii.exports = vc; function vc(t) { var e = typeof setImmediate == "function" ? setImmediate : typeof process == "object" && typeof process.nextTick == "function" ? process.nextTick : null; e ? e(t) : setTimeout(t, 0) } }); var xa = g((Xu, ri) => { var si = oi(); ri.exports = bc; function bc(t) { var e = !1; return si(function () { e = !0 }), function (n, i) { e ? t(n, i) : si(function () { t(n, i) }) } } }); var ha = g((Qu, ci) => { ci.exports = gc; function gc(t) { Object.keys(t.jobs).forEach(yc.bind(t)), t.jobs = {} } function yc(t) { typeof this.jobs[t] == "function" && this.jobs[t]() } }); var va = g((Zu, li) => { var pi = xa(), wc = ha(); li.exports = Sc; function Sc(t, e, a, n) { var i = a.keyedList ? a.keyedList[a.index] : a.index; a.jobs[i] = Ec(e, i, t[i], function (o, s) { i in a.jobs && (delete a.jobs[i], o ? wc(a) : a.results[i] = s, n(o, a.results)) }) } function Ec(t, e, a, n) { var i; return t.length == 2 ? i = t(a, pi(n)) : i = t(a, e, pi(n)), i } }); var ba = g((ed, ui) => { ui.exports = Cc; function Cc(t, e) { var a = !Array.isArray(t), n = { index: 0, keyedList: a || e ? Object.keys(t) : null, jobs: {}, results: a ? {} : [], size: a ? Object.keys(t).length : t.length }; return e && n.keyedList.sort(a ? e : function (i, o) { return e(t[i], t[o]) }), n } }); var ga = g((td, di) => { var _c = ha(), kc = xa(); di.exports = Tc; function Tc(t) { Object.keys(this.jobs).length && (this.index = this.size, _c(this), kc(t)(null, this.results)) } }); var fi = g((ad, mi) => { var Rc = va(), Ac = ba(), Oc = ga(); mi.exports = jc; function jc(t, e, a) { for (var n = Ac(t); n.index < (n.keyedList || t).length;)Rc(t, e, n, function (i, o) { if (i) { a(i, o); return } if (Object.keys(n.jobs).length === 0) { a(null, n.results); return } }), n.index++; return Oc.bind(n, a) } }); var ya = g((nd, Dt) => { var xi = va(), Pc = ba(), Lc = ga(); Dt.exports = Ic; Dt.exports.ascending = hi; Dt.exports.descending = Dc; function Ic(t, e, a, n) { var i = Pc(t, a); return xi(t, e, i, function o(s, r) { if (s) { n(s, r); return } if (i.index++, i.index < (i.keyedList || t).length) { xi(t, e, i, o); return } n(null, i.results) }), Lc.bind(i, n) } function hi(t, e) { return t < e ? -1 : t > e ? 1 : 0 } function Dc(t, e) { return -1 * hi(t, e) } }); var bi = g((id, vi) => { var Fc = ya(); vi.exports = Nc; function Nc(t, e, a) { return Fc(t, e, null, a) } }); var yi = g((od, gi) => { gi.exports = { parallel: fi(), serial: bi(), serialOrdered: ya() } }); var wa = g((sd, wi) => { "use strict"; wi.exports = Object }); var Ei = g((rd, Si) => { "use strict"; Si.exports = Error }); var _i = g((cd, Ci) => { "use strict"; Ci.exports = EvalError }); var Ti = g((pd, ki) => { "use strict"; ki.exports = RangeError }); var Ai = g((ld, Ri) => { "use strict"; Ri.exports = ReferenceError }); var ji = g((ud, Oi) => { "use strict"; Oi.exports = SyntaxError }); var Ft = g((dd, Pi) => { "use strict"; Pi.exports = TypeError }); var Ii = g((md, Li) => { "use strict"; Li.exports = URIError }); var Fi = g((fd, Di) => { "use strict"; Di.exports = Math.abs }); var qi = g((xd, Ni) => { "use strict"; Ni.exports = Math.floor }); var Bi = g((hd, Ui) => { "use strict"; Ui.exports = Math.max }); var zi = g((vd, Mi) => { "use strict"; Mi.exports = Math.min }); var $i = g((bd, Hi) => { "use strict"; Hi.exports = Math.pow }); var Wi = g((gd, Gi) => { "use strict"; Gi.exports = Math.round }); var Ji = g((yd, Vi) => { "use strict"; Vi.exports = Number.isNaN || function (e) { return e !== e } }); var Yi = g((wd, Ki) => { "use strict"; var qc = Ji(); Ki.exports = function (e) { return qc(e) || e === 0 ? e : e < 0 ? -1 : 1 } }); var Qi = g((Sd, Xi) => { "use strict"; Xi.exports = Object.getOwnPropertyDescriptor }); var Sa = g((Ed, Zi) => { "use strict"; var Nt = Qi(); if (Nt) try { Nt([], "length") } catch { Nt = null } Zi.exports = Nt }); var to = g((Cd, eo) => { "use strict"; var qt = Object.defineProperty || !1; if (qt) try { qt({}, "a", { value: 1 }) } catch { qt = !1 } eo.exports = qt }); var Ea = g((_d, ao) => { "use strict"; ao.exports = function () { if (typeof Symbol != "function" || typeof Object.getOwnPropertySymbols != "function") return !1; if (typeof Symbol.iterator == "symbol") return !0; var e = {}, a = Symbol("test"), n = Object(a); if (typeof a == "string" || Object.prototype.toString.call(a) !== "[object Symbol]" || Object.prototype.toString.call(n) !== "[object Symbol]") return !1; var i = 42; e[a] = i; for (var o in e) return !1; if (typeof Object.keys == "function" && Object.keys(e).length !== 0 || typeof Object.getOwnPropertyNames == "function" && Object.getOwnPropertyNames(e).length !== 0) return !1; var s = Object.getOwnPropertySymbols(e); if (s.length !== 1 || s[0] !== a || !Object.prototype.propertyIsEnumerable.call(e, a)) return !1; if (typeof Object.getOwnPropertyDescriptor == "function") { var r = Object.getOwnPropertyDescriptor(e, a); if (r.value !== i || r.enumerable !== !0) return !1 } return !0 } }); var oo = g((kd, io) => { "use strict"; var no = typeof Symbol < "u" && Symbol, Uc = Ea(); io.exports = function () { return typeof no != "function" || typeof Symbol != "function" || typeof no("foo") != "symbol" || typeof Symbol("bar") != "symbol" ? !1 : Uc() } }); var Ca = g((Td, so) => { "use strict"; so.exports = typeof Reflect < "u" && Reflect.getPrototypeOf || null }); var _a = g((Rd, ro) => { "use strict"; var Bc = wa(); ro.exports = Bc.getPrototypeOf || null }); var lo = g((Ad, po) => { "use strict"; var Mc = "Function.prototype.bind called on incompatible ", zc = Object.prototype.toString, Hc = Math.max, $c = "[object Function]", co = function (e, a) { for (var n = [], i = 0; i < e.length; i += 1)n[i] = e[i]; for (var o = 0; o < a.length; o += 1)n[o + e.length] = a[o]; return n }, Gc = function (e, a) { for (var n = [], i = a || 0, o = 0; i < e.length; i += 1, o += 1)n[o] = e[i]; return n }, Wc = function (t, e) { for (var a = "", n = 0; n < t.length; n += 1)a += t[n], n + 1 < t.length && (a += e); return a }; po.exports = function (e) { var a = this; if (typeof a != "function" || zc.apply(a) !== $c) throw new TypeError(Mc + a); for (var n = Gc(arguments, 1), i, o = function () { if (this instanceof i) { var c = a.apply(this, co(n, arguments)); return Object(c) === c ? c : this } return a.apply(e, co(n, arguments)) }, s = Hc(0, a.length - n.length), r = [], u = 0; u < s; u++)r[u] = "$" + u; if (i = Function("binder", "return function (" + Wc(r, ",") + "){ return binder.apply(this,arguments); }")(o), a.prototype) { var l = function () { }; l.prototype = a.prototype, i.prototype = new l, l.prototype = null } return i } }); var ut = g((Od, uo) => { "use strict"; var Vc = lo(); uo.exports = Function.prototype.bind || Vc }); var Ut = g((jd, mo) => { "use strict"; mo.exports = Function.prototype.call }); var ka = g((Pd, fo) => { "use strict"; fo.exports = Function.prototype.apply }); var ho = g((Ld, xo) => { "use strict"; xo.exports = typeof Reflect < "u" && Reflect && Reflect.apply }); var bo = g((Id, vo) => { "use strict"; var Jc = ut(), Kc = ka(), Yc = Ut(), Xc = ho(); vo.exports = Xc || Jc.call(Yc, Kc) }); var yo = g((Dd, go) => { "use strict"; var Qc = ut(), Zc = Ft(), ep = Ut(), tp = bo(); go.exports = function (e) { if (e.length < 1 || typeof e[0] != "function") throw new Zc("a function is required"); return tp(Qc, ep, e) } }); var ko = g((Fd, _o) => { "use strict"; var ap = yo(), wo = Sa(), Eo; try { Eo = [].__proto__ === Array.prototype } catch (t) { if (!t || typeof t != "object" || !("code" in t) || t.code !== "ERR_PROTO_ACCESS") throw t } var Ta = !!Eo && wo && wo(Object.prototype, "__proto__"), Co = Object, So = Co.getPrototypeOf; _o.exports = Ta && typeof Ta.get == "function" ? ap([Ta.get]) : typeof So == "function" ? function (e) { return So(e == null ? e : Co(e)) } : !1 }); var jo = g((Nd, Oo) => { "use strict"; var To = Ca(), Ro = _a(), Ao = ko(); Oo.exports = To ? function (e) { return To(e) } : Ro ? function (e) { if (!e || typeof e != "object" && typeof e != "function") throw new TypeError("getProto: not an object"); return Ro(e) } : Ao ? function (e) { return Ao(e) } : null }); var Bt = g((qd, Po) => { "use strict"; var np = Function.prototype.call, ip = Object.prototype.hasOwnProperty, op = ut(); Po.exports = op.call(np, ip) }); var Uo = g((Ud, qo) => { "use strict"; var E, sp = wa(), rp = Ei(), cp = _i(), pp = Ti(), lp = Ai(), $e = ji(), He = Ft(), up = Ii(), dp = Fi(), mp = qi(), fp = Bi(), xp = zi(), hp = $i(), vp = Wi(), bp = Yi(), Fo = Function, Ra = function (t) { try { return Fo('"use strict"; return (' + t + ").constructor;")() } catch { } }, dt = Sa(), gp = to(), Aa = function () { throw new He }, yp = dt ? (function () { try { return arguments.callee, Aa } catch { try { return dt(arguments, "callee").get } catch { return Aa } } })() : Aa, Me = oo()(), U = jo(), wp = _a(), Sp = Ca(), No = ka(), mt = Ut(), ze = {}, Ep = typeof Uint8Array > "u" || !U ? E : U(Uint8Array), Ce = { __proto__: null, "%AggregateError%": typeof AggregateError > "u" ? E : AggregateError, "%Array%": Array, "%ArrayBuffer%": typeof ArrayBuffer > "u" ? E : ArrayBuffer, "%ArrayIteratorPrototype%": Me && U ? U([][Symbol.iterator]()) : E, "%AsyncFromSyncIteratorPrototype%": E, "%AsyncFunction%": ze, "%AsyncGenerator%": ze, "%AsyncGeneratorFunction%": ze, "%AsyncIteratorPrototype%": ze, "%Atomics%": typeof Atomics > "u" ? E : Atomics, "%BigInt%": typeof BigInt > "u" ? E : BigInt, "%BigInt64Array%": typeof BigInt64Array > "u" ? E : BigInt64Array, "%BigUint64Array%": typeof BigUint64Array > "u" ? E : BigUint64Array, "%Boolean%": Boolean, "%DataView%": typeof DataView > "u" ? E : DataView, "%Date%": Date, "%decodeURI%": decodeURI, "%decodeURIComponent%": decodeURIComponent, "%encodeURI%": encodeURI, "%encodeURIComponent%": encodeURIComponent, "%Error%": rp, "%eval%": eval, "%EvalError%": cp, "%Float16Array%": typeof Float16Array > "u" ? E : Float16Array, "%Float32Array%": typeof Float32Array > "u" ? E : Float32Array, "%Float64Array%": typeof Float64Array > "u" ? E : Float64Array, "%FinalizationRegistry%": typeof FinalizationRegistry > "u" ? E : FinalizationRegistry, "%Function%": Fo, "%GeneratorFunction%": ze, "%Int8Array%": typeof Int8Array > "u" ? E : Int8Array, "%Int16Array%": typeof Int16Array > "u" ? E : Int16Array, "%Int32Array%": typeof Int32Array > "u" ? E : Int32Array, "%isFinite%": isFinite, "%isNaN%": isNaN, "%IteratorPrototype%": Me && U ? U(U([][Symbol.iterator]())) : E, "%JSON%": typeof JSON == "object" ? JSON : E, "%Map%": typeof Map > "u" ? E : Map, "%MapIteratorPrototype%": typeof Map > "u" || !Me || !U ? E : U(new Map()[Symbol.iterator]()), "%Math%": Math, "%Number%": Number, "%Object%": sp, "%Object.getOwnPropertyDescriptor%": dt, "%parseFloat%": parseFloat, "%parseInt%": parseInt, "%Promise%": typeof Promise > "u" ? E : Promise, "%Proxy%": typeof Proxy > "u" ? E : Proxy, "%RangeError%": pp, "%ReferenceError%": lp, "%Reflect%": typeof Reflect > "u" ? E : Reflect, "%RegExp%": RegExp, "%Set%": typeof Set > "u" ? E : Set, "%SetIteratorPrototype%": typeof Set > "u" || !Me || !U ? E : U(new Set()[Symbol.iterator]()), "%SharedArrayBuffer%": typeof SharedArrayBuffer > "u" ? E : SharedArrayBuffer, "%String%": String, "%StringIteratorPrototype%": Me && U ? U(""[Symbol.iterator]()) : E, "%Symbol%": Me ? Symbol : E, "%SyntaxError%": $e, "%ThrowTypeError%": yp, "%TypedArray%": Ep, "%TypeError%": He, "%Uint8Array%": typeof Uint8Array > "u" ? E : Uint8Array, "%Uint8ClampedArray%": typeof Uint8ClampedArray > "u" ? E : Uint8ClampedArray, "%Uint16Array%": typeof Uint16Array > "u" ? E : Uint16Array, "%Uint32Array%": typeof Uint32Array > "u" ? E : Uint32Array, "%URIError%": up, "%WeakMap%": typeof WeakMap > "u" ? E : WeakMap, "%WeakRef%": typeof WeakRef > "u" ? E : WeakRef, "%WeakSet%": typeof WeakSet > "u" ? E : WeakSet, "%Function.prototype.call%": mt, "%Function.prototype.apply%": No, "%Object.defineProperty%": gp, "%Object.getPrototypeOf%": wp, "%Math.abs%": dp, "%Math.floor%": mp, "%Math.max%": fp, "%Math.min%": xp, "%Math.pow%": hp, "%Math.round%": vp, "%Math.sign%": bp, "%Reflect.getPrototypeOf%": Sp }; if (U) try { null.error } catch (t) { Lo = U(U(t)), Ce["%Error.prototype%"] = Lo } var Lo, Cp = function t(e) { var a; if (e === "%AsyncFunction%") a = Ra("async function () {}"); else if (e === "%GeneratorFunction%") a = Ra("function* () {}"); else if (e === "%AsyncGeneratorFunction%") a = Ra("async function* () {}"); else if (e === "%AsyncGenerator%") { var n = t("%AsyncGeneratorFunction%"); n && (a = n.prototype) } else if (e === "%AsyncIteratorPrototype%") { var i = t("%AsyncGenerator%"); i && U && (a = U(i.prototype)) } return Ce[e] = a, a }, Io = { __proto__: null, "%ArrayBufferPrototype%": ["ArrayBuffer", "prototype"], "%ArrayPrototype%": ["Array", "prototype"], "%ArrayProto_entries%": ["Array", "prototype", "entries"], "%ArrayProto_forEach%": ["Array", "prototype", "forEach"], "%ArrayProto_keys%": ["Array", "prototype", "keys"], "%ArrayProto_values%": ["Array", "prototype", "values"], "%AsyncFunctionPrototype%": ["AsyncFunction", "prototype"], "%AsyncGenerator%": ["AsyncGeneratorFunction", "prototype"], "%AsyncGeneratorPrototype%": ["AsyncGeneratorFunction", "prototype", "prototype"], "%BooleanPrototype%": ["Boolean", "prototype"], "%DataViewPrototype%": ["DataView", "prototype"], "%DatePrototype%": ["Date", "prototype"], "%ErrorPrototype%": ["Error", "prototype"], "%EvalErrorPrototype%": ["EvalError", "prototype"], "%Float32ArrayPrototype%": ["Float32Array", "prototype"], "%Float64ArrayPrototype%": ["Float64Array", "prototype"], "%FunctionPrototype%": ["Function", "prototype"], "%Generator%": ["GeneratorFunction", "prototype"], "%GeneratorPrototype%": ["GeneratorFunction", "prototype", "prototype"], "%Int8ArrayPrototype%": ["Int8Array", "prototype"], "%Int16ArrayPrototype%": ["Int16Array", "prototype"], "%Int32ArrayPrototype%": ["Int32Array", "prototype"], "%JSONParse%": ["JSON", "parse"], "%JSONStringify%": ["JSON", "stringify"], "%MapPrototype%": ["Map", "prototype"], "%NumberPrototype%": ["Number", "prototype"], "%ObjectPrototype%": ["Object", "prototype"], "%ObjProto_toString%": ["Object", "prototype", "toString"], "%ObjProto_valueOf%": ["Object", "prototype", "valueOf"], "%PromisePrototype%": ["Promise", "prototype"], "%PromiseProto_then%": ["Promise", "prototype", "then"], "%Promise_all%": ["Promise", "all"], "%Promise_reject%": ["Promise", "reject"], "%Promise_resolve%": ["Promise", "resolve"], "%RangeErrorPrototype%": ["RangeError", "prototype"], "%ReferenceErrorPrototype%": ["ReferenceError", "prototype"], "%RegExpPrototype%": ["RegExp", "prototype"], "%SetPrototype%": ["Set", "prototype"], "%SharedArrayBufferPrototype%": ["SharedArrayBuffer", "prototype"], "%StringPrototype%": ["String", "prototype"], "%SymbolPrototype%": ["Symbol", "prototype"], "%SyntaxErrorPrototype%": ["SyntaxError", "prototype"], "%TypedArrayPrototype%": ["TypedArray", "prototype"], "%TypeErrorPrototype%": ["TypeError", "prototype"], "%Uint8ArrayPrototype%": ["Uint8Array", "prototype"], "%Uint8ClampedArrayPrototype%": ["Uint8ClampedArray", "prototype"], "%Uint16ArrayPrototype%": ["Uint16Array", "prototype"], "%Uint32ArrayPrototype%": ["Uint32Array", "prototype"], "%URIErrorPrototype%": ["URIError", "prototype"], "%WeakMapPrototype%": ["WeakMap", "prototype"], "%WeakSetPrototype%": ["WeakSet", "prototype"] }, ft = ut(), Mt = Bt(), _p = ft.call(mt, Array.prototype.concat), kp = ft.call(No, Array.prototype.splice), Do = ft.call(mt, String.prototype.replace), zt = ft.call(mt, String.prototype.slice), Tp = ft.call(mt, RegExp.prototype.exec), Rp = /[^%.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|%$))/g, Ap = /\\(\\)?/g, Op = function (e) { var a = zt(e, 0, 1), n = zt(e, -1); if (a === "%" && n !== "%") throw new $e("invalid intrinsic syntax, expected closing `%`"); if (n === "%" && a !== "%") throw new $e("invalid intrinsic syntax, expected opening `%`"); var i = []; return Do(e, Rp, function (o, s, r, u) { i[i.length] = r ? Do(u, Ap, "$1") : s || o }), i }, jp = function (e, a) { var n = e, i; if (Mt(Io, n) && (i = Io[n], n = "%" + i[0] + "%"), Mt(Ce, n)) { var o = Ce[n]; if (o === ze && (o = Cp(n)), typeof o > "u" && !a) throw new He("intrinsic " + e + " exists, but is not available. Please file an issue!"); return { alias: i, name: n, value: o } } throw new $e("intrinsic " + e + " does not exist!") }; qo.exports = function (e, a) { if (typeof e != "string" || e.length === 0) throw new He("intrinsic name must be a non-empty string"); if (arguments.length > 1 && typeof a != "boolean") throw new He('"allowMissing" argument must be a boolean'); if (Tp(/^%?[^%]*%?$/, e) === null) throw new $e("`%` may not be present anywhere but at the beginning and end of the intrinsic name"); var n = Op(e), i = n.length > 0 ? n[0] : "", o = jp("%" + i + "%", a), s = o.name, r = o.value, u = !1, l = o.alias; l && (i = l[0], kp(n, _p([0, 1], l))); for (var c = 1, d = !0; c < n.length; c += 1) { var m = n[c], v = zt(m, 0, 1), f = zt(m, -1); if ((v === '"' || v === "'" || v === "`" || f === '"' || f === "'" || f === "`") && v !== f) throw new $e("property names with quotes must have matching quotes"); if ((m === "constructor" || !d) && (u = !0), i += "." + m, s = "%" + i + "%", Mt(Ce, s)) r = Ce[s]; else if (r != null) { if (!(m in r)) { if (!a) throw new He("base intrinsic for " + e + " exists, but the property is not available."); return } if (dt && c + 1 >= n.length) { var h = dt(r, m); d = !!h, d && "get" in h && !("originalValue" in h.get) ? r = h.get : r = r[m] } else d = Mt(r, m), r = r[m]; d && !u && (Ce[s] = r) } } return r } }); var Mo = g((Bd, Bo) => { "use strict"; var Pp = Ea(); Bo.exports = function () { return Pp() && !!Symbol.toStringTag } }); var $o = g((Md, Ho) => { "use strict"; var Lp = Uo(), zo = Lp("%Object.defineProperty%", !0), Ip = Mo()(), Dp = Bt(), Fp = Ft(), Ht = Ip ? Symbol.toStringTag : null; Ho.exports = function (e, a) { var n = arguments.length > 2 && !!arguments[2] && arguments[2].force, i = arguments.length > 2 && !!arguments[2] && arguments[2].nonConfigurable; if (typeof n < "u" && typeof n != "boolean" || typeof i < "u" && typeof i != "boolean") throw new Fp("if provided, the `overrideIfSet` and `nonConfigurable` options must be booleans"); Ht && (n || !Dp(e, Ht)) && (zo ? zo(e, Ht, { configurable: !i, enumerable: !1, value: a, writable: !1 }) : e[Ht] = a) } }); var Wo = g((zd, Go) => { "use strict"; Go.exports = function (t, e) { return Object.keys(e).forEach(function (a) { t[a] = t[a] || e[a] }), t } }); var Jo = g((Hd, Vo) => {
	"use strict"; var La = Xn(), Np = require("util"), Oa = require("path"), qp = require("http"), Up = require("https"), Bp = require("url").parse, Mp = require("fs"), zp = require("stream").Stream, Hp = require("crypto"), ja = ni(), $p = yi(), Gp = $o(), ge = Bt(), Pa = Wo(); function _(t) { if (!(this instanceof _)) return new _(t); this._overheadLength = 0, this._valueLength = 0, this._valuesToMeasure = [], La.call(this), t = t || {}; for (var e in t) this[e] = t[e] } Np.inherits(_, La); _.LINE_BREAK = `\r
`; _.DEFAULT_CONTENT_TYPE = "application/octet-stream"; _.prototype.append = function (t, e, a) { a = a || {}, typeof a == "string" && (a = { filename: a }); var n = La.prototype.append.bind(this); if ((typeof e == "number" || e == null) && (e = String(e)), Array.isArray(e)) { this._error(new Error("Arrays are not supported.")); return } var i = this._multiPartHeader(t, e, a), o = this._multiPartFooter(); n(i), n(e), n(o), this._trackLength(i, e, a) }; _.prototype._trackLength = function (t, e, a) { var n = 0; a.knownLength != null ? n += Number(a.knownLength) : Buffer.isBuffer(e) ? n = e.length : typeof e == "string" && (n = Buffer.byteLength(e)), this._valueLength += n, this._overheadLength += Buffer.byteLength(t) + _.LINE_BREAK.length, !(!e || !e.path && !(e.readable && ge(e, "httpVersion")) && !(e instanceof zp)) && (a.knownLength || this._valuesToMeasure.push(e)) }; _.prototype._lengthRetriever = function (t, e) { ge(t, "fd") ? t.end != null && t.end != 1 / 0 && t.start != null ? e(null, t.end + 1 - (t.start ? t.start : 0)) : Mp.stat(t.path, function (a, n) { if (a) { e(a); return } var i = n.size - (t.start ? t.start : 0); e(null, i) }) : ge(t, "httpVersion") ? e(null, Number(t.headers["content-length"])) : ge(t, "httpModule") ? (t.on("response", function (a) { t.pause(), e(null, Number(a.headers["content-length"])) }), t.resume()) : e("Unknown stream") }; _.prototype._multiPartHeader = function (t, e, a) { if (typeof a.header == "string") return a.header; var n = this._getContentDisposition(e, a), i = this._getContentType(e, a), o = "", s = { "Content-Disposition": ["form-data", 'name="' + t + '"'].concat(n || []), "Content-Type": [].concat(i || []) }; typeof a.header == "object" && Pa(s, a.header); var r; for (var u in s) if (ge(s, u)) { if (r = s[u], r == null) continue; Array.isArray(r) || (r = [r]), r.length && (o += u + ": " + r.join("; ") + _.LINE_BREAK) } return "--" + this.getBoundary() + _.LINE_BREAK + o + _.LINE_BREAK }; _.prototype._getContentDisposition = function (t, e) { var a; if (typeof e.filepath == "string" ? a = Oa.normalize(e.filepath).replace(/\\/g, "/") : e.filename || t && (t.name || t.path) ? a = Oa.basename(e.filename || t && (t.name || t.path)) : t && t.readable && ge(t, "httpVersion") && (a = Oa.basename(t.client._httpMessage.path || "")), a) return 'filename="' + a + '"' }; _.prototype._getContentType = function (t, e) { var a = e.contentType; return !a && t && t.name && (a = ja.lookup(t.name)), !a && t && t.path && (a = ja.lookup(t.path)), !a && t && t.readable && ge(t, "httpVersion") && (a = t.headers["content-type"]), !a && (e.filepath || e.filename) && (a = ja.lookup(e.filepath || e.filename)), !a && t && typeof t == "object" && (a = _.DEFAULT_CONTENT_TYPE), a }; _.prototype._multiPartFooter = function () { return function (t) { var e = _.LINE_BREAK, a = this._streams.length === 0; a && (e += this._lastBoundary()), t(e) }.bind(this) }; _.prototype._lastBoundary = function () { return "--" + this.getBoundary() + "--" + _.LINE_BREAK }; _.prototype.getHeaders = function (t) { var e, a = { "content-type": "multipart/form-data; boundary=" + this.getBoundary() }; for (e in t) ge(t, e) && (a[e.toLowerCase()] = t[e]); return a }; _.prototype.setBoundary = function (t) { if (typeof t != "string") throw new TypeError("FormData boundary must be a string"); this._boundary = t }; _.prototype.getBoundary = function () { return this._boundary || this._generateBoundary(), this._boundary }; _.prototype.getBuffer = function () { for (var t = new Buffer.alloc(0), e = this.getBoundary(), a = 0, n = this._streams.length; a < n; a++)typeof this._streams[a] != "function" && (Buffer.isBuffer(this._streams[a]) ? t = Buffer.concat([t, this._streams[a]]) : t = Buffer.concat([t, Buffer.from(this._streams[a])]), (typeof this._streams[a] != "string" || this._streams[a].substring(2, e.length + 2) !== e) && (t = Buffer.concat([t, Buffer.from(_.LINE_BREAK)]))); return Buffer.concat([t, Buffer.from(this._lastBoundary())]) }; _.prototype._generateBoundary = function () { this._boundary = "--------------------------" + Hp.randomBytes(12).toString("hex") }; _.prototype.getLengthSync = function () { var t = this._overheadLength + this._valueLength; return this._streams.length && (t += this._lastBoundary().length), this.hasKnownLength() || this._error(new Error("Cannot calculate proper length in synchronous way.")), t }; _.prototype.hasKnownLength = function () { var t = !0; return this._valuesToMeasure.length && (t = !1), t }; _.prototype.getLength = function (t) { var e = this._overheadLength + this._valueLength; if (this._streams.length && (e += this._lastBoundary().length), !this._valuesToMeasure.length) { process.nextTick(t.bind(this, null, e)); return } $p.parallel(this._valuesToMeasure, this._lengthRetriever, function (a, n) { if (a) { t(a); return } n.forEach(function (i) { e += i }), t(null, e) }) }; _.prototype.submit = function (t, e) { var a, n, i = { method: "post" }; return typeof t == "string" ? (t = Bp(t), n = Pa({ port: t.port, path: t.pathname, host: t.hostname, protocol: t.protocol }, i)) : (n = Pa(t, i), n.port || (n.port = n.protocol === "https:" ? 443 : 80)), n.headers = this.getHeaders(t.headers), n.protocol === "https:" ? a = Up.request(n) : a = qp.request(n), this.getLength(function (o, s) { if (o && o !== "Unknown stream") { this._error(o); return } if (s && a.setHeader("Content-Length", s), this.pipe(a), e) { var r, u = function (l, c) { return a.removeListener("error", u), a.removeListener("response", r), e.call(this, l, c) }; r = u.bind(this, null), a.on("error", u), a.on("response", r) } }.bind(this)), a }; _.prototype._error = function (t) { this.error || (this.error = t, this.pause(), this.emit("error", t)) }; _.prototype.toString = function () { return "[object FormData]" }; Gp(_.prototype, "FormData"); Vo.exports = _
}); var us = g(ls => { "use strict"; var pl = require("url").parse, ll = { ftp: 21, gopher: 70, http: 80, https: 443, ws: 80, wss: 443 }, ul = String.prototype.endsWith || function (t) { return t.length <= this.length && this.indexOf(t, this.length - t.length) !== -1 }; function dl(t) { var e = typeof t == "string" ? pl(t) : t || {}, a = e.protocol, n = e.host, i = e.port; if (typeof n != "string" || !n || typeof a != "string" || (a = a.split(":", 1)[0], n = n.replace(/:\d*$/, ""), i = parseInt(i) || ll[a] || 0, !ml(n, i))) return ""; var o = Je("npm_config_" + a + "_proxy") || Je(a + "_proxy") || Je("npm_config_proxy") || Je("all_proxy"); return o && o.indexOf("://") === -1 && (o = a + "://" + o), o } function ml(t, e) { var a = (Je("npm_config_no_proxy") || Je("no_proxy")).toLowerCase(); return a ? a === "*" ? !1 : a.split(/[,\s]/).every(function (n) { if (!n) return !0; var i = n.match(/^(.+):(\d+)$/), o = i ? i[1] : n, s = i ? parseInt(i[2]) : 0; return s && s !== e ? !0 : /^[.*]/.test(o) ? (o.charAt(0) === "*" && (o = o.slice(1)), !ul.call(t, o)) : t !== o }) : !0 } function Je(t) { return process.env[t.toLowerCase()] || process.env[t.toUpperCase()] || "" } ls.getProxyForUrl = dl }); var ms = g((Hm, ds) => { var Ke = 1e3, Ye = Ke * 60, Xe = Ye * 60, Te = Xe * 24, fl = Te * 7, xl = Te * 365.25; ds.exports = function (t, e) { e = e || {}; var a = typeof t; if (a === "string" && t.length > 0) return hl(t); if (a === "number" && isFinite(t)) return e.long ? bl(t) : vl(t); throw new Error("val is not a non-empty string or a valid number. val=" + JSON.stringify(t)) }; function hl(t) { if (t = String(t), !(t.length > 100)) { var e = /^(-?(?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i.exec(t); if (e) { var a = parseFloat(e[1]), n = (e[2] || "ms").toLowerCase(); switch (n) { case "years": case "year": case "yrs": case "yr": case "y": return a * xl; case "weeks": case "week": case "w": return a * fl; case "days": case "day": case "d": return a * Te; case "hours": case "hour": case "hrs": case "hr": case "h": return a * Xe; case "minutes": case "minute": case "mins": case "min": case "m": return a * Ye; case "seconds": case "second": case "secs": case "sec": case "s": return a * Ke; case "milliseconds": case "millisecond": case "msecs": case "msec": case "ms": return a; default: return } } } } function vl(t) { var e = Math.abs(t); return e >= Te ? Math.round(t / Te) + "d" : e >= Xe ? Math.round(t / Xe) + "h" : e >= Ye ? Math.round(t / Ye) + "m" : e >= Ke ? Math.round(t / Ke) + "s" : t + "ms" } function bl(t) { var e = Math.abs(t); return e >= Te ? Vt(t, e, Te, "day") : e >= Xe ? Vt(t, e, Xe, "hour") : e >= Ye ? Vt(t, e, Ye, "minute") : e >= Ke ? Vt(t, e, Ke, "second") : t + " ms" } function Vt(t, e, a, n) { var i = e >= a * 1.5; return Math.round(t / a) + " " + n + (i ? "s" : "") } }); var Va = g(($m, fs) => { function gl(t) { a.debug = a, a.default = a, a.coerce = u, a.disable = s, a.enable = i, a.enabled = r, a.humanize = ms(), a.destroy = l, Object.keys(t).forEach(c => { a[c] = t[c] }), a.names = [], a.skips = [], a.formatters = {}; function e(c) { let d = 0; for (let m = 0; m < c.length; m++)d = (d << 5) - d + c.charCodeAt(m), d |= 0; return a.colors[Math.abs(d) % a.colors.length] } a.selectColor = e; function a(c) { let d, m = null, v, f; function h(...x) { if (!h.enabled) return; let S = h, k = Number(new Date), C = k - (d || k); S.diff = C, S.prev = d, S.curr = k, d = k, x[0] = a.coerce(x[0]), typeof x[0] != "string" && x.unshift("%O"); let O = 0; x[0] = x[0].replace(/%([a-zA-Z%])/g, (j, F) => { if (j === "%%") return "%"; O++; let N = a.formatters[F]; if (typeof N == "function") { let re = x[O]; j = N.call(S, re), x.splice(O, 1), O-- } return j }), a.formatArgs.call(S, x), (S.log || a.log).apply(S, x) } return h.namespace = c, h.useColors = a.useColors(), h.color = a.selectColor(c), h.extend = n, h.destroy = a.destroy, Object.defineProperty(h, "enabled", { enumerable: !0, configurable: !1, get: () => m !== null ? m : (v !== a.namespaces && (v = a.namespaces, f = a.enabled(c)), f), set: x => { m = x } }), typeof a.init == "function" && a.init(h), h } function n(c, d) { let m = a(this.namespace + (typeof d > "u" ? ":" : d) + c); return m.log = this.log, m } function i(c) { a.save(c), a.namespaces = c, a.names = [], a.skips = []; let d = (typeof c == "string" ? c : "").trim().replace(/\s+/g, ",").split(",").filter(Boolean); for (let m of d) m[0] === "-" ? a.skips.push(m.slice(1)) : a.names.push(m) } function o(c, d) { let m = 0, v = 0, f = -1, h = 0; for (; m < c.length;)if (v < d.length && (d[v] === c[m] || d[v] === "*")) d[v] === "*" ? (f = v, h = m, v++) : (m++, v++); else if (f !== -1) v = f + 1, h++, m = h; else return !1; for (; v < d.length && d[v] === "*";)v++; return v === d.length } function s() { let c = [...a.names, ...a.skips.map(d => "-" + d)].join(","); return a.enable(""), c } function r(c) { for (let d of a.skips) if (o(c, d)) return !1; for (let d of a.names) if (o(c, d)) return !0; return !1 } function u(c) { return c instanceof Error ? c.stack || c.message : c } function l() { console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.") } return a.enable(a.load()), a } fs.exports = gl }); var xs = g((Y, Jt) => { Y.formatArgs = wl; Y.save = Sl; Y.load = El; Y.useColors = yl; Y.storage = Cl(); Y.destroy = (() => { let t = !1; return () => { t || (t = !0, console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.")) } })(); Y.colors = ["#0000CC", "#0000FF", "#0033CC", "#0033FF", "#0066CC", "#0066FF", "#0099CC", "#0099FF", "#00CC00", "#00CC33", "#00CC66", "#00CC99", "#00CCCC", "#00CCFF", "#3300CC", "#3300FF", "#3333CC", "#3333FF", "#3366CC", "#3366FF", "#3399CC", "#3399FF", "#33CC00", "#33CC33", "#33CC66", "#33CC99", "#33CCCC", "#33CCFF", "#6600CC", "#6600FF", "#6633CC", "#6633FF", "#66CC00", "#66CC33", "#9900CC", "#9900FF", "#9933CC", "#9933FF", "#99CC00", "#99CC33", "#CC0000", "#CC0033", "#CC0066", "#CC0099", "#CC00CC", "#CC00FF", "#CC3300", "#CC3333", "#CC3366", "#CC3399", "#CC33CC", "#CC33FF", "#CC6600", "#CC6633", "#CC9900", "#CC9933", "#CCCC00", "#CCCC33", "#FF0000", "#FF0033", "#FF0066", "#FF0099", "#FF00CC", "#FF00FF", "#FF3300", "#FF3333", "#FF3366", "#FF3399", "#FF33CC", "#FF33FF", "#FF6600", "#FF6633", "#FF9900", "#FF9933", "#FFCC00", "#FFCC33"]; function yl() { if (typeof window < "u" && window.process && (window.process.type === "renderer" || window.process.__nwjs)) return !0; if (typeof navigator < "u" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/(edge|trident)\/(\d+)/)) return !1; let t; return typeof document < "u" && document.documentElement && document.documentElement.style && document.documentElement.style.WebkitAppearance || typeof window < "u" && window.console && (window.console.firebug || window.console.exception && window.console.table) || typeof navigator < "u" && navigator.userAgent && (t = navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/)) && parseInt(t[1], 10) >= 31 || typeof navigator < "u" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/applewebkit\/(\d+)/) } function wl(t) { if (t[0] = (this.useColors ? "%c" : "") + this.namespace + (this.useColors ? " %c" : " ") + t[0] + (this.useColors ? "%c " : " ") + "+" + Jt.exports.humanize(this.diff), !this.useColors) return; let e = "color: " + this.color; t.splice(1, 0, e, "color: inherit"); let a = 0, n = 0; t[0].replace(/%[a-zA-Z%]/g, i => { i !== "%%" && (a++, i === "%c" && (n = a)) }), t.splice(n, 0, e) } Y.log = console.debug || console.log || (() => { }); function Sl(t) { try { t ? Y.storage.setItem("debug", t) : Y.storage.removeItem("debug") } catch { } } function El() { let t; try { t = Y.storage.getItem("debug") || Y.storage.getItem("DEBUG") } catch { } return !t && typeof process < "u" && "env" in process && (t = process.env.DEBUG), t } function Cl() { try { return localStorage } catch { } } Jt.exports = Va()(Y); var { formatters: _l } = Jt.exports; _l.j = function (t) { try { return JSON.stringify(t) } catch (e) { return "[UnexpectedJSONParseError]: " + e.message } } }); var vs = g((Gm, hs) => { "use strict"; hs.exports = (t, e = process.argv) => { let a = t.startsWith("-") ? "" : t.length === 1 ? "-" : "--", n = e.indexOf(a + t), i = e.indexOf("--"); return n !== -1 && (i === -1 || n < i) } }); var ys = g((Wm, gs) => { "use strict"; var kl = require("os"), bs = require("tty"), ae = vs(), { env: B } = process, Kt; ae("no-color") || ae("no-colors") || ae("color=false") || ae("color=never") ? Kt = 0 : (ae("color") || ae("colors") || ae("color=true") || ae("color=always")) && (Kt = 1); function Tl() { if ("FORCE_COLOR" in B) return B.FORCE_COLOR === "true" ? 1 : B.FORCE_COLOR === "false" ? 0 : B.FORCE_COLOR.length === 0 ? 1 : Math.min(Number.parseInt(B.FORCE_COLOR, 10), 3) } function Rl(t) { return t === 0 ? !1 : { level: t, hasBasic: !0, has256: t >= 2, has16m: t >= 3 } } function Al(t, { streamIsTTY: e, sniffFlags: a = !0 } = {}) { let n = Tl(); n !== void 0 && (Kt = n); let i = a ? Kt : n; if (i === 0) return 0; if (a) { if (ae("color=16m") || ae("color=full") || ae("color=truecolor")) return 3; if (ae("color=256")) return 2 } if (t && !e && i === void 0) return 0; let o = i || 0; if (B.TERM === "dumb") return o; if (process.platform === "win32") { let s = kl.release().split("."); return Number(s[0]) >= 10 && Number(s[2]) >= 10586 ? Number(s[2]) >= 14931 ? 3 : 2 : 1 } if ("CI" in B) return ["TRAVIS", "CIRCLECI", "APPVEYOR", "GITLAB_CI", "GITHUB_ACTIONS", "BUILDKITE", "DRONE"].some(s => s in B) || B.CI_NAME === "codeship" ? 1 : o; if ("TEAMCITY_VERSION" in B) return /^(9\.(0*[1-9]\d*)\.|\d{2,}\.)/.test(B.TEAMCITY_VERSION) ? 1 : 0; if (B.COLORTERM === "truecolor") return 3; if ("TERM_PROGRAM" in B) { let s = Number.parseInt((B.TERM_PROGRAM_VERSION || "").split(".")[0], 10); switch (B.TERM_PROGRAM) { case "iTerm.app": return s >= 3 ? 3 : 2; case "Apple_Terminal": return 2 } } return /-256(color)?$/i.test(B.TERM) ? 2 : /^screen|^xterm|^vt100|^vt220|^rxvt|color|ansi|cygwin|linux/i.test(B.TERM) || "COLORTERM" in B ? 1 : o } function Ja(t, e = {}) { let a = Al(t, { streamIsTTY: t && t.isTTY, ...e }); return Rl(a) } gs.exports = { supportsColor: Ja, stdout: Ja({ isTTY: bs.isatty(1) }), stderr: Ja({ isTTY: bs.isatty(2) }) } }); var Ss = g((M, Xt) => {
	var Ol = require("tty"), Yt = require("util"); M.init = Nl; M.log = Il; M.formatArgs = Pl; M.save = Dl; M.load = Fl; M.useColors = jl; M.destroy = Yt.deprecate(() => { }, "Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`."); M.colors = [6, 2, 3, 4, 5, 1]; try { let t = ys(); t && (t.stderr || t).level >= 2 && (M.colors = [20, 21, 26, 27, 32, 33, 38, 39, 40, 41, 42, 43, 44, 45, 56, 57, 62, 63, 68, 69, 74, 75, 76, 77, 78, 79, 80, 81, 92, 93, 98, 99, 112, 113, 128, 129, 134, 135, 148, 149, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 178, 179, 184, 185, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 214, 215, 220, 221]) } catch { } M.inspectOpts = Object.keys(process.env).filter(t => /^debug_/i.test(t)).reduce((t, e) => { let a = e.substring(6).toLowerCase().replace(/_([a-z])/g, (i, o) => o.toUpperCase()), n = process.env[e]; return /^(yes|on|true|enabled)$/i.test(n) ? n = !0 : /^(no|off|false|disabled)$/i.test(n) ? n = !1 : n === "null" ? n = null : n = Number(n), t[a] = n, t }, {}); function jl() { return "colors" in M.inspectOpts ? !!M.inspectOpts.colors : Ol.isatty(process.stderr.fd) } function Pl(t) {
		let { namespace: e, useColors: a } = this; if (a) {
			let n = this.color, i = "\x1B[3" + (n < 8 ? n : "8;5;" + n), o = `  ${i};1m${e} \x1B[0m`; t[0] = o + t[0].split(`
`).join(`
`+ o), t.push(i + "m+" + Xt.exports.humanize(this.diff) + "\x1B[0m")
		} else t[0] = Ll() + e + " " + t[0]
	} function Ll() { return M.inspectOpts.hideDate ? "" : new Date().toISOString() + " " } function Il(...t) {
		return process.stderr.write(Yt.formatWithOptions(M.inspectOpts, ...t) + `
`)
	} function Dl(t) { t ? process.env.DEBUG = t : delete process.env.DEBUG } function Fl() { return process.env.DEBUG } function Nl(t) { t.inspectOpts = {}; let e = Object.keys(M.inspectOpts); for (let a = 0; a < e.length; a++)t.inspectOpts[e[a]] = M.inspectOpts[e[a]] } Xt.exports = Va()(M); var { formatters: ws } = Xt.exports; ws.o = function (t) {
		return this.inspectOpts.colors = this.useColors, Yt.inspect(t, this.inspectOpts).split(`
`).map(e => e.trim()).join(" ")
	}; ws.O = function (t) { return this.inspectOpts.colors = this.useColors, Yt.inspect(t, this.inspectOpts) }
}); var Es = g((Vm, Ka) => { typeof process > "u" || process.type === "renderer" || process.browser === !0 || process.__nwjs ? Ka.exports = xs() : Ka.exports = Ss() }); var _s = g((Jm, Cs) => { var bt; Cs.exports = function () { if (!bt) { try { bt = Es()("follow-redirects") } catch { } typeof bt != "function" && (bt = function () { }) } bt.apply(null, arguments) } }); var Os = g((Km, cn) => { var yt = require("url"), gt = yt.URL, ql = require("http"), Ul = require("https"), en = require("stream").Writable, tn = require("assert"), ks = _s(); (function () { var e = typeof process < "u", a = typeof window < "u" && typeof document < "u", n = Ae(Error.captureStackTrace); !e && (a || !n) && console.warn("The follow-redirects package should be excluded from browser builds.") })(); var an = !1; try { tn(new gt("")) } catch (t) { an = t.code === "ERR_INVALID_URL" } var Bl = ["auth", "host", "hostname", "href", "path", "pathname", "port", "protocol", "query", "search", "hash"], nn = ["abort", "aborted", "connect", "error", "socket", "timeout"], on = Object.create(null); nn.forEach(function (t) { on[t] = function (e, a, n) { this._redirectable.emit(t, e, a, n) } }); var Xa = wt("ERR_INVALID_URL", "Invalid URL", TypeError), Qa = wt("ERR_FR_REDIRECTION_FAILURE", "Redirected request failed"), Ml = wt("ERR_FR_TOO_MANY_REDIRECTS", "Maximum number of redirects exceeded", Qa), zl = wt("ERR_FR_MAX_BODY_LENGTH_EXCEEDED", "Request body larger than maxBodyLength limit"), Hl = wt("ERR_STREAM_WRITE_AFTER_END", "write after end"), $l = en.prototype.destroy || Rs; function X(t, e) { en.call(this), this._sanitizeOptions(t), this._options = t, this._ended = !1, this._ending = !1, this._redirectCount = 0, this._redirects = [], this._requestBodyLength = 0, this._requestBodyBuffers = [], e && this.on("response", e); var a = this; this._onNativeResponse = function (n) { try { a._processResponse(n) } catch (i) { a.emit("error", i instanceof Qa ? i : new Qa({ cause: i })) } }, this._performRequest() } X.prototype = Object.create(en.prototype); X.prototype.abort = function () { rn(this._currentRequest), this._currentRequest.abort(), this.emit("abort") }; X.prototype.destroy = function (t) { return rn(this._currentRequest, t), $l.call(this, t), this }; X.prototype.write = function (t, e, a) { if (this._ending) throw new Hl; if (!Re(t) && !Vl(t)) throw new TypeError("data should be a string, Buffer or Uint8Array"); if (Ae(e) && (a = e, e = null), t.length === 0) { a && a(); return } this._requestBodyLength + t.length <= this._options.maxBodyLength ? (this._requestBodyLength += t.length, this._requestBodyBuffers.push({ data: t, encoding: e }), this._currentRequest.write(t, e, a)) : (this.emit("error", new zl), this.abort()) }; X.prototype.end = function (t, e, a) { if (Ae(t) ? (a = t, t = e = null) : Ae(e) && (a = e, e = null), !t) this._ended = this._ending = !0, this._currentRequest.end(null, null, a); else { var n = this, i = this._currentRequest; this.write(t, e, function () { n._ended = !0, i.end(null, null, a) }), this._ending = !0 } }; X.prototype.setHeader = function (t, e) { this._options.headers[t] = e, this._currentRequest.setHeader(t, e) }; X.prototype.removeHeader = function (t) { delete this._options.headers[t], this._currentRequest.removeHeader(t) }; X.prototype.setTimeout = function (t, e) { var a = this; function n(s) { s.setTimeout(t), s.removeListener("timeout", s.destroy), s.addListener("timeout", s.destroy) } function i(s) { a._timeout && clearTimeout(a._timeout), a._timeout = setTimeout(function () { a.emit("timeout"), o() }, t), n(s) } function o() { a._timeout && (clearTimeout(a._timeout), a._timeout = null), a.removeListener("abort", o), a.removeListener("error", o), a.removeListener("response", o), a.removeListener("close", o), e && a.removeListener("timeout", e), a.socket || a._currentRequest.removeListener("socket", i) } return e && this.on("timeout", e), this.socket ? i(this.socket) : this._currentRequest.once("socket", i), this.on("socket", n), this.on("abort", o), this.on("error", o), this.on("response", o), this.on("close", o), this };["flushHeaders", "getHeader", "setNoDelay", "setSocketKeepAlive"].forEach(function (t) { X.prototype[t] = function (e, a) { return this._currentRequest[t](e, a) } });["aborted", "connection", "socket"].forEach(function (t) { Object.defineProperty(X.prototype, t, { get: function () { return this._currentRequest[t] } }) }); X.prototype._sanitizeOptions = function (t) { if (t.headers || (t.headers = {}), t.host && (t.hostname || (t.hostname = t.host), delete t.host), !t.pathname && t.path) { var e = t.path.indexOf("?"); e < 0 ? t.pathname = t.path : (t.pathname = t.path.substring(0, e), t.search = t.path.substring(e)) } }; X.prototype._performRequest = function () { var t = this._options.protocol, e = this._options.nativeProtocols[t]; if (!e) throw new TypeError("Unsupported protocol " + t); if (this._options.agents) { var a = t.slice(0, -1); this._options.agent = this._options.agents[a] } var n = this._currentRequest = e.request(this._options, this._onNativeResponse); n._redirectable = this; for (var i of nn) n.on(i, on[i]); if (this._currentUrl = /^\//.test(this._options.path) ? yt.format(this._options) : this._options.path, this._isRedirect) { var o = 0, s = this, r = this._requestBodyBuffers; (function u(l) { if (n === s._currentRequest) if (l) s.emit("error", l); else if (o < r.length) { var c = r[o++]; n.finished || n.write(c.data, c.encoding, u) } else s._ended && n.end() })() } }; X.prototype._processResponse = function (t) { var e = t.statusCode; this._options.trackRedirects && this._redirects.push({ url: this._currentUrl, headers: t.headers, statusCode: e }); var a = t.headers.location; if (!a || this._options.followRedirects === !1 || e < 300 || e >= 400) { t.responseUrl = this._currentUrl, t.redirects = this._redirects, this.emit("response", t), this._requestBodyBuffers = []; return } if (rn(this._currentRequest), t.destroy(), ++this._redirectCount > this._options.maxRedirects) throw new Ml; var n, i = this._options.beforeRedirect; i && (n = Object.assign({ Host: t.req.getHeader("host") }, this._options.headers)); var o = this._options.method; ((e === 301 || e === 302) && this._options.method === "POST" || e === 303 && !/^(?:GET|HEAD)$/.test(this._options.method)) && (this._options.method = "GET", this._requestBodyBuffers = [], Ya(/^content-/i, this._options.headers)); var s = Ya(/^host$/i, this._options.headers), r = sn(this._currentUrl), u = s || r.host, l = /^\w+:/.test(a) ? this._currentUrl : yt.format(Object.assign(r, { host: u })), c = Gl(a, l); if (ks("redirecting to", c.href), this._isRedirect = !0, Za(c, this._options), (c.protocol !== r.protocol && c.protocol !== "https:" || c.host !== u && !Wl(c.host, u)) && Ya(/^(?:(?:proxy-)?authorization|cookie)$/i, this._options.headers), Ae(i)) { var d = { headers: t.headers, statusCode: e }, m = { url: l, method: o, headers: n }; i(this._options, d, m), this._sanitizeOptions(this._options) } this._performRequest() }; function Ts(t) { var e = { maxRedirects: 21, maxBodyLength: 10485760 }, a = {}; return Object.keys(t).forEach(function (n) { var i = n + ":", o = a[i] = t[n], s = e[n] = Object.create(o); function r(l, c, d) { return Jl(l) ? l = Za(l) : Re(l) ? l = Za(sn(l)) : (d = c, c = As(l), l = { protocol: i }), Ae(c) && (d = c, c = null), c = Object.assign({ maxRedirects: e.maxRedirects, maxBodyLength: e.maxBodyLength }, l, c), c.nativeProtocols = a, !Re(c.host) && !Re(c.hostname) && (c.hostname = "::1"), tn.equal(c.protocol, i, "protocol mismatch"), ks("options", c), new X(c, d) } function u(l, c, d) { var m = s.request(l, c, d); return m.end(), m } Object.defineProperties(s, { request: { value: r, configurable: !0, enumerable: !0, writable: !0 }, get: { value: u, configurable: !0, enumerable: !0, writable: !0 } }) }), e } function Rs() { } function sn(t) { var e; if (an) e = new gt(t); else if (e = As(yt.parse(t)), !Re(e.protocol)) throw new Xa({ input: t }); return e } function Gl(t, e) { return an ? new gt(t, e) : sn(yt.resolve(e, t)) } function As(t) { if (/^\[/.test(t.hostname) && !/^\[[:0-9a-f]+\]$/i.test(t.hostname)) throw new Xa({ input: t.href || t }); if (/^\[/.test(t.host) && !/^\[[:0-9a-f]+\](:\d+)?$/i.test(t.host)) throw new Xa({ input: t.href || t }); return t } function Za(t, e) { var a = e || {}; for (var n of Bl) a[n] = t[n]; return a.hostname.startsWith("[") && (a.hostname = a.hostname.slice(1, -1)), a.port !== "" && (a.port = Number(a.port)), a.path = a.search ? a.pathname + a.search : a.pathname, a } function Ya(t, e) { var a; for (var n in e) t.test(n) && (a = e[n], delete e[n]); return a === null || typeof a > "u" ? void 0 : String(a).trim() } function wt(t, e, a) { function n(i) { Ae(Error.captureStackTrace) && Error.captureStackTrace(this, this.constructor), Object.assign(this, i || {}), this.code = t, this.message = this.cause ? e + ": " + this.cause.message : e } return n.prototype = new (a || Error), Object.defineProperties(n.prototype, { constructor: { value: n, enumerable: !1 }, name: { value: "Error [" + t + "]", enumerable: !1 } }), n } function rn(t, e) { for (var a of nn) t.removeListener(a, on[a]); t.on("error", Rs), t.destroy(e) } function Wl(t, e) { tn(Re(t) && Re(e)); var a = t.length - e.length - 1; return a > 0 && t[a] === "." && t.endsWith(e) } function Re(t) { return typeof t == "string" || t instanceof String } function Ae(t) { return typeof t == "function" } function Vl(t) { return typeof t == "object" && "length" in t } function Jl(t) { return gt && t instanceof gt } cn.exports = Ts({ http: ql, https: Ul }); cn.exports.wrap = Ts }); var Ou = {}; On(Ou, { activate: () => Ru, deactivate: () => Au }); module.exports = Sr(Ou); var Q = L(require("vscode")); var In = require("child_process"), Dn = require("util"), Fn = L(require("https")), be = L(require("process")); var da = class { constructor() { this.outputChannel = null; this.logLevel = 1 } init() { try { let e = require("vscode"); this.outputChannel = e.window.createOutputChannel("MedidorTokens") } catch { } } log(e, a, n) { if (e < this.logLevel) return; let o = `[${new Date().toISOString()}] [${a}] ${n}`; this.outputChannel?.appendLine(o), console.log(o) } debug(e) { this.log(0, "DEBUG", e) } info(e) { this.log(1, "INFO", e) } warn(e) { this.log(2, "WARN", e) } error(e) { this.log(3, "ERROR", e) } }, T = new da; var Pn = L(require("process")), Ie = class { isAntigravityProcess(e) { return !e.includes("--extension_server_port") || !e.includes("--csrf_token") ? !1 : /--app_data_dir\s+antigravity\b/i.test(e) } getProcessListCommand(e) { return `chcp 65001 >nul && powershell -NoProfile -Command "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-CimInstance Win32_Process -Filter 'name=''${e}''' | Select-Object ProcessId,CommandLine | ConvertTo-Json"` } getProcessByKeywordCommand() { return `chcp 65001 >nul && powershell -NoProfile -Command "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'csrf_token' } | Select-Object ProcessId,Name,CommandLine | ConvertTo-Json"` } parseProcessInfo(e) { T.debug("[WindowsStrategy] Parsing JSON process info..."); try { let a = e.indexOf("["), n = e.indexOf("{"), i = e; if (a >= 0 || n >= 0) { let r = a >= 0 && n >= 0 ? Math.min(a, n) : Math.max(a, n); i = e.substring(r) } let o = JSON.parse(i.trim()); if (Array.isArray(o) || (o = [o]), o.length === 0) return []; let s = []; for (let r of o) { let u = r.CommandLine || ""; if (!u || !this.isAntigravityProcess(u)) continue; let l = r.ProcessId; if (!l) continue; let c = u.match(/--extension_server_port[=\s]+(\d+)/), d = u.match(/--csrf_token[=\s]+([a-f0-9-]+)/i); if (!d?.[1]) continue; let m = c?.[1] ? parseInt(c[1], 10) : 0, v = d[1]; s.push({ pid: l, extensionPort: m, csrfToken: v }) } return s } catch { return [] } } getPortListCommand(e) { return `chcp 65001 >nul && powershell -NoProfile -NonInteractive -Command "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; $ports = Get-NetTCPConnection -State Listen -OwningProcess ${e} -ErrorAction SilentlyContinue | Select-Object -ExpandProperty LocalPort; if ($ports) { $ports | Sort-Object -Unique }"` } parseListeningPorts(e) { let a = new Set, n = e.match(/\b\d{1,5}\b/g) || []; for (let i of n) { let o = parseInt(i, 10); o > 0 && o <= 65535 && a.add(o) } return Array.from(a).sort((i, o) => i - o) } getErrorMessages() { return { processNotFound: "language_server process not found", commandNotAvailable: "PowerShell command failed", requirements: ["Antigravity running", "language_server_windows_x64 running"] } } getDiagnosticCommand() { return `powershell -NoProfile -Command "Get-Process | Where-Object { $_.ProcessName -match 'language|antigravity' } | Select-Object Id,ProcessName,Path | Format-Table -AutoSize"` } }, De = class {
	constructor(e) { this.availablePortCommand = null; this.portCommandChecked = !1; this.targetPid = 0; this.platform = e } async detectAvailablePortCommand() { if (!this.portCommandChecked) { this.portCommandChecked = !0; try { let { exec: e } = await import("child_process"), { promisify: a } = await import("util"), n = a(e), i = ["lsof", "ss", "netstat"]; for (let o of i) try { await n(`which ${o}`, { timeout: 3e3 }), this.availablePortCommand = o; return } catch { } } catch { } } } isAntigravityProcess(e) { return !e.includes("--extension_server_port") || !e.includes("--csrf_token") ? !1 : /--app_data_dir\s+antigravity\b/i.test(e) } getProcessListCommand(e) { return `ps -ww -eo pid,ppid,args | grep "${e}" | grep -v grep` } parseProcessInfo(e) {
		let a = e.split(`
`).filter(o => o.trim()), n = Pn.pid, i = []; for (let o of a) { let s = o.trim().split(/\s+/); if (s.length < 3) continue; let r = parseInt(s[0], 10), u = parseInt(s[1], 10), l = s.slice(2).join(" "); if (isNaN(r)) continue; let c = l.match(/--extension_server_port[=\s]+(\d+)/), d = l.match(/--csrf_token[=\s]+([a-zA-Z0-9-]+)/i); if (d?.[1] && this.isAntigravityProcess(l)) { let m = c?.[1] ? parseInt(c[1], 10) : 0, v = d[1]; i.push({ pid: r, ppid: u, extensionPort: m, csrfToken: v }) } } return i.sort((o, s) => o.ppid === n ? -1 : s.ppid === n ? 1 : 0)
	} getPortListCommand(e) { return this.targetPid = e, this.platform === "darwin" ? `lsof -nP -a -iTCP -sTCP:LISTEN -p ${e} 2>/dev/null | grep -E "^\\S+\\s+${e}\\s"` : `ss -tlnp 2>/dev/null | grep "pid=${e}," || lsof -nP -a -iTCP -sTCP:LISTEN -p ${e} 2>/dev/null | grep -E "^\\S+\\s+${e}\\s" || netstat -tulpn 2>/dev/null | grep ${e}` } async ensurePortCommandAvailable() { await this.detectAvailablePortCommand() } parseListeningPorts(e) {
		let a = []; if (this.platform === "darwin") {
			let n = e.split(`
`); for (let i of n) { if (!i.includes("(LISTEN)")) continue; let o = i.match(/[*\d.:]+:(\d+)\s+\(LISTEN\)/); if (o) { let s = parseInt(o[1], 10); a.includes(s) || a.push(s) } }
		} else { let n = /LISTEN\s+\d+\s+\d+\s+(?:\*|[\d.]+|\[[\da-f:]*\]):(\d+)/gi, i; for (; (i = n.exec(e)) !== null;) { let o = parseInt(i[1], 10); a.includes(o) || a.push(o) } if (a.length === 0) { let o = /(?:TCP|UDP)\s+(?:\*|[\d.]+|\[[\da-f:]+\]):(\d+)\s+\(LISTEN\)/gi; for (; (i = o.exec(e)) !== null;) { let s = parseInt(i[1], 10); a.includes(s) || a.push(s) } } } return a.sort((n, i) => n - i)
	} getErrorMessages() { return { processNotFound: "Process not found", commandNotAvailable: "Command check failed", requirements: ["lsof or netstat or ss"] } } getDiagnosticCommand() { return "ps aux | grep -E 'language|antigravity' | grep -v grep" }
}; var Fe = { PROCESS_SCAN_RETRY_MS: 100, HTTP_TIMEOUT_MS: 1e4, PROCESS_CMD_TIMEOUT_MS: 15e3 }, st = { windows: "language_server_windows_x64.exe", darwin_arm: "language_server_macos_arm", darwin_x64: "language_server_macos", linux: "language_server_linux" }, Ne = { GET_ALL_CASCADE_TRAJECTORIES: "exa.language_server_pb.LanguageServerService/GetAllCascadeTrajectories", GET_CASCADE_METADATA: "exa.language_server_pb.LanguageServerService/GetCascadeTrajectoryGeneratorMetadata", GET_CASCADE_STEPS: "exa.language_server_pb.LanguageServerService/GetCascadeTrajectorySteps", GET_UNLEASH_DATA: "exa.language_server_pb.LanguageServerService/GetUnleashData", GET_USER_STATUS: "exa.language_server_pb.LanguageServerService/GetUserStatus" }; var Ln = (0, Dn.promisify)(In.exec), qe = class {
	constructor(e) { this.lastDiagnostics = { scan_method: "unknown", target_process: "", attempts: 0, found_candidates: 0 }; this.execFn = e || Ln, T.debug(`Initializing ProcessHunter. Platform: ${be.platform}, Arch: ${be.arch}`), be.platform === "win32" ? (this.strategy = new Ie, this.targetProcess = st.windows) : be.platform === "darwin" ? (this.strategy = new De("darwin"), this.targetProcess = be.arch === "arm64" ? st.darwin_arm : st.darwin_x64) : (this.strategy = new De("linux"), this.targetProcess = st.linux) } async scanEnvironment(e = 3) { T.info(`Scanning environment, max attempts: ${e}`); try { let n = require("fs"), i = require("path"), o = require("os"), s = i.join(o.homedir(), ".gemini", "antigravity", "brain"); if (Q.extensions?.getExtension("google.google-antigravity") && n.existsSync(s)) return T.info("Detected Antigravity IDE 2.6+ (local brain transcripts)"), { extensionPort: 0, connectPort: -1, csrfToken: "agy-2.6-local-brain" } } catch { } let a = await this.scanByProcessName(e); if (a || be.platform === "win32" && this.strategy instanceof Ie && (a = await this.scanByKeyword(), a)) return a; try { let n = require("fs"), i = require("path"), o = require("os"), s = i.join(o.homedir(), ".gemini", "antigravity", "brain"); if (n.existsSync(s)) return T.info("Fallback to Antigravity local brain transcripts"), { extensionPort: 0, connectPort: -1, csrfToken: "agy-2.6-local-brain" } } catch { } return await this.runDiagnostics(), null } async scanByProcessName(e) { this.lastDiagnostics = { scan_method: "process_name", target_process: this.targetProcess, attempts: e, found_candidates: 0 }; for (let a = 0; a < e; a++)try { let n = this.strategy.getProcessListCommand(this.targetProcess), { stdout: i } = await this.execFn(n, { timeout: Fe.PROCESS_CMD_TIMEOUT_MS }); if (!i || !i.trim()) continue; let o = this.strategy.parseProcessInfo(i); if (o.length > 0) { this.lastDiagnostics.found_candidates = o.length; for (let s of o) { let r = await this.verifyAndConnect(s); if (r) return r } } } catch (n) { T.error(`Attempt ${a + 1} failed: ${n.message}`), a < e - 1 && await new Promise(i => setTimeout(i, Fe.PROCESS_SCAN_RETRY_MS)) } return null } async scanByKeyword() { if (!(this.strategy instanceof Ie)) return null; try { let e = this.strategy.getProcessByKeywordCommand(), { stdout: a } = await this.execFn(e, { timeout: Fe.PROCESS_CMD_TIMEOUT_MS }), n = this.strategy.parseProcessInfo(a); for (let i of n) { let o = await this.verifyAndConnect(i); if (o) return o } } catch (e) { T.error(`Keyword search failed: ${e.message}`) } return null } async verifyAndConnect(e) { let a = await this.identifyPorts(e.pid); if (a.length > 0) { let n = await this.verifyConnection(a, e.csrfToken); if (n) return T.info(`\u2705 Verified connection on port ${n}`), { extensionPort: e.extensionPort, connectPort: n, csrfToken: e.csrfToken } } return null } async identifyPorts(e) { try { this.strategy instanceof De && await this.strategy.ensurePortCommandAvailable(); let a = this.strategy.getPortListCommand(e), { stdout: n } = await this.execFn(a); return this.strategy.parseListeningPorts(n) } catch (a) { return T.error(`Port ID failed: ${a.message}`), [] } } async verifyConnection(e, a) { for (let n of e) if (await this.pingPort(n, a)) return n; return null } pingPort(e, a) { return new Promise(n => { let i = { hostname: "127.0.0.1", port: e, path: "/" + Ne.GET_UNLEASH_DATA, method: "POST", headers: { "Content-Type": "application/json", "X-Codeium-Csrf-Token": a, "Connect-Protocol-Version": "1" }, rejectUnauthorized: !1, timeout: 5e3, agent: !1 }, o = Fn.request(i, s => n(s.statusCode === 200)); o.on("error", () => n(!1)), o.on("timeout", () => { o.destroy(), n(!1) }), o.write(JSON.stringify({ wrapper_data: {} })), o.end() }) } async runDiagnostics() {
		try {
			let e = this.strategy.getDiagnosticCommand(), { stdout: a } = await Ln(e, { timeout: 5e3 }); T.info(`Diagnostics:
${a}`)
		} catch (e) { T.error(`Diagnostics failed: ${e.message}`) }
	}
}; function rt(t, e) { return function () { return t.apply(e, arguments) } } var { toString: Er } = Object.prototype, { getPrototypeOf: fa } = Object, { iterator: jt, toStringTag: qn } = Symbol, Pt = (t => e => { let a = Er.call(e); return t[a] || (t[a] = a.slice(8, -1).toLowerCase()) })(Object.create(null)), ie = t => (t = t.toLowerCase(), e => Pt(e) === t), Lt = t => e => typeof e === t, { isArray: Be } = Array, Ue = Lt("undefined"); function ct(t) { return t !== null && !Ue(t) && t.constructor !== null && !Ue(t.constructor) && J(t.constructor.isBuffer) && t.constructor.isBuffer(t) } var Un = ie("ArrayBuffer"); function Cr(t) { let e; return typeof ArrayBuffer < "u" && ArrayBuffer.isView ? e = ArrayBuffer.isView(t) : e = t && t.buffer && Un(t.buffer), e } var _r = Lt("string"), J = Lt("function"), Bn = Lt("number"), pt = t => t !== null && typeof t == "object", kr = t => t === !0 || t === !1, Ot = t => { if (Pt(t) !== "object") return !1; let e = fa(t); return (e === null || e === Object.prototype || Object.getPrototypeOf(e) === null) && !(qn in t) && !(jt in t) }, Tr = t => { if (!pt(t) || ct(t)) return !1; try { return Object.keys(t).length === 0 && Object.getPrototypeOf(t) === Object.prototype } catch { return !1 } }, Rr = ie("Date"), Ar = ie("File"), Or = ie("Blob"), jr = ie("FileList"), Pr = t => pt(t) && J(t.pipe), Lr = t => { let e; return t && (typeof FormData == "function" && t instanceof FormData || J(t.append) && ((e = Pt(t)) === "formdata" || e === "object" && J(t.toString) && t.toString() === "[object FormData]")) }, Ir = ie("URLSearchParams"), [Dr, Fr, Nr, qr] = ["ReadableStream", "Request", "Response", "Headers"].map(ie), Ur = t => t.trim ? t.trim() : t.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, ""); function lt(t, e, { allOwnKeys: a = !1 } = {}) { if (t === null || typeof t > "u") return; let n, i; if (typeof t != "object" && (t = [t]), Be(t)) for (n = 0, i = t.length; n < i; n++)e.call(null, t[n], n, t); else { if (ct(t)) return; let o = a ? Object.getOwnPropertyNames(t) : Object.keys(t), s = o.length, r; for (n = 0; n < s; n++)r = o[n], e.call(null, t[r], r, t) } } function Mn(t, e) { if (ct(t)) return null; e = e.toLowerCase(); let a = Object.keys(t), n = a.length, i; for (; n-- > 0;)if (i = a[n], e === i.toLowerCase()) return i; return null } var Ee = typeof globalThis < "u" ? globalThis : typeof self < "u" ? self : typeof window < "u" ? window : global, zn = t => !Ue(t) && t !== Ee; function ma() { let { caseless: t, skipUndefined: e } = zn(this) && this || {}, a = {}, n = (i, o) => { let s = t && Mn(a, o) || o; Ot(a[s]) && Ot(i) ? a[s] = ma(a[s], i) : Ot(i) ? a[s] = ma({}, i) : Be(i) ? a[s] = i.slice() : (!e || !Ue(i)) && (a[s] = i) }; for (let i = 0, o = arguments.length; i < o; i++)arguments[i] && lt(arguments[i], n); return a } var Br = (t, e, a, { allOwnKeys: n } = {}) => (lt(e, (i, o) => { a && J(i) ? Object.defineProperty(t, o, { value: rt(i, a), writable: !0, enumerable: !0, configurable: !0 }) : Object.defineProperty(t, o, { value: i, writable: !0, enumerable: !0, configurable: !0 }) }, { allOwnKeys: n }), t), Mr = t => (t.charCodeAt(0) === 65279 && (t = t.slice(1)), t), zr = (t, e, a, n) => { t.prototype = Object.create(e.prototype, n), Object.defineProperty(t.prototype, "constructor", { value: t, writable: !0, enumerable: !1, configurable: !0 }), Object.defineProperty(t, "super", { value: e.prototype }), a && Object.assign(t.prototype, a) }, Hr = (t, e, a, n) => { let i, o, s, r = {}; if (e = e || {}, t == null) return e; do { for (i = Object.getOwnPropertyNames(t), o = i.length; o-- > 0;)s = i[o], (!n || n(s, t, e)) && !r[s] && (e[s] = t[s], r[s] = !0); t = a !== !1 && fa(t) } while (t && (!a || a(t, e)) && t !== Object.prototype); return e }, $r = (t, e, a) => { t = String(t), (a === void 0 || a > t.length) && (a = t.length), a -= e.length; let n = t.indexOf(e, a); return n !== -1 && n === a }, Gr = t => { if (!t) return null; if (Be(t)) return t; let e = t.length; if (!Bn(e)) return null; let a = new Array(e); for (; e-- > 0;)a[e] = t[e]; return a }, Wr = (t => e => t && e instanceof t)(typeof Uint8Array < "u" && fa(Uint8Array)), Vr = (t, e) => { let n = (t && t[jt]).call(t), i; for (; (i = n.next()) && !i.done;) { let o = i.value; e.call(t, o[0], o[1]) } }, Jr = (t, e) => { let a, n = []; for (; (a = t.exec(e)) !== null;)n.push(a); return n }, Kr = ie("HTMLFormElement"), Yr = t => t.toLowerCase().replace(/[-_\s]([a-z\d])(\w*)/g, function (a, n, i) { return n.toUpperCase() + i }), Nn = (({ hasOwnProperty: t }) => (e, a) => t.call(e, a))(Object.prototype), Xr = ie("RegExp"), Hn = (t, e) => { let a = Object.getOwnPropertyDescriptors(t), n = {}; lt(a, (i, o) => { let s; (s = e(i, o, t)) !== !1 && (n[o] = s || i) }), Object.defineProperties(t, n) }, Qr = t => { Hn(t, (e, a) => { if (J(t) && ["arguments", "caller", "callee"].indexOf(a) !== -1) return !1; let n = t[a]; if (J(n)) { if (e.enumerable = !1, "writable" in e) { e.writable = !1; return } e.set || (e.set = () => { throw Error("Can not rewrite read-only method '" + a + "'") }) } }) }, Zr = (t, e) => { let a = {}, n = i => { i.forEach(o => { a[o] = !0 }) }; return Be(t) ? n(t) : n(String(t).split(e)), a }, ec = () => { }, tc = (t, e) => t != null && Number.isFinite(t = +t) ? t : e; function ac(t) { return !!(t && J(t.append) && t[qn] === "FormData" && t[jt]) } var nc = t => { let e = new Array(10), a = (n, i) => { if (pt(n)) { if (e.indexOf(n) >= 0) return; if (ct(n)) return n; if (!("toJSON" in n)) { e[i] = n; let o = Be(n) ? [] : {}; return lt(n, (s, r) => { let u = a(s, i + 1); !Ue(u) && (o[r] = u) }), e[i] = void 0, o } } return n }; return a(t, 0) }, ic = ie("AsyncFunction"), oc = t => t && (pt(t) || J(t)) && J(t.then) && J(t.catch), $n = ((t, e) => t ? setImmediate : e ? ((a, n) => (Ee.addEventListener("message", ({ source: i, data: o }) => { i === Ee && o === a && n.length && n.shift()() }, !1), i => { n.push(i), Ee.postMessage(a, "*") }))(`axios@${Math.random()}`, []) : a => setTimeout(a))(typeof setImmediate == "function", J(Ee.postMessage)), sc = typeof queueMicrotask < "u" ? queueMicrotask.bind(Ee) : typeof process < "u" && process.nextTick || $n, rc = t => t != null && J(t[jt]), p = { isArray: Be, isArrayBuffer: Un, isBuffer: ct, isFormData: Lr, isArrayBufferView: Cr, isString: _r, isNumber: Bn, isBoolean: kr, isObject: pt, isPlainObject: Ot, isEmptyObject: Tr, isReadableStream: Dr, isRequest: Fr, isResponse: Nr, isHeaders: qr, isUndefined: Ue, isDate: Rr, isFile: Ar, isBlob: Or, isRegExp: Xr, isFunction: J, isStream: Pr, isURLSearchParams: Ir, isTypedArray: Wr, isFileList: jr, forEach: lt, merge: ma, extend: Br, trim: Ur, stripBOM: Mr, inherits: zr, toFlatObject: Hr, kindOf: Pt, kindOfTest: ie, endsWith: $r, toArray: Gr, forEachEntry: Vr, matchAll: Jr, isHTMLForm: Kr, hasOwnProperty: Nn, hasOwnProp: Nn, reduceDescriptors: Hn, freezeMethods: Qr, toObjectSet: Zr, toCamelCase: Yr, noop: ec, toFiniteNumber: tc, findKey: Mn, global: Ee, isContextDefined: zn, isSpecCompliantForm: ac, toJSONObject: nc, isAsyncFn: ic, isThenable: oc, setImmediate: $n, asap: sc, isIterable: rc }; var G = class t extends Error { static from(e, a, n, i, o, s) { let r = new t(e.message, a || e.code, n, i, o); return r.cause = e, r.name = e.name, s && Object.assign(r, s), r } constructor(e, a, n, i, o) { super(e), this.name = "AxiosError", this.isAxiosError = !0, a && (this.code = a), n && (this.config = n), i && (this.request = i), o && (this.response = o, this.status = o.status) } toJSON() { return { message: this.message, name: this.name, description: this.description, number: this.number, fileName: this.fileName, lineNumber: this.lineNumber, columnNumber: this.columnNumber, stack: this.stack, config: p.toJSONObject(this.config), code: this.code, status: this.status } } }; G.ERR_BAD_OPTION_VALUE = "ERR_BAD_OPTION_VALUE"; G.ERR_BAD_OPTION = "ERR_BAD_OPTION"; G.ECONNABORTED = "ECONNABORTED"; G.ETIMEDOUT = "ETIMEDOUT"; G.ERR_NETWORK = "ERR_NETWORK"; G.ERR_FR_TOO_MANY_REDIRECTS = "ERR_FR_TOO_MANY_REDIRECTS"; G.ERR_DEPRECATED = "ERR_DEPRECATED"; G.ERR_BAD_RESPONSE = "ERR_BAD_RESPONSE"; G.ERR_BAD_REQUEST = "ERR_BAD_REQUEST"; G.ERR_CANCELED = "ERR_CANCELED"; G.ERR_NOT_SUPPORT = "ERR_NOT_SUPPORT"; G.ERR_INVALID_URL = "ERR_INVALID_URL"; var b = G; var Ko = L(Jo(), 1), $t = Ko.default; function Ia(t) { return p.isPlainObject(t) || p.isArray(t) } function Xo(t) { return p.endsWith(t, "[]") ? t.slice(0, -2) : t } function Yo(t, e, a) { return t ? t.concat(e).map(function (i, o) { return i = Xo(i), !a && o ? "[" + i + "]" : i }).join(a ? "." : "") : e } function Wp(t) { return p.isArray(t) && !t.some(Ia) } var Vp = p.toFlatObject(p, {}, null, function (e) { return /^is[A-Z]/.test(e) }); function Jp(t, e, a) { if (!p.isObject(t)) throw new TypeError("target must be an object"); e = e || new ($t || FormData), a = p.toFlatObject(a, { metaTokens: !0, dots: !1, indexes: !1 }, !1, function (h, x) { return !p.isUndefined(x[h]) }); let n = a.metaTokens, i = a.visitor || c, o = a.dots, s = a.indexes, u = (a.Blob || typeof Blob < "u" && Blob) && p.isSpecCompliantForm(e); if (!p.isFunction(i)) throw new TypeError("visitor must be a function"); function l(f) { if (f === null) return ""; if (p.isDate(f)) return f.toISOString(); if (p.isBoolean(f)) return f.toString(); if (!u && p.isBlob(f)) throw new b("Blob is not supported. Use a Buffer instead."); return p.isArrayBuffer(f) || p.isTypedArray(f) ? u && typeof Blob == "function" ? new Blob([f]) : Buffer.from(f) : f } function c(f, h, x) { let S = f; if (f && !x && typeof f == "object") { if (p.endsWith(h, "{}")) h = n ? h : h.slice(0, -2), f = JSON.stringify(f); else if (p.isArray(f) && Wp(f) || (p.isFileList(f) || p.endsWith(h, "[]")) && (S = p.toArray(f))) return h = Xo(h), S.forEach(function (C, O) { !(p.isUndefined(C) || C === null) && e.append(s === !0 ? Yo([h], O, o) : s === null ? h : h + "[]", l(C)) }), !1 } return Ia(f) ? !0 : (e.append(Yo(x, h, o), l(f)), !1) } let d = [], m = Object.assign(Vp, { defaultVisitor: c, convertValue: l, isVisitable: Ia }); function v(f, h) { if (!p.isUndefined(f)) { if (d.indexOf(f) !== -1) throw Error("Circular reference detected in " + h.join(".")); d.push(f), p.forEach(f, function (S, k) { (!(p.isUndefined(S) || S === null) && i.call(e, S, p.isString(k) ? k.trim() : k, h, m)) === !0 && v(S, h ? h.concat(k) : [k]) }), d.pop() } } if (!p.isObject(t)) throw new TypeError("data must be an object"); return v(t), e } var ye = Jp; function Qo(t) { let e = { "!": "%21", "'": "%27", "(": "%28", ")": "%29", "~": "%7E", "%20": "+", "%00": "\0" }; return encodeURIComponent(t).replace(/[!'()~]|%20|%00/g, function (n) { return e[n] }) } function Zo(t, e) { this._pairs = [], t && ye(t, this, e) } var es = Zo.prototype; es.append = function (e, a) { this._pairs.push([e, a]) }; es.toString = function (e) { let a = e ? function (n) { return e.call(this, n, Qo) } : Qo; return this._pairs.map(function (i) { return a(i[0]) + "=" + a(i[1]) }, "").join("&") }; var ts = Zo; function Kp(t) { return encodeURIComponent(t).replace(/%3A/gi, ":").replace(/%24/g, "$").replace(/%2C/gi, ",").replace(/%20/g, "+") } function _e(t, e, a) { if (!e) return t; let n = a && a.encode || Kp, i = p.isFunction(a) ? { serialize: a } : a, o = i && i.serialize, s; if (o ? s = o(e, i) : s = p.isURLSearchParams(e) ? e.toString() : new ts(e, i).toString(n), s) { let r = t.indexOf("#"); r !== -1 && (t = t.slice(0, r)), t += (t.indexOf("?") === -1 ? "?" : "&") + s } return t } var Da = class { constructor() { this.handlers = [] } use(e, a, n) { return this.handlers.push({ fulfilled: e, rejected: a, synchronous: n ? n.synchronous : !1, runWhen: n ? n.runWhen : null }), this.handlers.length - 1 } eject(e) { this.handlers[e] && (this.handlers[e] = null) } clear() { this.handlers && (this.handlers = []) } forEach(e) { p.forEach(this.handlers, function (n) { n !== null && e(n) }) } }, Fa = Da; var Ge = { silentJSONParsing: !0, forcedJSONParsing: !0, clarifyTimeoutError: !1 }; var os = L(require("crypto"), 1); var as = L(require("url"), 1), ns = as.default.URLSearchParams; var Na = "abcdefghijklmnopqrstuvwxyz", is = "0123456789", ss = { DIGIT: is, ALPHA: Na, ALPHA_DIGIT: Na + Na.toUpperCase() + is }, Yp = (t = 16, e = ss.ALPHA_DIGIT) => { let a = "", { length: n } = e, i = new Uint32Array(t); os.default.randomFillSync(i); for (let o = 0; o < t; o++)a += e[i[o] % n]; return a }, rs = { isNode: !0, classes: { URLSearchParams: ns, FormData: $t, Blob: typeof Blob < "u" && Blob || null }, ALPHABET: ss, generateString: Yp, protocols: ["http", "https", "file", "data"] }; var Ba = {}; On(Ba, { hasBrowserEnv: () => Ua, hasStandardBrowserEnv: () => Xp, hasStandardBrowserWebWorkerEnv: () => Qp, navigator: () => qa, origin: () => Zp }); var Ua = typeof window < "u" && typeof document < "u", qa = typeof navigator == "object" && navigator || void 0, Xp = Ua && (!qa || ["ReactNative", "NativeScript", "NS"].indexOf(qa.product) < 0), Qp = typeof WorkerGlobalScope < "u" && self instanceof WorkerGlobalScope && typeof self.importScripts == "function", Zp = Ua && window.location.href || "http://localhost"; var R = { ...Ba, ...rs }; function Ma(t, e) { return ye(t, new R.classes.URLSearchParams, { visitor: function (a, n, i, o) { return R.isNode && p.isBuffer(a) ? (this.append(n, a.toString("base64")), !1) : o.defaultVisitor.apply(this, arguments) }, ...e }) } function el(t) { return p.matchAll(/\w+|\[(\w*)]/g, t).map(e => e[0] === "[]" ? "" : e[1] || e[0]) } function tl(t) { let e = {}, a = Object.keys(t), n, i = a.length, o; for (n = 0; n < i; n++)o = a[n], e[o] = t[o]; return e } function al(t) { function e(a, n, i, o) { let s = a[o++]; if (s === "__proto__") return !0; let r = Number.isFinite(+s), u = o >= a.length; return s = !s && p.isArray(i) ? i.length : s, u ? (p.hasOwnProp(i, s) ? i[s] = [i[s], n] : i[s] = n, !r) : ((!i[s] || !p.isObject(i[s])) && (i[s] = []), e(a, n, i[s], o) && p.isArray(i[s]) && (i[s] = tl(i[s])), !r) } if (p.isFormData(t) && p.isFunction(t.entries)) { let a = {}; return p.forEachEntry(t, (n, i) => { e(el(n), i, a, 0) }), a } return null } var Gt = al; function nl(t, e, a) { if (p.isString(t)) try { return (e || JSON.parse)(t), p.trim(t) } catch (n) { if (n.name !== "SyntaxError") throw n } return (a || JSON.stringify)(t) } var za = { transitional: Ge, adapter: ["xhr", "http", "fetch"], transformRequest: [function (e, a) { let n = a.getContentType() || "", i = n.indexOf("application/json") > -1, o = p.isObject(e); if (o && p.isHTMLForm(e) && (e = new FormData(e)), p.isFormData(e)) return i ? JSON.stringify(Gt(e)) : e; if (p.isArrayBuffer(e) || p.isBuffer(e) || p.isStream(e) || p.isFile(e) || p.isBlob(e) || p.isReadableStream(e)) return e; if (p.isArrayBufferView(e)) return e.buffer; if (p.isURLSearchParams(e)) return a.setContentType("application/x-www-form-urlencoded;charset=utf-8", !1), e.toString(); let r; if (o) { if (n.indexOf("application/x-www-form-urlencoded") > -1) return Ma(e, this.formSerializer).toString(); if ((r = p.isFileList(e)) || n.indexOf("multipart/form-data") > -1) { let u = this.env && this.env.FormData; return ye(r ? { "files[]": e } : e, u && new u, this.formSerializer) } } return o || i ? (a.setContentType("application/json", !1), nl(e)) : e }], transformResponse: [function (e) { let a = this.transitional || za.transitional, n = a && a.forcedJSONParsing, i = this.responseType === "json"; if (p.isResponse(e) || p.isReadableStream(e)) return e; if (e && p.isString(e) && (n && !this.responseType || i)) { let s = !(a && a.silentJSONParsing) && i; try { return JSON.parse(e, this.parseReviver) } catch (r) { if (s) throw r.name === "SyntaxError" ? b.from(r, b.ERR_BAD_RESPONSE, this, null, this.response) : r } } return e }], timeout: 0, xsrfCookieName: "XSRF-TOKEN", xsrfHeaderName: "X-XSRF-TOKEN", maxContentLength: -1, maxBodyLength: -1, env: { FormData: R.classes.FormData, Blob: R.classes.Blob }, validateStatus: function (e) { return e >= 200 && e < 300 }, headers: { common: { Accept: "application/json, text/plain, */*", "Content-Type": void 0 } } }; p.forEach(["delete", "get", "head", "post", "put", "patch"], t => { za.headers[t] = {} }); var We = za; var il = p.toObjectSet(["age", "authorization", "content-length", "content-type", "etag", "expires", "from", "host", "if-modified-since", "if-unmodified-since", "last-modified", "location", "max-forwards", "proxy-authorization", "referer", "retry-after", "user-agent"]), cs = t => {
	let e = {}, a, n, i; return t && t.split(`
`).forEach(function (s) { i = s.indexOf(":"), a = s.substring(0, i).trim().toLowerCase(), n = s.substring(i + 1).trim(), !(!a || e[a] && il[a]) && (a === "set-cookie" ? e[a] ? e[a].push(n) : e[a] = [n] : e[a] = e[a] ? e[a] + ", " + n : n) }), e
}; var ps = Symbol("internals"); function xt(t) { return t && String(t).trim().toLowerCase() } function Wt(t) { return t === !1 || t == null ? t : p.isArray(t) ? t.map(Wt) : String(t) } function ol(t) { let e = Object.create(null), a = /([^\s,;=]+)\s*(?:=\s*([^,;]+))?/g, n; for (; n = a.exec(t);)e[n[1]] = n[2]; return e } var sl = t => /^[-_a-zA-Z0-9^`|~,!#$%&'*+.]+$/.test(t.trim()); function Ha(t, e, a, n, i) { if (p.isFunction(n)) return n.call(this, e, a); if (i && (e = a), !!p.isString(e)) { if (p.isString(n)) return e.indexOf(n) !== -1; if (p.isRegExp(n)) return n.test(e) } } function rl(t) { return t.trim().toLowerCase().replace(/([a-z\d])(\w*)/g, (e, a, n) => a.toUpperCase() + n) } function cl(t, e) { let a = p.toCamelCase(" " + e);["get", "set", "has"].forEach(n => { Object.defineProperty(t, n + a, { value: function (i, o, s) { return this[n].call(this, e, i, o, s) }, configurable: !0 }) }) } var Ve = class {
	constructor(e) { e && this.set(e) } set(e, a, n) { let i = this; function o(r, u, l) { let c = xt(u); if (!c) throw new Error("header name must be a non-empty string"); let d = p.findKey(i, c); (!d || i[d] === void 0 || l === !0 || l === void 0 && i[d] !== !1) && (i[d || u] = Wt(r)) } let s = (r, u) => p.forEach(r, (l, c) => o(l, c, u)); if (p.isPlainObject(e) || e instanceof this.constructor) s(e, a); else if (p.isString(e) && (e = e.trim()) && !sl(e)) s(cs(e), a); else if (p.isObject(e) && p.isIterable(e)) { let r = {}, u, l; for (let c of e) { if (!p.isArray(c)) throw TypeError("Object iterator must return a key-value pair"); r[l = c[0]] = (u = r[l]) ? p.isArray(u) ? [...u, c[1]] : [u, c[1]] : c[1] } s(r, a) } else e != null && o(a, e, n); return this } get(e, a) { if (e = xt(e), e) { let n = p.findKey(this, e); if (n) { let i = this[n]; if (!a) return i; if (a === !0) return ol(i); if (p.isFunction(a)) return a.call(this, i, n); if (p.isRegExp(a)) return a.exec(i); throw new TypeError("parser must be boolean|regexp|function") } } } has(e, a) { if (e = xt(e), e) { let n = p.findKey(this, e); return !!(n && this[n] !== void 0 && (!a || Ha(this, this[n], n, a))) } return !1 } delete(e, a) { let n = this, i = !1; function o(s) { if (s = xt(s), s) { let r = p.findKey(n, s); r && (!a || Ha(n, n[r], r, a)) && (delete n[r], i = !0) } } return p.isArray(e) ? e.forEach(o) : o(e), i } clear(e) { let a = Object.keys(this), n = a.length, i = !1; for (; n--;) { let o = a[n]; (!e || Ha(this, this[o], o, e, !0)) && (delete this[o], i = !0) } return i } normalize(e) { let a = this, n = {}; return p.forEach(this, (i, o) => { let s = p.findKey(n, o); if (s) { a[s] = Wt(i), delete a[o]; return } let r = e ? rl(o) : String(o).trim(); r !== o && delete a[o], a[r] = Wt(i), n[r] = !0 }), this } concat(...e) { return this.constructor.concat(this, ...e) } toJSON(e) { let a = Object.create(null); return p.forEach(this, (n, i) => { n != null && n !== !1 && (a[i] = e && p.isArray(n) ? n.join(", ") : n) }), a } [Symbol.iterator]() { return Object.entries(this.toJSON())[Symbol.iterator]() } toString() {
		return Object.entries(this.toJSON()).map(([e, a]) => e + ": " + a).join(`
`)
	} getSetCookie() { return this.get("set-cookie") || [] } get [Symbol.toStringTag]() { return "AxiosHeaders" } static from(e) { return e instanceof this ? e : new this(e) } static concat(e, ...a) { let n = new this(e); return a.forEach(i => n.set(i)), n } static accessor(e) { let n = (this[ps] = this[ps] = { accessors: {} }).accessors, i = this.prototype; function o(s) { let r = xt(s); n[r] || (cl(i, s), n[r] = !0) } return p.isArray(e) ? e.forEach(o) : o(e), this }
}; Ve.accessor(["Content-Type", "Content-Length", "Accept", "Accept-Encoding", "User-Agent", "Authorization"]); p.reduceDescriptors(Ve.prototype, ({ value: t }, e) => { let a = e[0].toUpperCase() + e.slice(1); return { get: () => t, set(n) { this[a] = n } } }); p.freezeMethods(Ve); var P = Ve; function ht(t, e) { let a = this || We, n = e || a, i = P.from(n.headers), o = n.data; return p.forEach(t, function (r) { o = r.call(a, o, i.normalize(), e ? e.status : void 0) }), i.normalize(), o } function vt(t) { return !!(t && t.__CANCEL__) } var $a = class extends b { constructor(e, a, n) { super(e ?? "canceled", b.ERR_CANCELED, a, n), this.name = "CanceledError", this.__CANCEL__ = !0 } }, te = $a; function pe(t, e, a) { let n = a.config.validateStatus; !a.status || !n || n(a.status) ? t(a) : e(new b("Request failed with status code " + a.status, [b.ERR_BAD_REQUEST, b.ERR_BAD_RESPONSE][Math.floor(a.status / 100) - 4], a.config, a.request, a)) } function Ga(t) { return /^([a-z][a-z\d+\-.]*:)?\/\//i.test(t) } function Wa(t, e) { return e ? t.replace(/\/?\/$/, "") + "/" + e.replace(/^\/+/, "") : t } function ke(t, e, a) { let n = !Ga(e); return t && (n || a == !1) ? Wa(t, e) : e } var Ws = L(us(), 1), Vs = L(require("http"), 1), Js = L(require("https"), 1), vn = L(require("http2"), 1), bn = L(require("util"), 1), Ks = L(Os(), 1), xe = L(require("zlib"), 1); var Oe = "1.13.4"; function St(t) { let e = /^([-+\w]{1,25})(:?\/\/|:)/.exec(t); return e && e[1] || "" } var Kl = /^(?:([^;]+);)?(?:[^;]+;)?(base64|),([\s\S]*)$/; function pn(t, e, a) { let n = a && a.Blob || R.classes.Blob, i = St(t); if (e === void 0 && n && (e = !0), i === "data") { t = i.length ? t.slice(i.length + 1) : t; let o = Kl.exec(t); if (!o) throw new b("Invalid URL", b.ERR_INVALID_URL); let s = o[1], r = o[2], u = o[3], l = Buffer.from(decodeURIComponent(u), r ? "base64" : "utf8"); if (e) { if (!n) throw new b("Blob is not supported", b.ERR_NOT_SUPPORT); return new n([l], { type: s }) } return l } throw new b("Unsupported protocol " + i, b.ERR_NOT_SUPPORT) } var fe = L(require("stream"), 1); var js = L(require("stream"), 1); var ln = Symbol("internals"), un = class extends js.default.Transform { constructor(e) { e = p.toFlatObject(e, { maxRate: 0, chunkSize: 64 * 1024, minChunkSize: 100, timeWindow: 500, ticksRate: 2, samplesCount: 15 }, null, (n, i) => !p.isUndefined(i[n])), super({ readableHighWaterMark: e.chunkSize }); let a = this[ln] = { timeWindow: e.timeWindow, chunkSize: e.chunkSize, maxRate: e.maxRate, minChunkSize: e.minChunkSize, bytesSeen: 0, isCaptured: !1, notifiedBytesLoaded: 0, ts: Date.now(), bytes: 0, onReadCallback: null }; this.on("newListener", n => { n === "progress" && (a.isCaptured || (a.isCaptured = !0)) }) } _read(e) { let a = this[ln]; return a.onReadCallback && a.onReadCallback(), super._read(e) } _transform(e, a, n) { let i = this[ln], o = i.maxRate, s = this.readableHighWaterMark, r = i.timeWindow, u = 1e3 / r, l = o / u, c = i.minChunkSize !== !1 ? Math.max(i.minChunkSize, l * .01) : 0, d = (v, f) => { let h = Buffer.byteLength(v); i.bytesSeen += h, i.bytes += h, i.isCaptured && this.emit("progress", i.bytesSeen), this.push(v) ? process.nextTick(f) : i.onReadCallback = () => { i.onReadCallback = null, process.nextTick(f) } }, m = (v, f) => { let h = Buffer.byteLength(v), x = null, S = s, k, C = 0; if (o) { let O = Date.now(); (!i.ts || (C = O - i.ts) >= r) && (i.ts = O, k = l - i.bytes, i.bytes = k < 0 ? -k : 0, C = 0), k = l - i.bytes } if (o) { if (k <= 0) return setTimeout(() => { f(null, v) }, r - C); k < S && (S = k) } S && h > S && h - S > c && (x = v.subarray(S), v = v.subarray(0, S)), d(v, x ? () => { process.nextTick(f, null, x) } : f) }; m(e, function v(f, h) { if (f) return n(f); h ? m(h, v) : n(null) }) } }, dn = un; var Ys = require("events"); var Ls = L(require("util"), 1), Is = require("stream"); var { asyncIterator: Ps } = Symbol, Yl = async function* (t) { t.stream ? yield* t.stream() : t.arrayBuffer ? yield await t.arrayBuffer() : t[Ps] ? yield* t[Ps]() : yield t }, Qt = Yl; var Xl = R.ALPHABET.ALPHA_DIGIT + "-_", Et = typeof TextEncoder == "function" ? new TextEncoder : new Ls.default.TextEncoder, je = `\r
`, Ql = Et.encode(je), Zl = 2, mn = class { constructor(e, a) { let { escapeName: n } = this.constructor, i = p.isString(a), o = `Content-Disposition: form-data; name="${n(e)}"${!i && a.name ? `; filename="${n(a.name)}"` : ""}${je}`; i ? a = Et.encode(String(a).replace(/\r?\n|\r\n?/g, je)) : o += `Content-Type: ${a.type || "application/octet-stream"}${je}`, this.headers = Et.encode(o + je), this.contentLength = i ? a.byteLength : a.size, this.size = this.headers.byteLength + this.contentLength + Zl, this.name = e, this.value = a } async*encode() { yield this.headers; let { value: e } = this; p.isTypedArray(e) ? yield e : yield* Qt(e), yield Ql } static escapeName(e) { return String(e).replace(/[\r\n"]/g, a => ({ "\r": "%0D", "\n": "%0A", '"': "%22" })[a]) } }, eu = (t, e, a) => { let { tag: n = "form-data-boundary", size: i = 25, boundary: o = n + "-" + R.generateString(i, Xl) } = a || {}; if (!p.isFormData(t)) throw TypeError("FormData instance required"); if (o.length < 1 || o.length > 70) throw Error("boundary must be 10-70 characters long"); let s = Et.encode("--" + o + je), r = Et.encode("--" + o + "--" + je), u = r.byteLength, l = Array.from(t.entries()).map(([d, m]) => { let v = new mn(d, m); return u += v.size, v }); u += s.byteLength * l.length, u = p.toFiniteNumber(u); let c = { "Content-Type": `multipart/form-data; boundary=${o}` }; return Number.isFinite(u) && (c["Content-Length"] = u), e && e(c), Is.Readable.from((async function* () { for (let d of l) yield s, yield* d.encode(); yield r })()) }, Ds = eu; var Fs = L(require("stream"), 1), fn = class extends Fs.default.Transform { __transform(e, a, n) { this.push(e), n() } _transform(e, a, n) { if (e.length !== 0 && (this._transform = this.__transform, e[0] !== 120)) { let i = Buffer.alloc(2); i[0] = 120, i[1] = 156, this.push(i, a) } this.__transform(e, a, n) } }, Ns = fn; var tu = (t, e) => p.isAsyncFn(t) ? function (...a) { let n = a.pop(); t.apply(this, a).then(i => { try { e ? n(null, ...e(i)) : n(null, i) } catch (o) { n(o) } }, n) } : t, qs = tu; function au(t, e) { t = t || 10; let a = new Array(t), n = new Array(t), i = 0, o = 0, s; return e = e !== void 0 ? e : 1e3, function (u) { let l = Date.now(), c = n[o]; s || (s = l), a[i] = u, n[i] = l; let d = o, m = 0; for (; d !== i;)m += a[d++], d = d % t; if (i = (i + 1) % t, i === o && (o = (o + 1) % t), l - s < e) return; let v = c && l - c; return v ? Math.round(m * 1e3 / v) : void 0 } } var Us = au; function nu(t, e) { let a = 0, n = 1e3 / e, i, o, s = (l, c = Date.now()) => { a = c, i = null, o && (clearTimeout(o), o = null), t(...l) }; return [(...l) => { let c = Date.now(), d = c - a; d >= n ? s(l, c) : (i = l, o || (o = setTimeout(() => { o = null, s(i) }, n - d))) }, () => i && s(i)] } var Bs = nu; var me = (t, e, a = 3) => { let n = 0, i = Us(50, 250); return Bs(o => { let s = o.loaded, r = o.lengthComputable ? o.total : void 0, u = s - n, l = i(u), c = s <= r; n = s; let d = { loaded: s, total: r, progress: r ? s / r : void 0, bytes: u, rate: l || void 0, estimated: l && r && c ? (r - s) / l : void 0, event: o, lengthComputable: r != null, [e ? "download" : "upload"]: !0 }; t(d) }, a) }, Qe = (t, e) => { let a = t != null; return [n => e[0]({ lengthComputable: a, total: t, loaded: n }), e[1]] }, Ze = t => (...e) => p.asap(() => t(...e)); function xn(t) { if (!t || typeof t != "string" || !t.startsWith("data:")) return 0; let e = t.indexOf(","); if (e < 0) return 0; let a = t.slice(5, e), n = t.slice(e + 1); if (/;base64/i.test(a)) { let o = n.length, s = n.length; for (let m = 0; m < s; m++)if (n.charCodeAt(m) === 37 && m + 2 < s) { let v = n.charCodeAt(m + 1), f = n.charCodeAt(m + 2); (v >= 48 && v <= 57 || v >= 65 && v <= 70 || v >= 97 && v <= 102) && (f >= 48 && f <= 57 || f >= 65 && f <= 70 || f >= 97 && f <= 102) && (o -= 2, m += 2) } let r = 0, u = s - 1, l = m => m >= 2 && n.charCodeAt(m - 2) === 37 && n.charCodeAt(m - 1) === 51 && (n.charCodeAt(m) === 68 || n.charCodeAt(m) === 100); u >= 0 && (n.charCodeAt(u) === 61 ? (r++, u--) : l(u) && (r++, u -= 3)), r === 1 && u >= 0 && (n.charCodeAt(u) === 61 || l(u)) && r++; let d = Math.floor(o / 4) * 3 - (r || 0); return d > 0 ? d : 0 } return Buffer.byteLength(n, "utf8") } var Ms = { flush: xe.default.constants.Z_SYNC_FLUSH, finishFlush: xe.default.constants.Z_SYNC_FLUSH }, iu = { flush: xe.default.constants.BROTLI_OPERATION_FLUSH, finishFlush: xe.default.constants.BROTLI_OPERATION_FLUSH }, zs = p.isFunction(xe.default.createBrotliDecompress), { http: ou, https: su } = Ks.default, ru = /https:?/, Hs = R.protocols.map(t => t + ":"), $s = (t, [e, a]) => (t.on("end", a).on("error", a), e), hn = class { constructor() { this.sessions = Object.create(null) } getSession(e, a) { a = Object.assign({ sessionTimeout: 1e3 }, a); let n = this.sessions[e]; if (n) { let c = n.length; for (let d = 0; d < c; d++) { let [m, v] = n[d]; if (!m.destroyed && !m.closed && bn.default.isDeepStrictEqual(v, a)) return m } } let i = vn.default.connect(e, a), o, s = () => { if (o) return; o = !0; let c = n, d = c.length, m = d; for (; m--;)if (c[m][0] === i) { d === 1 ? delete this.sessions[e] : c.splice(m, 1); return } }, r = i.request, { sessionTimeout: u } = a; if (u != null) { let c, d = 0; i.request = function () { let m = r.apply(this, arguments); return d++, c && (clearTimeout(c), c = null), m.once("close", () => { --d || (c = setTimeout(() => { c = null, s() }, u)) }), m } } i.once("close", s); let l = [i, a]; return n ? n.push(l) : n = this.sessions[e] = [l], i } }, cu = new hn; function pu(t, e) { t.beforeRedirects.proxy && t.beforeRedirects.proxy(t), t.beforeRedirects.config && t.beforeRedirects.config(t, e) } function Xs(t, e, a) { let n = e; if (!n && n !== !1) { let i = Ws.default.getProxyForUrl(a); i && (n = new URL(i)) } if (n) { if (n.username && (n.auth = (n.username || "") + ":" + (n.password || "")), n.auth) { if (!!(n.auth.username || n.auth.password)) n.auth = (n.auth.username || "") + ":" + (n.auth.password || ""); else if (typeof n.auth == "object") throw new b("Invalid proxy authorization", b.ERR_BAD_OPTION, { proxy: n }); let s = Buffer.from(n.auth, "utf8").toString("base64"); t.headers["Proxy-Authorization"] = "Basic " + s } t.headers.host = t.hostname + (t.port ? ":" + t.port : ""); let i = n.hostname || n.host; t.hostname = i, t.host = i, t.port = n.port, t.path = a, n.protocol && (t.protocol = n.protocol.includes(":") ? n.protocol : `${n.protocol}:`) } t.beforeRedirects.proxy = function (o) { Xs(o, e, o.href) } } var lu = typeof process < "u" && p.kindOf(process) === "process", uu = t => new Promise((e, a) => { let n, i, o = (u, l) => { i || (i = !0, n && n(u, l)) }, s = u => { o(u), e(u) }, r = u => { o(u, !0), a(u) }; t(s, r, u => n = u).catch(r) }), du = ({ address: t, family: e }) => { if (!p.isString(t)) throw TypeError("address must be a string"); return { address: t, family: e || (t.indexOf(".") < 0 ? 6 : 4) } }, Gs = (t, e) => du(p.isObject(t) ? t : { address: t, family: e }), mu = { request(t, e) { let a = t.protocol + "//" + t.hostname + ":" + (t.port || (t.protocol === "https:" ? 443 : 80)), { http2Options: n, headers: i } = t, o = cu.getSession(a, n), { HTTP2_HEADER_SCHEME: s, HTTP2_HEADER_METHOD: r, HTTP2_HEADER_PATH: u, HTTP2_HEADER_STATUS: l } = vn.default.constants, c = { [s]: t.protocol.replace(":", ""), [r]: t.method, [u]: t.path }; p.forEach(i, (m, v) => { v.charAt(0) !== ":" && (c[v] = m) }); let d = o.request(c); return d.once("response", m => { let v = d; m = Object.assign({}, m); let f = m[l]; delete m[l], v.headers = m, v.statusCode = +f, e(v) }), d } }, Qs = lu && function (e) { return uu(async function (n, i, o) { let { data: s, lookup: r, family: u, httpVersion: l = 1, http2Options: c } = e, { responseType: d, responseEncoding: m } = e, v = e.method.toUpperCase(), f, h = !1, x; if (l = +l, Number.isNaN(l)) throw TypeError(`Invalid protocol version: '${e.httpVersion}' is not a number`); if (l !== 1 && l !== 2) throw TypeError(`Unsupported protocol version '${l}'`); let S = l === 2; if (r) { let w = qs(r, y => p.isArray(y) ? y : [y]); r = (y, A, ee) => { w(y, A, (D, ne, ve) => { if (D) return ee(D); let V = p.isArray(ne) ? ne.map(Rt => Gs(Rt)) : [Gs(ne, ve)]; A.all ? ee(D, V) : ee(D, V[0].address, V[0].family) }) } } let k = new Ys.EventEmitter; function C(w) { try { k.emit("abort", !w || w.type ? new te(null, e, x) : w) } catch (y) { console.warn("emit error", y) } } k.once("abort", i); let O = () => { e.cancelToken && e.cancelToken.unsubscribe(C), e.signal && e.signal.removeEventListener("abort", C), k.removeAllListeners() }; (e.cancelToken || e.signal) && (e.cancelToken && e.cancelToken.subscribe(C), e.signal && (e.signal.aborted ? C() : e.signal.addEventListener("abort", C))), o((w, y) => { if (f = !0, y) { h = !0, O(); return } let { data: A } = w; if (A instanceof fe.default.Readable || A instanceof fe.default.Duplex) { let ee = fe.default.finished(A, () => { ee(), O() }) } else O() }); let H = ke(e.baseURL, e.url, e.allowAbsoluteUrls), j = new URL(H, R.hasBrowserEnv ? R.origin : void 0), F = j.protocol || Hs[0]; if (F === "data:") { if (e.maxContentLength > -1) { let y = String(e.url || H || ""); if (xn(y) > e.maxContentLength) return i(new b("maxContentLength size of " + e.maxContentLength + " exceeded", b.ERR_BAD_RESPONSE, e)) } let w; if (v !== "GET") return pe(n, i, { status: 405, statusText: "method not allowed", headers: {}, config: e }); try { w = pn(e.url, d === "blob", { Blob: e.env && e.env.Blob }) } catch (y) { throw b.from(y, b.ERR_BAD_REQUEST, e) } return d === "text" ? (w = w.toString(m), (!m || m === "utf8") && (w = p.stripBOM(w))) : d === "stream" && (w = fe.default.Readable.from(w)), pe(n, i, { data: w, status: 200, statusText: "OK", headers: new P, config: e }) } if (Hs.indexOf(F) === -1) return i(new b("Unsupported protocol " + F, b.ERR_BAD_REQUEST, e)); let N = P.from(e.headers).normalize(); N.set("User-Agent", "axios/" + Oe, !1); let { onUploadProgress: re, onDownloadProgress: Pe } = e, Se = e.maxRate, ue, ce; if (p.isSpecCompliantForm(s)) { let w = N.getContentType(/boundary=([-_\w\d]{10,70})/i); s = Ds(s, y => { N.set(y) }, { tag: `axios-${Oe}-boundary`, boundary: w && w[1] || void 0 }) } else if (p.isFormData(s) && p.isFunction(s.getHeaders)) { if (N.set(s.getHeaders()), !N.hasContentLength()) try { let w = await bn.default.promisify(s.getLength).call(s); Number.isFinite(w) && w >= 0 && N.setContentLength(w) } catch { } } else if (p.isBlob(s) || p.isFile(s)) s.size && N.setContentType(s.type || "application/octet-stream"), N.setContentLength(s.size || 0), s = fe.default.Readable.from(Qt(s)); else if (s && !p.isStream(s)) { if (!Buffer.isBuffer(s)) if (p.isArrayBuffer(s)) s = Buffer.from(new Uint8Array(s)); else if (p.isString(s)) s = Buffer.from(s, "utf-8"); else return i(new b("Data after transformation must be a string, an ArrayBuffer, a Buffer, or a Stream", b.ERR_BAD_REQUEST, e)); if (N.setContentLength(s.length, !1), e.maxBodyLength > -1 && s.length > e.maxBodyLength) return i(new b("Request body larger than maxBodyLength limit", b.ERR_BAD_REQUEST, e)) } let de = p.toFiniteNumber(N.getContentLength()); p.isArray(Se) ? (ue = Se[0], ce = Se[1]) : ue = ce = Se, s && (re || ue) && (p.isStream(s) || (s = fe.default.Readable.from(s, { objectMode: !1 })), s = fe.default.pipeline([s, new dn({ maxRate: p.toFiniteNumber(ue) })], p.noop), re && s.on("progress", $s(s, Qe(de, me(Ze(re), !1, 3))))); let he; if (e.auth) { let w = e.auth.username || "", y = e.auth.password || ""; he = w + ":" + y } if (!he && j.username) { let w = j.username, y = j.password; he = w + ":" + y } he && N.delete("authorization"); let Z; try { Z = _e(j.pathname + j.search, e.params, e.paramsSerializer).replace(/^\?/, "") } catch (w) { let y = new Error(w.message); return y.config = e, y.url = e.url, y.exists = !0, i(y) } N.set("Accept-Encoding", "gzip, compress, deflate" + (zs ? ", br" : ""), !1); let z = { path: Z, method: v, headers: N.toJSON(), agents: { http: e.httpAgent, https: e.httpsAgent }, auth: he, protocol: F, family: u, beforeRedirect: pu, beforeRedirects: {}, http2Options: c }; !p.isUndefined(r) && (z.lookup = r), e.socketPath ? z.socketPath = e.socketPath : (z.hostname = j.hostname.startsWith("[") ? j.hostname.slice(1, -1) : j.hostname, z.port = j.port, Xs(z, e.proxy, F + "//" + j.hostname + (j.port ? ":" + j.port : "") + z.path)); let W, Le = ru.test(z.protocol); if (z.agent = Le ? e.httpsAgent : e.httpAgent, S ? W = mu : e.transport ? W = e.transport : e.maxRedirects === 0 ? W = Le ? Js.default : Vs.default : (e.maxRedirects && (z.maxRedirects = e.maxRedirects), e.beforeRedirect && (z.beforeRedirects.config = e.beforeRedirect), W = Le ? su : ou), e.maxBodyLength > -1 ? z.maxBodyLength = e.maxBodyLength : z.maxBodyLength = 1 / 0, e.insecureHTTPParser && (z.insecureHTTPParser = e.insecureHTTPParser), x = W.request(z, function (y) { if (x.destroyed) return; let A = [y], ee = p.toFiniteNumber(y.headers["content-length"]); if (Pe || ce) { let V = new dn({ maxRate: p.toFiniteNumber(ce) }); Pe && V.on("progress", $s(V, Qe(ee, me(Ze(Pe), !0, 3)))), A.push(V) } let D = y, ne = y.req || x; if (e.decompress !== !1 && y.headers["content-encoding"]) switch ((v === "HEAD" || y.statusCode === 204) && delete y.headers["content-encoding"], (y.headers["content-encoding"] || "").toLowerCase()) { case "gzip": case "x-gzip": case "compress": case "x-compress": A.push(xe.default.createUnzip(Ms)), delete y.headers["content-encoding"]; break; case "deflate": A.push(new Ns), A.push(xe.default.createUnzip(Ms)), delete y.headers["content-encoding"]; break; case "br": zs && (A.push(xe.default.createBrotliDecompress(iu)), delete y.headers["content-encoding"]) }D = A.length > 1 ? fe.default.pipeline(A, p.noop) : A[0]; let ve = { status: y.statusCode, statusText: y.statusMessage, headers: new P(y.headers), config: e, request: ne }; if (d === "stream") ve.data = D, pe(n, i, ve); else { let V = [], Rt = 0; D.on("data", function ($) { V.push($), Rt += $.length, e.maxContentLength > -1 && Rt > e.maxContentLength && (h = !0, D.destroy(), C(new b("maxContentLength size of " + e.maxContentLength + " exceeded", b.ERR_BAD_RESPONSE, e, ne))) }), D.on("aborted", function () { if (h) return; let $ = new b("stream has been aborted", b.ERR_BAD_RESPONSE, e, ne); D.destroy($), i($) }), D.on("error", function ($) { x.destroyed || i(b.from($, null, e, ne)) }), D.on("end", function () { try { let $ = V.length === 1 ? V[0] : Buffer.concat(V); d !== "arraybuffer" && ($ = $.toString(m), (!m || m === "utf8") && ($ = p.stripBOM($))), ve.data = $ } catch ($) { return i(b.from($, null, e, ve.request, ve)) } pe(n, i, ve) }) } k.once("abort", V => { D.destroyed || (D.emit("error", V), D.destroy()) }) }), k.once("abort", w => { x.close ? x.close() : x.destroy(w) }), x.on("error", function (y) { i(b.from(y, null, e, x)) }), x.on("socket", function (y) { y.setKeepAlive(!0, 1e3 * 60) }), e.timeout) { let w = parseInt(e.timeout, 10); if (Number.isNaN(w)) { C(new b("error trying to parse `config.timeout` to int", b.ERR_BAD_OPTION_VALUE, e, x)); return } x.setTimeout(w, function () { if (f) return; let A = e.timeout ? "timeout of " + e.timeout + "ms exceeded" : "timeout exceeded", ee = e.transitional || Ge; e.timeoutErrorMessage && (A = e.timeoutErrorMessage), C(new b(A, ee.clarifyTimeoutError ? b.ETIMEDOUT : b.ECONNABORTED, e, x)) }) } else x.setTimeout(0); if (p.isStream(s)) { let w = !1, y = !1; s.on("end", () => { w = !0 }), s.once("error", A => { y = !0, x.destroy(A) }), s.on("close", () => { !w && !y && C(new te("Request stream has been aborted", e, x)) }), s.pipe(x) } else s && x.write(s), x.end() }) }; var Zs = R.hasStandardBrowserEnv ? ((t, e) => a => (a = new URL(a, R.origin), t.protocol === a.protocol && t.host === a.host && (e || t.port === a.port)))(new URL(R.origin), R.navigator && /(msie|trident)/i.test(R.navigator.userAgent)) : () => !0; var er = R.hasStandardBrowserEnv ? { write(t, e, a, n, i, o, s) { if (typeof document > "u") return; let r = [`${t}=${encodeURIComponent(e)}`]; p.isNumber(a) && r.push(`expires=${new Date(a).toUTCString()}`), p.isString(n) && r.push(`path=${n}`), p.isString(i) && r.push(`domain=${i}`), o === !0 && r.push("secure"), p.isString(s) && r.push(`SameSite=${s}`), document.cookie = r.join("; ") }, read(t) { if (typeof document > "u") return null; let e = document.cookie.match(new RegExp("(?:^|; )" + t + "=([^;]*)")); return e ? decodeURIComponent(e[1]) : null }, remove(t) { this.write(t, "", Date.now() - 864e5, "/") } } : { write() { }, read() { return null }, remove() { } }; var tr = t => t instanceof P ? { ...t } : t; function se(t, e) { e = e || {}; let a = {}; function n(l, c, d, m) { return p.isPlainObject(l) && p.isPlainObject(c) ? p.merge.call({ caseless: m }, l, c) : p.isPlainObject(c) ? p.merge({}, c) : p.isArray(c) ? c.slice() : c } function i(l, c, d, m) { if (p.isUndefined(c)) { if (!p.isUndefined(l)) return n(void 0, l, d, m) } else return n(l, c, d, m) } function o(l, c) { if (!p.isUndefined(c)) return n(void 0, c) } function s(l, c) { if (p.isUndefined(c)) { if (!p.isUndefined(l)) return n(void 0, l) } else return n(void 0, c) } function r(l, c, d) { if (d in e) return n(l, c); if (d in t) return n(void 0, l) } let u = { url: o, method: o, data: o, baseURL: s, transformRequest: s, transformResponse: s, paramsSerializer: s, timeout: s, timeoutMessage: s, withCredentials: s, withXSRFToken: s, adapter: s, responseType: s, xsrfCookieName: s, xsrfHeaderName: s, onUploadProgress: s, onDownloadProgress: s, decompress: s, maxContentLength: s, maxBodyLength: s, beforeRedirect: s, transport: s, httpAgent: s, httpsAgent: s, cancelToken: s, socketPath: s, responseEncoding: s, validateStatus: r, headers: (l, c, d) => i(tr(l), tr(c), d, !0) }; return p.forEach(Object.keys({ ...t, ...e }), function (c) { let d = u[c] || i, m = d(t[c], e[c], c); p.isUndefined(m) && d !== r || (a[c] = m) }), a } var Zt = t => { let e = se({}, t), { data: a, withXSRFToken: n, xsrfHeaderName: i, xsrfCookieName: o, headers: s, auth: r } = e; if (e.headers = s = P.from(s), e.url = _e(ke(e.baseURL, e.url, e.allowAbsoluteUrls), t.params, t.paramsSerializer), r && s.set("Authorization", "Basic " + btoa((r.username || "") + ":" + (r.password ? unescape(encodeURIComponent(r.password)) : ""))), p.isFormData(a)) { if (R.hasStandardBrowserEnv || R.hasStandardBrowserWebWorkerEnv) s.setContentType(void 0); else if (p.isFunction(a.getHeaders)) { let u = a.getHeaders(), l = ["content-type", "content-length"]; Object.entries(u).forEach(([c, d]) => { l.includes(c.toLowerCase()) && s.set(c, d) }) } } if (R.hasStandardBrowserEnv && (n && p.isFunction(n) && (n = n(e)), n || n !== !1 && Zs(e.url))) { let u = i && o && er.read(o); u && s.set(i, u) } return e }; var fu = typeof XMLHttpRequest < "u", ar = fu && function (t) { return new Promise(function (a, n) { let i = Zt(t), o = i.data, s = P.from(i.headers).normalize(), { responseType: r, onUploadProgress: u, onDownloadProgress: l } = i, c, d, m, v, f; function h() { v && v(), f && f(), i.cancelToken && i.cancelToken.unsubscribe(c), i.signal && i.signal.removeEventListener("abort", c) } let x = new XMLHttpRequest; x.open(i.method.toUpperCase(), i.url, !0), x.timeout = i.timeout; function S() { if (!x) return; let C = P.from("getAllResponseHeaders" in x && x.getAllResponseHeaders()), H = { data: !r || r === "text" || r === "json" ? x.responseText : x.response, status: x.status, statusText: x.statusText, headers: C, config: t, request: x }; pe(function (F) { a(F), h() }, function (F) { n(F), h() }, H), x = null } "onloadend" in x ? x.onloadend = S : x.onreadystatechange = function () { !x || x.readyState !== 4 || x.status === 0 && !(x.responseURL && x.responseURL.indexOf("file:") === 0) || setTimeout(S) }, x.onabort = function () { x && (n(new b("Request aborted", b.ECONNABORTED, t, x)), x = null) }, x.onerror = function (O) { let H = O && O.message ? O.message : "Network Error", j = new b(H, b.ERR_NETWORK, t, x); j.event = O || null, n(j), x = null }, x.ontimeout = function () { let O = i.timeout ? "timeout of " + i.timeout + "ms exceeded" : "timeout exceeded", H = i.transitional || Ge; i.timeoutErrorMessage && (O = i.timeoutErrorMessage), n(new b(O, H.clarifyTimeoutError ? b.ETIMEDOUT : b.ECONNABORTED, t, x)), x = null }, o === void 0 && s.setContentType(null), "setRequestHeader" in x && p.forEach(s.toJSON(), function (O, H) { x.setRequestHeader(H, O) }), p.isUndefined(i.withCredentials) || (x.withCredentials = !!i.withCredentials), r && r !== "json" && (x.responseType = i.responseType), l && ([m, f] = me(l, !0), x.addEventListener("progress", m)), u && x.upload && ([d, v] = me(u), x.upload.addEventListener("progress", d), x.upload.addEventListener("loadend", v)), (i.cancelToken || i.signal) && (c = C => { x && (n(!C || C.type ? new te(null, t, x) : C), x.abort(), x = null) }, i.cancelToken && i.cancelToken.subscribe(c), i.signal && (i.signal.aborted ? c() : i.signal.addEventListener("abort", c))); let k = St(i.url); if (k && R.protocols.indexOf(k) === -1) { n(new b("Unsupported protocol " + k + ":", b.ERR_BAD_REQUEST, t)); return } x.send(o || null) }) }; var xu = (t, e) => { let { length: a } = t = t ? t.filter(Boolean) : []; if (e || a) { let n = new AbortController, i, o = function (l) { if (!i) { i = !0, r(); let c = l instanceof Error ? l : this.reason; n.abort(c instanceof b ? c : new te(c instanceof Error ? c.message : c)) } }, s = e && setTimeout(() => { s = null, o(new b(`timeout of ${e}ms exceeded`, b.ETIMEDOUT)) }, e), r = () => { t && (s && clearTimeout(s), s = null, t.forEach(l => { l.unsubscribe ? l.unsubscribe(o) : l.removeEventListener("abort", o) }), t = null) }; t.forEach(l => l.addEventListener("abort", o)); let { signal: u } = n; return u.unsubscribe = () => p.asap(r), u } }, nr = xu; var hu = function* (t, e) { let a = t.byteLength; if (!e || a < e) { yield t; return } let n = 0, i; for (; n < a;)i = n + e, yield t.slice(n, i), n = i }, vu = async function* (t, e) { for await (let a of bu(t)) yield* hu(a, e) }, bu = async function* (t) { if (t[Symbol.asyncIterator]) { yield* t; return } let e = t.getReader(); try { for (; ;) { let { done: a, value: n } = await e.read(); if (a) break; yield n } } finally { await e.cancel() } }, gn = (t, e, a, n) => { let i = vu(t, e), o = 0, s, r = u => { s || (s = !0, n && n(u)) }; return new ReadableStream({ async pull(u) { try { let { done: l, value: c } = await i.next(); if (l) { r(), u.close(); return } let d = c.byteLength; if (a) { let m = o += d; a(m) } u.enqueue(new Uint8Array(c)) } catch (l) { throw r(l), l } }, cancel(u) { return r(u), i.return() } }, { highWaterMark: 2 }) }; var ir = 64 * 1024, { isFunction: ea } = p, gu = (({ Request: t, Response: e }) => ({ Request: t, Response: e }))(p.global), { ReadableStream: or, TextEncoder: sr } = p.global, rr = (t, ...e) => { try { return !!t(...e) } catch { return !1 } }, yu = t => { t = p.merge.call({ skipUndefined: !0 }, gu, t); let { fetch: e, Request: a, Response: n } = t, i = e ? ea(e) : typeof fetch == "function", o = ea(a), s = ea(n); if (!i) return !1; let r = i && ea(or), u = i && (typeof sr == "function" ? (f => h => f.encode(h))(new sr) : async f => new Uint8Array(await new a(f).arrayBuffer())), l = o && r && rr(() => { let f = !1, h = new a(R.origin, { body: new or, method: "POST", get duplex() { return f = !0, "half" } }).headers.has("Content-Type"); return f && !h }), c = s && r && rr(() => p.isReadableStream(new n("").body)), d = { stream: c && (f => f.body) }; i && ["text", "arrayBuffer", "blob", "formData", "stream"].forEach(f => { !d[f] && (d[f] = (h, x) => { let S = h && h[f]; if (S) return S.call(h); throw new b(`Response type '${f}' is not supported`, b.ERR_NOT_SUPPORT, x) }) }); let m = async f => { if (f == null) return 0; if (p.isBlob(f)) return f.size; if (p.isSpecCompliantForm(f)) return (await new a(R.origin, { method: "POST", body: f }).arrayBuffer()).byteLength; if (p.isArrayBufferView(f) || p.isArrayBuffer(f)) return f.byteLength; if (p.isURLSearchParams(f) && (f = f + ""), p.isString(f)) return (await u(f)).byteLength }, v = async (f, h) => { let x = p.toFiniteNumber(f.getContentLength()); return x ?? m(h) }; return async f => { let { url: h, method: x, data: S, signal: k, cancelToken: C, timeout: O, onDownloadProgress: H, onUploadProgress: j, responseType: F, headers: N, withCredentials: re = "same-origin", fetchOptions: Pe } = Zt(f), Se = e || fetch; F = F ? (F + "").toLowerCase() : "text"; let ue = nr([k, C && C.toAbortSignal()], O), ce = null, de = ue && ue.unsubscribe && (() => { ue.unsubscribe() }), he; try { if (j && l && x !== "get" && x !== "head" && (he = await v(N, S)) !== 0) { let y = new a(h, { method: "POST", body: S, duplex: "half" }), A; if (p.isFormData(S) && (A = y.headers.get("content-type")) && N.setContentType(A), y.body) { let [ee, D] = Qe(he, me(Ze(j))); S = gn(y.body, ir, ee, D) } } p.isString(re) || (re = re ? "include" : "omit"); let Z = o && "credentials" in a.prototype, z = { ...Pe, signal: ue, method: x.toUpperCase(), headers: N.normalize().toJSON(), body: S, duplex: "half", credentials: Z ? re : void 0 }; ce = o && new a(h, z); let W = await (o ? Se(ce, Pe) : Se(h, z)), Le = c && (F === "stream" || F === "response"); if (c && (H || Le && de)) { let y = {};["status", "statusText", "headers"].forEach(ne => { y[ne] = W[ne] }); let A = p.toFiniteNumber(W.headers.get("content-length")), [ee, D] = H && Qe(A, me(Ze(H), !0)) || []; W = new n(gn(W.body, ir, ee, () => { D && D(), de && de() }), y) } F = F || "text"; let w = await d[p.findKey(d, F) || "text"](W, f); return !Le && de && de(), await new Promise((y, A) => { pe(y, A, { data: w, headers: P.from(W.headers), status: W.status, statusText: W.statusText, config: f, request: ce }) }) } catch (Z) { throw de && de(), Z && Z.name === "TypeError" && /Load failed|fetch/i.test(Z.message) ? Object.assign(new b("Network Error", b.ERR_NETWORK, f, ce), { cause: Z.cause || Z }) : b.from(Z, Z && Z.code, f, ce) } } }, wu = new Map, yn = t => { let e = t && t.env || {}, { fetch: a, Request: n, Response: i } = e, o = [n, i, a], s = o.length, r = s, u, l, c = wu; for (; r--;)u = o[r], l = c.get(u), l === void 0 && c.set(u, l = r ? new Map : yu(e)), c = l; return l }, Tx = yn(); var wn = { http: Qs, xhr: ar, fetch: { get: yn } }; p.forEach(wn, (t, e) => { if (t) { try { Object.defineProperty(t, "name", { value: e }) } catch { } Object.defineProperty(t, "adapterName", { value: e }) } }); var cr = t => `- ${t}`, Eu = t => p.isFunction(t) || t === null || t === !1; function Cu(t, e) {
	t = p.isArray(t) ? t : [t]; let { length: a } = t, n, i, o = {}; for (let s = 0; s < a; s++) { n = t[s]; let r; if (i = n, !Eu(n) && (i = wn[(r = String(n)).toLowerCase()], i === void 0)) throw new b(`Unknown adapter '${r}'`); if (i && (p.isFunction(i) || (i = i.get(e)))) break; o[r || "#" + s] = i } if (!i) {
		let s = Object.entries(o).map(([u, l]) => `adapter ${u} ` + (l === !1 ? "is not supported by the environment" : "is not available in the build")), r = a ? s.length > 1 ? `since :
`+ s.map(cr).join(`
`) : " " + cr(s[0]) : "as no adapter specified"; throw new b("There is no suitable adapter to dispatch the request " + r, "ERR_NOT_SUPPORT")
	} return i
} var ta = { getAdapter: Cu, adapters: wn }; function Sn(t) { if (t.cancelToken && t.cancelToken.throwIfRequested(), t.signal && t.signal.aborted) throw new te(null, t) } function aa(t) { return Sn(t), t.headers = P.from(t.headers), t.data = ht.call(t, t.transformRequest), ["post", "put", "patch"].indexOf(t.method) !== -1 && t.headers.setContentType("application/x-www-form-urlencoded", !1), ta.getAdapter(t.adapter || We.adapter, t)(t).then(function (n) { return Sn(t), n.data = ht.call(t, t.transformResponse, n), n.headers = P.from(n.headers), n }, function (n) { return vt(n) || (Sn(t), n && n.response && (n.response.data = ht.call(t, t.transformResponse, n.response), n.response.headers = P.from(n.response.headers))), Promise.reject(n) }) } var na = {};["object", "boolean", "number", "function", "string", "symbol"].forEach((t, e) => { na[t] = function (n) { return typeof n === t || "a" + (e < 1 ? "n " : " ") + t } }); var pr = {}; na.transitional = function (e, a, n) { function i(o, s) { return "[Axios v" + Oe + "] Transitional option '" + o + "'" + s + (n ? ". " + n : "") } return (o, s, r) => { if (e === !1) throw new b(i(s, " has been removed" + (a ? " in " + a : "")), b.ERR_DEPRECATED); return a && !pr[s] && (pr[s] = !0, console.warn(i(s, " has been deprecated since v" + a + " and will be removed in the near future"))), e ? e(o, s, r) : !0 } }; na.spelling = function (e) { return (a, n) => (console.warn(`${n} is likely a misspelling of ${e}`), !0) }; function _u(t, e, a) { if (typeof t != "object") throw new b("options must be an object", b.ERR_BAD_OPTION_VALUE); let n = Object.keys(t), i = n.length; for (; i-- > 0;) { let o = n[i], s = e[o]; if (s) { let r = t[o], u = r === void 0 || s(r, o, t); if (u !== !0) throw new b("option " + o + " must be " + u, b.ERR_BAD_OPTION_VALUE); continue } if (a !== !0) throw new b("Unknown option " + o, b.ERR_BAD_OPTION) } } var Ct = { assertOptions: _u, validators: na }; var le = Ct.validators, et = class {
	constructor(e) { this.defaults = e || {}, this.interceptors = { request: new Fa, response: new Fa } } async request(e, a) {
		try { return await this._request(e, a) } catch (n) {
			if (n instanceof Error) {
				let i = {}; Error.captureStackTrace ? Error.captureStackTrace(i) : i = new Error; let o = i.stack ? i.stack.replace(/^.+\n/, "") : ""; try {
					n.stack ? o && !String(n.stack).endsWith(o.replace(/^.+\n.+\n/, "")) && (n.stack += `
`+ o) : n.stack = o
				} catch { }
			} throw n
		}
	} _request(e, a) { typeof e == "string" ? (a = a || {}, a.url = e) : a = e || {}, a = se(this.defaults, a); let { transitional: n, paramsSerializer: i, headers: o } = a; n !== void 0 && Ct.assertOptions(n, { silentJSONParsing: le.transitional(le.boolean), forcedJSONParsing: le.transitional(le.boolean), clarifyTimeoutError: le.transitional(le.boolean) }, !1), i != null && (p.isFunction(i) ? a.paramsSerializer = { serialize: i } : Ct.assertOptions(i, { encode: le.function, serialize: le.function }, !0)), a.allowAbsoluteUrls !== void 0 || (this.defaults.allowAbsoluteUrls !== void 0 ? a.allowAbsoluteUrls = this.defaults.allowAbsoluteUrls : a.allowAbsoluteUrls = !0), Ct.assertOptions(a, { baseUrl: le.spelling("baseURL"), withXsrfToken: le.spelling("withXSRFToken") }, !0), a.method = (a.method || this.defaults.method || "get").toLowerCase(); let s = o && p.merge(o.common, o[a.method]); o && p.forEach(["delete", "get", "head", "post", "put", "patch", "common"], f => { delete o[f] }), a.headers = P.concat(s, o); let r = [], u = !0; this.interceptors.request.forEach(function (h) { typeof h.runWhen == "function" && h.runWhen(a) === !1 || (u = u && h.synchronous, r.unshift(h.fulfilled, h.rejected)) }); let l = []; this.interceptors.response.forEach(function (h) { l.push(h.fulfilled, h.rejected) }); let c, d = 0, m; if (!u) { let f = [aa.bind(this), void 0]; for (f.unshift(...r), f.push(...l), m = f.length, c = Promise.resolve(a); d < m;)c = c.then(f[d++], f[d++]); return c } m = r.length; let v = a; for (; d < m;) { let f = r[d++], h = r[d++]; try { v = f(v) } catch (x) { h.call(this, x); break } } try { c = aa.call(this, v) } catch (f) { return Promise.reject(f) } for (d = 0, m = l.length; d < m;)c = c.then(l[d++], l[d++]); return c } getUri(e) { e = se(this.defaults, e); let a = ke(e.baseURL, e.url, e.allowAbsoluteUrls); return _e(a, e.params, e.paramsSerializer) }
}; p.forEach(["delete", "get", "head", "options"], function (e) { et.prototype[e] = function (a, n) { return this.request(se(n || {}, { method: e, url: a, data: (n || {}).data })) } }); p.forEach(["post", "put", "patch"], function (e) { function a(n) { return function (o, s, r) { return this.request(se(r || {}, { method: e, headers: n ? { "Content-Type": "multipart/form-data" } : {}, url: o, data: s })) } } et.prototype[e] = a(), et.prototype[e + "Form"] = a(!0) }); var _t = et; var En = class t { constructor(e) { if (typeof e != "function") throw new TypeError("executor must be a function."); let a; this.promise = new Promise(function (o) { a = o }); let n = this; this.promise.then(i => { if (!n._listeners) return; let o = n._listeners.length; for (; o-- > 0;)n._listeners[o](i); n._listeners = null }), this.promise.then = i => { let o, s = new Promise(r => { n.subscribe(r), o = r }).then(i); return s.cancel = function () { n.unsubscribe(o) }, s }, e(function (o, s, r) { n.reason || (n.reason = new te(o, s, r), a(n.reason)) }) } throwIfRequested() { if (this.reason) throw this.reason } subscribe(e) { if (this.reason) { e(this.reason); return } this._listeners ? this._listeners.push(e) : this._listeners = [e] } unsubscribe(e) { if (!this._listeners) return; let a = this._listeners.indexOf(e); a !== -1 && this._listeners.splice(a, 1) } toAbortSignal() { let e = new AbortController, a = n => { e.abort(n) }; return this.subscribe(a), e.signal.unsubscribe = () => this.unsubscribe(a), e.signal } static source() { let e; return { token: new t(function (i) { e = i }), cancel: e } } }, lr = En; function Cn(t) { return function (a) { return t.apply(null, a) } } function _n(t) { return p.isObject(t) && t.isAxiosError === !0 } var kn = { Continue: 100, SwitchingProtocols: 101, Processing: 102, EarlyHints: 103, Ok: 200, Created: 201, Accepted: 202, NonAuthoritativeInformation: 203, NoContent: 204, ResetContent: 205, PartialContent: 206, MultiStatus: 207, AlreadyReported: 208, ImUsed: 226, MultipleChoices: 300, MovedPermanently: 301, Found: 302, SeeOther: 303, NotModified: 304, UseProxy: 305, Unused: 306, TemporaryRedirect: 307, PermanentRedirect: 308, BadRequest: 400, Unauthorized: 401, PaymentRequired: 402, Forbidden: 403, NotFound: 404, MethodNotAllowed: 405, NotAcceptable: 406, ProxyAuthenticationRequired: 407, RequestTimeout: 408, Conflict: 409, Gone: 410, LengthRequired: 411, PreconditionFailed: 412, PayloadTooLarge: 413, UriTooLong: 414, UnsupportedMediaType: 415, RangeNotSatisfiable: 416, ExpectationFailed: 417, ImATeapot: 418, MisdirectedRequest: 421, UnprocessableEntity: 422, Locked: 423, FailedDependency: 424, TooEarly: 425, UpgradeRequired: 426, PreconditionRequired: 428, TooManyRequests: 429, RequestHeaderFieldsTooLarge: 431, UnavailableForLegalReasons: 451, InternalServerError: 500, NotImplemented: 501, BadGateway: 502, ServiceUnavailable: 503, GatewayTimeout: 504, HttpVersionNotSupported: 505, VariantAlsoNegotiates: 506, InsufficientStorage: 507, LoopDetected: 508, NotExtended: 510, NetworkAuthenticationRequired: 511, WebServerIsDown: 521, ConnectionTimedOut: 522, OriginIsUnreachable: 523, TimeoutOccurred: 524, SslHandshakeFailed: 525, InvalidSslCertificate: 526 }; Object.entries(kn).forEach(([t, e]) => { kn[e] = t }); var ur = kn; function dr(t) { let e = new _t(t), a = rt(_t.prototype.request, e); return p.extend(a, _t.prototype, e, { allOwnKeys: !0 }), p.extend(a, e, null, { allOwnKeys: !0 }), a.create = function (i) { return dr(se(t, i)) }, a } var q = dr(We); q.Axios = _t; q.CanceledError = te; q.CancelToken = lr; q.isCancel = vt; q.VERSION = Oe; q.toFormData = ye; q.AxiosError = b; q.Cancel = q.CanceledError; q.all = function (e) { return Promise.all(e) }; q.spread = Cn; q.isAxiosError = _n; q.mergeConfig = se; q.AxiosHeaders = P; q.formToJSON = t => Gt(p.isHTMLForm(t) ? new FormData(t) : t); q.getAdapter = ta.getAdapter; q.HttpStatusCode = ur; q.default = q; var ia = q; var { Axios: Eh, AxiosError: Ch, CanceledError: _h, isCancel: kh, CancelToken: Th, VERSION: Rh, all: Ah, Cancel: Oh, isAxiosError: jh, spread: Ph, toFormData: Lh, AxiosHeaders: Ih, HttpStatusCode: Dh, formToJSON: Fh, getAdapter: Nh, mergeConfig: qh } = ia; var mr = L(require("https")); var tt = class { constructor(e, a, n = 20) { this.delayMs = n; this.port = e, this.token = a, this.isLocalBrain = e === -1 || a === "agy-2.6-local-brain", this._defaultModel = null, this.client = ia.create({ baseURL: `https://127.0.0.1:${Math.max(1, e)}`, headers: { "X-Codeium-Csrf-Token": a, "Connect-Protocol-Version": "1", "Content-Type": "application/json" }, httpsAgent: new mr.Agent({ rejectUnauthorized: !1 }), timeout: Fe.HTTP_TIMEOUT_MS }) } _getBrainDir() { let e = require("os"), a = require("path"); return a.join(e.homedir(), ".gemini", "antigravity", "brain") } _detectDefaultModel() { if (this._defaultModel) return this._defaultModel; try { let e = require("fs"), a = require("path"), n = this._getBrainDir(); if (!e.existsSync(n)) return this._defaultModel = "Gemini (Antigravity)"; let i = e.readdirSync(n, { withFileTypes: !0 }).filter(o => o.isDirectory()).map(o => { let s = a.join(n, o.name, ".system_generated", "logs", "transcript.jsonl"); if (!e.existsSync(s)) return null; let r = e.statSync(s); return { path: s, mtimeMs: r.mtimeMs } }).filter(Boolean).sort((o, s) => s.mtimeMs - o.mtimeMs); for (let o of i.slice(0, 15)) { let s = e.readFileSync(o.path, "utf8"), r = /changed setting `Model Selection` from [^\r\n]+? to ([^\r\n<]+?)(?:\.\s+No need|\.\r?\n|\.<|\.$|$)/g, u, l = null; for (; (u = r.exec(s)) !== null;)l = u[1].trim(); if (l) return this._defaultModel = l } } catch { } return this._defaultModel = "Gemini (Antigravity)" } async makeRequest(e, a = {}) { this.delayMs > 0 && await new Promise(n => setTimeout(n, this.delayMs)), console.log(`[LSClient] Making request: ${e}`); try { let n = await this.client.post(`/${e}`, a); return console.log(`[LSClient] Success: ${e}`), n.data } catch (n) { throw console.error(`[LSClient] Request Error (${e}):`, n.message), n } } async getAllCascadeTrajectories() { if (this.isLocalBrain) { let e = require("fs"), a = require("path"), n = this._getBrainDir(), i = {}; if (!e.existsSync(n)) return { trajectorySummaries: i }; let o = e.readdirSync(n, { withFileTypes: !0 }); for (let s of o) { if (!s.isDirectory()) continue; let r = a.join(n, s.name, ".system_generated", "logs", "transcript.jsonl"); try { if (e.existsSync(r)) { let u = e.statSync(r); if (u.size > 0) { let l = new Date(Math.floor(u.mtimeMs / 1e3) * 1e3 + u.size % 1e3).toISOString(); let dateStr = ""; try { let d = new Date(u.mtimeMs); dateStr = new Intl.DateTimeFormat('pt-BR', { timeZone: 'America/Sao_Paulo', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }).format(d).replace(',', ''); } catch { dateStr = l.substring(5, 10) + " " + l.substring(11, 16); } let title = s.name.substring(0, 8); try { let fd = e.openSync(r, "r"); let buf = Buffer.alloc(Math.min(u.size, 8192)); let bytesRead = e.readSync(fd, buf, 0, buf.length, 0); e.closeSync(fd); let head = buf.toString("utf8", 0, bytesRead); let mReq = head.match(/<USER_REQUEST>([\s\S]*?)<\/USER_REQUEST>/); if (mReq && mReq[1]) { title = mReq[1].replace(/[\r\n\t]+/g, " ").trim().substring(0, 32); } else { let mInput = head.match(/"type"\s*:\s*"USER_INPUT"[^}]*?"content"\s*:\s*"([^"]+)"/); if (mInput && mInput[1]) { title = mInput[1].replace(/\\n|\\r|\\t/g, " ").trim().substring(0, 32); } } } catch {} i[s.name] = { cascadeId: s.name, summary: `${dateStr} - ${title}`, lastModifiedTime: l, stepCount: "1" }; } } } catch { } } return { trajectorySummaries: i } } return this.makeRequest(Ne.GET_ALL_CASCADE_TRAJECTORIES, {}) } async getCascadeMetadata(e, a = 0) { if (this.isLocalBrain) { let n = require("fs"), i = require("path"), o = i.join(this._getBrainDir(), e, ".system_generated", "logs", "transcript.jsonl"); if (!n.existsSync(o)) return { generatorMetadata: [] }; try { let s = n.statSync(o), r = s.mtime.toISOString(), u = n.readFileSync(o, "utf8").split(/\r?\n/), l = this._detectDefaultModel(), c = []; for (let d of u) { if (!d || !d.trim()) continue; try { let m = JSON.parse(d); if (typeof m.content == "string" && m.content.includes("Model Selection")) { let v = /changed setting `Model Selection` from [^\r\n]+? to ([^\r\n<]+?)(?:\.\s+No need|\.\r?\n|\.<|\.$|$)/g, f; for (; (f = v.exec(m.content)) !== null;)f[1] && (l = f[1].trim()) } if (m.type === "PLANNER_RESPONSE" || typeof m.input_tokens == "number" || typeof m.output_tokens == "number") { let v = Number(m.input_tokens) || 0, f = Number(m.output_tokens) || 0, h = Number(m.cache_read_tokens) || 0, x = v + h + f, S = m.created_at || r, k = m.model || m.model_name || l; c.push({ timestamp: S, chatModel: { model: k, usage: { model: k, inputTokens: v, outputTokens: f, cacheReadTokens: h, contextTokens: x }, chatStartMetadata: { createdAt: S, contextWindowMetadata: { estimatedTokensUsed: x } } } }) } } catch { } } return { generatorMetadata: c.slice(a) } } catch { return { generatorMetadata: [] } } } return this.makeRequest(Ne.GET_CASCADE_METADATA, { cascade_id: e, generator_metadata_offset: a }) } async getCascadeSteps(e, a = 0) { if (this.isLocalBrain) return { steps: [] }; return this.makeRequest(Ne.GET_CASCADE_STEPS, { cascade_id: e, step_offset: a }) } }; var oa = class {
	constructor() {
		this.cacheMap = new Map();
		this.cache = null;
	}
	isValid(e) {
		let t = e ? this.cacheMap.get(e) : this.cache;
		return !!t;
	}
	getEntry(e) {
		if (e) return this.cacheMap.get(e) || null;
		return this.cache;
	}
	get(e) {
		if (e) return this.cacheMap.get(e)?.stats || null;
		return this.cache?.stats || null;
	}
	set(e, a, n, i, o) {
		let entry = { cascadeId: e, lastModifiedTime: a, stats: n, timestamp: Date.now(), nextMetaOffset: i, nextStepOffset: o };
		this.cacheMap.set(e, entry);
		this.cache = entry;
	}
	invalidate(e) {
		if (e) this.cacheMap.delete(e);
		else {
			this.cacheMap.clear();
			this.cache = null;
		}
	}
	getInfo() {
		if (!this.cache) return "Cache: empty";
		let e = Math.round((Date.now() - this.cache.timestamp) / 1e3);
		return `Cache: ${this.cache.cascadeId.substring(0, 8)}... (${e}s old, ${this.cacheMap.size} sess\xF5es)`;
	}
}; var sa = { "Orchestrator (Argon)": { displayName: "Orchestrator (Argon)", color: "#7C3AED", order: 0 }, "gemini-pro-orchestrator-g1-pro": { displayName: "Orchestrator (Argon)", color: "#7C3AED", order: 0 }, "Gemini 4 Argon (High)": { displayName: "Gemini 4 Argon (High)", color: "#185ABC", order: 1 }, "Gemini 4 Argon (Medium)": { displayName: "Gemini 4 Argon (Medium)", color: "#1A73E8", order: 2 }, "Gemini 4 Argon (Low)": { displayName: "Gemini 4 Argon (Low)", color: "#4285F4", order: 3 }, "Gemini 4 Argon": { displayName: "Gemini 4 Argon", color: "#1A73E8", order: 4 }, "Gemini 4 Neon (Extra)": { displayName: "Gemini 4 Neon (Extra)", color: "#C2185B", order: 4.1 }, "Gemini 4 Neon (High)": { displayName: "Gemini 4 Neon (High)", color: "#E52592", order: 4.2 }, "Gemini 4 Neon (Medium)": { displayName: "Gemini 4 Neon (Medium)", color: "#F538A0", order: 4.3 }, "Gemini 4 Neon (Low)": { displayName: "Gemini 4 Neon (Low)", color: "#F48FB1", order: 4.4 }, "Gemini 4 Neon": { displayName: "Gemini 4 Neon", color: "#E52592", order: 4.5 }, "Gemini 3.8 Flash (High)": { displayName: "Gemini 3.8 Flash (High)", color: "#00ACC1", order: 5 }, "Gemini 3.8 Flash (Medium)": { displayName: "Gemini 3.8 Flash (Medium)", color: "#26C6DA", order: 6 }, "Gemini 3.8 Flash (Low)": { displayName: "Gemini 3.8 Flash (Low)", color: "#80DEEA", order: 7 }, "Gemini 3.8 Flash": { displayName: "Gemini 3.8 Flash", color: "#26C6DA", order: 8 }, "Claude Opus 5.5 (High)": { displayName: "Claude Opus 5.5 (High)", color: "#D93025", order: 9 }, "Claude Opus 5.5 (Medium)": { displayName: "Claude Opus 5.5 (Medium)", color: "#FF6B35", order: 10 }, "Claude Opus 5.5 (Low)": { displayName: "Claude Opus 5.5 (Low)", color: "#FF8A65", order: 11 }, "Claude Opus 5.5 (Thinking)": { displayName: "Claude Opus 5.5 (Thinking)", color: "#FF6B35", order: 12 }, "Claude Opus 5.5": { displayName: "Claude Opus 5.5", color: "#FF6B35", order: 13 }, "Claude Sonnet 5.5 (High)": { displayName: "Claude Sonnet 5.5 (High)", color: "#E37400", order: 14 }, "Claude Sonnet 5.5 (Medium)": { displayName: "Claude Sonnet 5.5 (Medium)", color: "#F9AB00", order: 15 }, "Claude Sonnet 5.5 (Low)": { displayName: "Claude Sonnet 5.5 (Low)", color: "#FDD663", order: 16 }, "Claude Sonnet 5.5 (Thinking)": { displayName: "Claude Sonnet 5.5 (Thinking)", color: "#E37400", order: 17 }, "Claude Sonnet 5.5": { displayName: "Claude Sonnet 5.5", color: "#F9AB00", order: 18 }, MODEL_OPENAI_GPT_OSS_120B_MEDIUM: { displayName: "GPT-OSS 120B (Medium)", color: "#10B981", order: 19 }, "GPT-OSS 120B (High)": { displayName: "GPT-OSS 120B (High)", color: "#059669", order: 20 }, "GPT-OSS 120B (Low)": { displayName: "GPT-OSS 120B (Low)", color: "#34D399", order: 21 }, MODEL_PLACEHOLDER_M18: { displayName: "Gemini 3 Flash", color: "#8AB4F8", order: 22 }, MODEL_PLACEHOLDER_M8: { displayName: "Gemini 3 Pro (High)", color: "#185ABC", order: 23 }, MODEL_PLACEHOLDER_M7: { displayName: "Gemini 3 Pro (Low)", color: "#1A73E8", order: 24 }, MODEL_PLACEHOLDER_M12: { displayName: "Claude Opus 4.5 (Thinking)", color: "#FF6B35", order: 25 }, MODEL_CLAUDE_4_5_SONNET_THINKING: { displayName: "Claude Sonnet 4.5 (Thinking)", color: "#E37400", order: 26 }, MODEL_CLAUDE_4_5_SONNET: { displayName: "Claude Sonnet 4.5", color: "#F9AB00", order: 27 }, MODEL_GOOGLE_GEMINI_2_5_FLASH: { displayName: "Gemini 2.5 Flash", color: "#4285F4", order: 28 }, MODEL_GOOGLE_GEMINI_2_5_FLASH_LITE: { displayName: "Gemini 2.5 Flash Lite", color: "#81C995", order: 29 }, "Gemini (Antigravity)": { displayName: "Gemini (Antigravity)", color: "#4285F4", order: 30 } }; var saSlugMap = { "orchestrator (argon)": "Orchestrator (Argon)", "orchestrator-argon": "Orchestrator (Argon)", "gemini-pro-orchestrator-g1-pro": "Orchestrator (Argon)", "gemini-pro-orchestrator": "Orchestrator (Argon)", "gemini-4-argon-high": "Gemini 4 Argon (High)", "gemini-4-argon-medium": "Gemini 4 Argon (Medium)", "gemini-4-argon-low": "Gemini 4 Argon (Low)", "gemini-4-argon": "Gemini 4 Argon", "gemini-4-neon-extra": "Gemini 4 Neon (Extra)", "gemini-4-neon-extra-high": "Gemini 4 Neon (Extra)", "gemini 4 neon (extra high)": "Gemini 4 Neon (Extra)", "gemini-4-neon-high": "Gemini 4 Neon (High)", "gemini-4-neon-medium": "Gemini 4 Neon (Medium)", "gemini-4-neon-low": "Gemini 4 Neon (Low)", "gemini-4-neon": "Gemini 4 Neon", "gemini-3.8-flash-high": "Gemini 3.8 Flash (High)", "gemini-3.8-flash-medium": "Gemini 3.8 Flash (Medium)", "gemini-3.8-flash-low": "Gemini 3.8 Flash (Low)", "gemini-3.8-flash": "Gemini 3.8 Flash", "claude-opus-5.5-high": "Claude Opus 5.5 (High)", "claude-opus-5.5-medium": "Claude Opus 5.5 (Medium)", "claude-opus-5.5-low": "Claude Opus 5.5 (Low)", "claude-opus-5.5-thinking": "Claude Opus 5.5 (Thinking)", "claude-opus-5.5": "Claude Opus 5.5", "claude-sonnet-5.5-high": "Claude Sonnet 5.5 (High)", "claude-sonnet-5.5-medium": "Claude Sonnet 5.5 (Medium)", "claude-sonnet-5.5-low": "Claude Sonnet 5.5 (Low)", "claude-sonnet-5.5-thinking": "Claude Sonnet 5.5 (Thinking)", "claude-sonnet-5.5": "Claude Sonnet 5.5", "gpt-oss-120b-high": "GPT-OSS 120B (High)", "gpt-oss-120b-medium": "GPT-OSS 120B (Medium)", "gpt-oss-120b-low": "GPT-OSS 120B (Low)" }; function at(t) { if (!t) return "Unknown"; if (sa[t]) return sa[t].displayName; let k = String(t).trim().toLowerCase(); if (saSlugMap[k]) return saSlugMap[k]; if (k.includes("orchestrator")) return "Orchestrator (Argon)"; return t } function ra() { let t = {}; for (let e of Object.values(sa)) t[e.displayName] = e.color; return new Proxy(t, { get(a, n) { if (typeof n === "string" && !(n in a)) { let i = 0; for (let o = 0; o < n.length; o++)i = ((i << 5) - i) + n.charCodeAt(o) | 0; let o = ["#1A73E8", "#E37400", "#10B981", "#D93025", "#8AB4F8", "#F9AB00", "#00ACC1", "#9334E8", "#E52592", "#00796B", "#F538A0", "#34A853", "#FBBC04", "#EA4335", "#4285F4", "#0288D1"]; a[n] = o[Math.abs(i) % o.length] } return a[n] } }) } function kt(t) { for (let e of Object.values(sa)) if (e.displayName === t) return e.order; return 999 } function fr() { return Object.values(sa).sort((t, e) => t.order - e.order).map(t => t.displayName) } var nt = class { static createEmptyStats() { return { totalCalls: 0, totalInput: 0, totalOutput: 0, totalCacheRead: 0, lastContextSize: 0, modelBreakdown: [] } } static mergeStats(e, a) { let n = this.createEmptyStats(); n.totalCalls = e.totalCalls + a.totalCalls, n.totalInput = e.totalInput + a.totalInput, n.totalOutput = e.totalOutput + a.totalOutput, n.totalCacheRead = e.totalCacheRead + a.totalCacheRead, n.lastContextSize = a.totalInput > 0 || a.totalCalls > 0 ? a.lastContextSize : e.lastContextSize; let i = {}, o = s => { if (!i[s.displayName]) i[s.displayName] = { ...s }; else { let r = i[s.displayName]; r.calls += s.calls, r.input += s.input, r.output += s.output, r.cacheRead += s.cacheRead } }; return e.modelBreakdown.forEach(o), a.modelBreakdown.forEach(o), n.modelBreakdown = Object.values(i).sort((s, r) => kt(s.displayName) - kt(r.displayName)), n } static calculateStats(e, a = []) { let n = {}, i = this.createEmptyStats(); return e.forEach((o, s) => { let r = o.chatModel || {}, u = r.usage || {}, l = u.model || r.model || "Unknown", c = at(l); n[c] || (n[c] = { displayName: c, calls: 0, input: 0, output: 0, cacheRead: 0 }); let d = n[c]; if (d.calls++, d.input += parseInt(u.inputTokens, 10) || 0, d.output += parseInt(u.outputTokens, 10) || 0, d.cacheRead += parseInt(u.cacheReadTokens, 10) || 0, s === e.length - 1) { let m = r.chatStartMetadata?.contextWindowMetadata?.estimatedTokensUsed || r.usage?.contextTokens || 0; i.lastContextSize = parseInt(m, 10) || 0 } i.totalCalls++, i.totalInput += parseInt(u.inputTokens, 10) || 0, i.totalOutput += parseInt(u.outputTokens, 10) || 0, i.totalCacheRead += parseInt(u.cacheReadTokens, 10) || 0 }), a && a.length > 0 && a.forEach(o => { let s = o.modelUsage || o.metadata?.modelUsage; if (s) { let r = s.model || "Unknown", u = at(r); n[u] || (n[u] = { displayName: u, calls: 0, input: 0, output: 0, cacheRead: 0 }); let l = n[u]; l.calls++; let c = parseInt(s.inputTokens, 10) || 0, d = parseInt(s.outputTokens, 10) || 0, m = parseInt(s.cacheReadTokens, 10) || 0; l.input += c, l.output += d, l.cacheRead += m, i.totalCalls++, i.totalInput += c, i.totalOutput += d, i.totalCacheRead += m } }), i.modelBreakdown = Object.values(n).sort((o, s) => kt(o.displayName) - kt(s.displayName)), i } }; var it = class { constructor(e) { this.client = e } async fetchSortedConversations() { let a = (await this.client.getAllCascadeTrajectories()).trajectorySummaries || {}, n = []; for (let [i, o] of Object.entries(a)) n.push({ cascadeId: o.cascadeId || i, summary: o.summary || "N/A", lastModifiedTime: o.lastModifiedTime || "", stepCount: parseInt(o.stepCount || "0", 10) }); return n.sort((i, o) => (o.lastModifiedTime || "").localeCompare(i.lastModifiedTime || "")), n } async getLatestConversation() { let e = await this.fetchSortedConversations(); return e.length > 0 ? e[0] : null } async fetchCascadeMetadata(e) { return this.client.getCascadeMetadata(e) } async fetchSessionData(e, a = 0, n = 0) { let i = 0, o = [], s = [], r = a; for (; ;) { i++; let c = (await this.client.getCascadeMetadata(e, r)).generatorMetadata || []; if (c.length === 0) break; o.push(...c), r += c.length } let u = n; for (; ;) { i++; let c = (await this.client.getCascadeSteps(e, u)).steps || []; if (c.length === 0) break; s.push(...c), u += c.length } return { metadata: o, steps: s, nextMetaOffset: r, nextStepOffset: u, apiCalls: i } } async fetchAllData(e) { let a = await this.fetchSessionData(e, 0, 0); return { metadata: a.metadata, steps: a.steps, apiCalls: a.apiCalls } } }; var we = L(require("vscode")); var Tt = class { static filterByModifiedDate(e, a) { let n = new Date; return n.setDate(n.getDate() - (a - 1)), n.setHours(0, 0, 0, 0), e.filter(i => i.lastModifiedTime ? new Date(i.lastModifiedTime) >= n : !1) } static aggregateByDay(e, a = [], n = 8) { let i = new Map, o = new Date; for (let l = 0; l < n; l++) { let c = new Date(o); c.setDate(o.getDate() - l); let d = this.formatDate(c); i.set(d, this.createEmptyDayStats(d)) } let s = "Unknown Date"; i.has(s) || i.set(s, this.createEmptyDayStats(s)); for (let l of e) { let c = this.extractDateFromItem(l); if (c || (c = s), !i.has(c) && c !== s) continue; let d = i.get(c); this.addItemToDay(d, l) } if (a && a.length > 0) for (let l of a) { let c = l.modelUsage || l.metadata?.modelUsage; if (c) { let d = this.extractDateFromStep(l); if (d || (d = s), i.has(d)) { let m = i.get(d); this.addStepToDay(m, c) } } } let r = i.get(s); r && r.totals.inputTokens === 0 && r.totals.outputTokens === 0 && i.delete(s); let u = Array.from(i.values()); for (let l of u) l.cacheEfficiency = this.calculateCacheEfficiency(l); return u.sort((l, c) => l.date === s ? 1 : c.date === s ? -1 : c.date.localeCompare(l.date)) } static formatDate(e) { try { return new Intl.DateTimeFormat('en-CA', { timeZone: 'America/Sao_Paulo', year: 'numeric', month: '2-digit', day: '2-digit' }).format(e) } catch { let a = e.getFullYear(), n = String(e.getMonth() + 1).padStart(2, "0"), i = String(e.getDate()).padStart(2, "0"); return `${a}-${n}-${i}` } } static createEmptyDayStats(e) { return { date: e, models: {}, totals: { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0 }, cacheEfficiency: 0 } } static extractDateFromItem(e) { let a = e.timestamp || e.chatModel?.chatStartMetadata?.createdAt; if (!a) return null; try { let n = new Date(a); return this.formatDate(n) } catch { return null } } static addItemToDay(e, a) { let n = a.chatModel?.usage || {}, i = n.model || a.chatModel?.model || "Unknown", o = at(i), s = this.toNumber(n.inputTokens), r = this.toNumber(n.outputTokens), u = this.toNumber(n.cacheReadTokens); e.models[o] || (e.models[o] = { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, calls: 0 }); let l = e.models[o]; l.inputTokens += s, l.outputTokens += r, l.cacheReadTokens += u, l.calls++, e.totals.inputTokens += s, e.totals.outputTokens += r, e.totals.cacheReadTokens += u } static toNumber(e) { return typeof e == "number" ? e : typeof e == "string" && parseInt(e, 10) || 0 } static extractDateFromStep(e) { let a = e.metadata?.createdAt; if (!a) return null; try { let n = new Date(a); return this.formatDate(n) } catch { return null } } static addStepToDay(e, a) { let n = a.model || "Unknown", i = at(n), o = this.toNumber(a.inputTokens), s = this.toNumber(a.outputTokens), r = this.toNumber(a.cacheReadTokens); e.models[i] || (e.models[i] = { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, calls: 0 }); let u = e.models[i]; u.inputTokens += o, u.outputTokens += s, u.cacheReadTokens += r, u.calls++, e.totals.inputTokens += o, e.totals.outputTokens += s, e.totals.cacheReadTokens += r } static calculateCacheEfficiency(e) { let a = e.totals.inputTokens, n = e.totals.cacheReadTokens, i = a + n; return i === 0 ? 0 : Math.round(n / i * 100) } }; var ot = class { static render(e, a) { let n = e; for (let [i, o] of Object.entries(a)) { let s = new RegExp(`\\{\\{\\s*${i}\\s*\\}\\}`, "g"); n = n.replace(s, o) } return n } }; var xr = `<!doctype html>
<html lang="en">
	<head>
		<meta charset="UTF-8" />
		<meta name="viewport" content="width=device-width, initial-scale=1.0" />
		<meta http-equiv="Content-Security-Policy" content="{{ csp }}" />
		<title>Estatísticas do MedidorTokens</title>
		<script src="{{ chartJsUri }}"></script>
		<style>
			body {
				font-family: var(--vscode-font-family);
				color: var(--vscode-foreground);
				background: var(--vscode-editor-background);
				padding: 20px;
				margin: 0;
			}

			.header {
				margin-bottom: 20px;
			}

			.header h1 {
				margin: 0 0 10px 0;
				font-size: 1.5em;
			}

			.header p {
				margin: 0;
				opacity: 0.8;
				font-size: 0.9em;
			}

			.generate-btn {
				background: var(--vscode-button-background);
				color: var(--vscode-button-foreground);
				border: none;
				padding: 10px 20px;
				cursor: pointer;
				font-size: 14px;
				border-radius: 4px;
				margin-top: 15px;
			}

			.generate-btn:hover {
				background: var(--vscode-button-hoverBackground);
			}

			.generate-btn:disabled {
				opacity: 0.5;
				cursor: not-allowed;
			}

			.loading {
				padding: 20px;
				text-align: center;
			}

			.error {
				color: var(--vscode-errorForeground);
				padding: 20px;
			}

			.charts {
				display: none;
			}

			.chart-container {
				margin-bottom: 40px;
			}

			.chart-container h2 {
				font-size: 1.1em;
				margin-bottom: 15px;
				opacity: 0.9;
			}

			canvas {
				max-height: 350px;
			}

			.table-section {
				margin-bottom: 30px;
			}

			.table-title {
				font-size: 1em;
				font-weight: 600;
				margin-bottom: 8px;
				color: var(--vscode-keywords-foreground);
				display: flex;
				align-items: center;
				gap: 8px;
			}

			table {
				width: 100%;
				border-collapse: collapse;
				font-size: 0.85em;
				background: var(--vscode-editor-background);
				border: 1px solid var(--vscode-panel-border);
			}

			th,
			td {
				padding: 8px 12px;
				text-align: right;
				border-bottom: 1px solid var(--vscode-panel-border);
			}

			th:first-child,
			td:first-child {
				text-align: left;
			}

			th {
				background: var(--vscode-editor-inactiveSelectionBackground);
				cursor: pointer;
				user-select: none;
			}

			th:hover {
				background: var(--vscode-editor-selectionBackground);
				color: var(--vscode-editor-selectionForeground);
			}

			tr:hover {
				background: var(--vscode-list-hoverBackground);
			}

			.grand-total {
				font-weight: bold;
				background: var(--vscode-editor-inactiveSelectionBackground);
			}

			.notice {
				margin: 15px 0;
				font-size: 0.9em;
				opacity: 0.8;
				font-style: italic;
				color: var(--vscode-descriptionForeground);
			}

			.notice b {
				color: var(--vscode-button-background);
			}

			.total-tag {
				font-size: 0.8em;
				font-weight: normal;
				opacity: 0.8;
				margin-left: 10px;
				background: var(--vscode-badge-background);
				color: var(--vscode-badge-foreground);
				padding: 2px 8px;
				border-radius: 10px;
			}

			.toolbar {
				display: flex;
				justify-content: space-between;
				align-items: center;
				margin-bottom: 20px;
				flex-wrap: wrap;
				gap: 10px;
				padding-bottom: 12px;
				border-bottom: 1px solid var(--vscode-panel-border, rgba(255,255,255,0.1));
			}

			.filter-group {
				display: flex;
				gap: 6px;
				align-items: center;
			}

			.filter-btn {
				background: var(--vscode-button-secondaryBackground, #3c3c3c);
				color: var(--vscode-button-secondaryForeground, #ffffff);
				border: 1px solid var(--vscode-button-border, transparent);
				padding: 5px 12px;
				font-size: 11px;
				border-radius: 3px;
				cursor: pointer;
				transition: all 0.1s;
			}

			.filter-btn:hover {
				background: var(--vscode-button-secondaryHoverBackground, #4c4c4c);
			}

			.filter-btn.active {
				background: var(--vscode-button-background, #007acc);
				color: var(--vscode-button-foreground, #ffffff);
				font-weight: 600;
			}

			.export-btn {
				background: var(--vscode-button-background, #007acc);
				color: var(--vscode-button-foreground, #ffffff);
				border: none;
				padding: 5px 14px;
				font-size: 11px;
				border-radius: 3px;
				cursor: pointer;
				display: inline-flex;
				align-items: center;
				gap: 6px;
				font-weight: 500;
			}

			.export-btn:hover {
				background: var(--vscode-button-hoverBackground, #0062a3);
			}
		</style>
	</head>

	<body>
		<div id="initialState">
			<div class="header">
				<h1>\u{1F4CA} Estatísticas de Uso de Tokens</h1>
				<p>Analise a divisão do uso de tokens em todos os modelos nos últimos 8 dias.</p>
				<p>Analise a divisão do uso de tokens em todos os modelos ao longo do período selecionado.</p>
				<div class="notice">
					Nota: Para atualizar os dados, feche e abra esta aba novamente.
					Nota: Para atualizar os dados, feche e abra esta aba novamente ou utilize os filtros abaixo.
					<br /><br />
					As estatísticas dependem do histórico de conversas locais disponíveis. A alternância frequente pode limitar a disponibilidade do histórico (ex.: o histórico completo de 8 dias pode não estar disponível).
					As estatísticas dependem do histórico de conversas locais disponíveis. A alternância frequente pode limitar a disponibilidade do histórico.
				</div>
			</div>
			<button class="generate-btn" id="generateBtn" onclick="generateStats()">
			<button class="generate-btn" id="generateBtn" onclick="generateStats(8)">
				Gerar Estatísticas
			</button>
		</div>

		<div id="loadingState" class="loading" style="display: none">
			<p>\u23F3 <span id="loadingMessage">Carregando...</span></p>
		</div>

		<div id="errorState" class="error" style="display: none">
			<p>\u274C <span id="errorMessage"></span></p>
			<button class="generate-btn" onclick="generateStats()">Tentar Novamente</button>
		</div>

		<div id="chartsState" class="charts">
			<div class="toolbar">
				<div class="filter-group">
					<span style="font-size: 11px; opacity: 0.8; margin-right: 4px;">Per\xEDodo:</span>
					<button class="filter-btn active" id="filter-8" onclick="setPeriod(8)">8 Dias</button>
					<button class="filter-btn" id="filter-15" onclick="setPeriod(15)">15 Dias</button>
					<button class="filter-btn" id="filter-30" onclick="setPeriod(30)">30 Dias</button>
					<button class="filter-btn" id="filter-9999" onclick="setPeriod(9999)">Tudo</button>
				</div>
				<button class="export-btn" id="exportBtn" onclick="exportCSV()">
					\u{1F4E5} Exportar CSV
				</button>
			</div>

			<div class="chart-container">
				<h2 id="inputHeader">\u{1F4E5} Tokens de Entrada</h2>
				<canvas id="inputChart"></canvas>
			</div>
			<div class="chart-container">
				<h2 id="outputHeader">\u{1F4E4} Tokens de Saída</h2>
				<canvas id="outputChart"></canvas>
			</div>

			<div id="tablesContainer"></div>
		</div>

		<script>
			const vscode = acquireVsCodeApi();
			const MODEL_COLORS = new Proxy({{ colorsJson }}, { get(t, p) { if (typeof p === 'string' && !(p in t)) { let h = 0; for (let i = 0; i < p.length; i++) h = ((h << 5) - h) + p.charCodeAt(i) | 0; const c = ['#1A73E8','#E37400','#10B981','#D93025','#8AB4F8','#F9AB00','#00ACC1','#9334E8','#E52592','#00796B','#F538A0','#34A853','#FBBC04','#EA4335','#4285F4','#0288D1']; t[p] = c[Math.abs(h) % c.length]; } return t[p]; } });
			const MODEL_ORDER = {{ orderJson }};
			let inputChart = null;
			let outputChart = null;

			function generateStats() {
			let currentDays = 8;
			let currentData = null;

			function setPeriod(days) {
				currentDays = days;
				document.querySelectorAll('.filter-btn').forEach(function(btn) { btn.classList.remove('active'); });
				var activeBtn = document.getElementById('filter-' + days);
				if (activeBtn) activeBtn.classList.add('active');
				generateStats(days);
			}

			function generateStats(days) {
				if (days) currentDays = days;
				document.getElementById('initialState').style.display = 'none';
				document.getElementById('loadingState').style.display = 'block';
				document.getElementById('errorState').style.display = 'none';
				document.getElementById('chartsState').style.display = 'none';
				document.getElementById('generateBtn').disabled = true;
				vscode.postMessage({ command: 'generateStats' });
				var genBtn = document.getElementById('generateBtn');
				if (genBtn) genBtn.disabled = true;
				vscode.postMessage({ command: 'generateStats', days: currentDays });
			}

			window.addEventListener('message', event => {
				const message = event.data;
				switch (message.command) {
					case 'loading':
						document.getElementById('loadingMessage').textContent = message.message;
						break;
					case 'statsReady':
						showStats(message.data);
						showStats(message.data, message.days);
						break;
					case 'error':
						showError(message.message);
						break;
				}
			});

			function showError(msg) {
				document.getElementById('loadingState').style.display = 'none';
				document.getElementById('errorState').style.display = 'block';
				document.getElementById('errorMessage').textContent = msg;
				document.getElementById('generateBtn').disabled = false;
				var genBtn = document.getElementById('generateBtn');
				if (genBtn) genBtn.disabled = false;
			}

			function showStats(data) {
			function showStats(data, days) {
				currentData = data;
				if (days) {
					currentDays = days;
					document.querySelectorAll('.filter-btn').forEach(function(btn) { btn.classList.remove('active'); });
					var activeBtn = document.getElementById('filter-' + days);
					if (activeBtn) activeBtn.classList.add('active');
				}
				document.getElementById('loadingState').style.display = 'none';
				document.getElementById('chartsState').style.display = 'block';

				renderCharts(data);
				renderTables(data);
			}

			function exportCSV() {
				if (!currentData || currentData.length === 0) return;
				var csv = 'Data;Modelo;Tokens Entrada;Tokens Saida;Cache %;Chamadas API\r\n';
				currentData.forEach(function(day) {
					var dateBR = toDateBR(day.date);
					for (var model in day.models) {
						var s = day.models[model];
						var input = (s.inputTokens || 0) + (s.cacheReadTokens || 0);
						var eff = input > 0 ? Math.round(((s.cacheReadTokens || 0) / input) * 100) : 0;
						csv += dateBR + ';"' + model + '";' + input + ';' + (s.outputTokens || 0) + ';' + eff + '%;' + (s.calls || 0) + '\r\n';
					}
					var dayIn = (day.totals.inputTokens || 0) + (day.totals.cacheReadTokens || 0);
					var dayEff = dayIn > 0 ? Math.round(((day.totals.cacheReadTokens || 0) / dayIn) * 100) : 0;
					var dayCalls = Object.values(day.models).reduce(function(sum, m) { return sum + (m.calls || 0); }, 0);
					csv += dateBR + ';"TOTAL DO DIA";' + dayIn + ';' + (day.totals.outputTokens || 0) + ';' + dayEff + '%;' + dayCalls + '\r\n';
				});
				var blob = new Blob(['\uFEFF' + csv], { type: 'text/csv;charset=utf-8;' });
				var url = URL.createObjectURL(blob);
				var a = document.createElement('a');
				var now = new Date();
				var pad = function(n) { return String(n).padStart(2, '0'); };
				var fname = 'medidortokens_' + now.getFullYear() + pad(now.getMonth() + 1) + pad(now.getDate()) + '_' + pad(now.getHours()) + pad(now.getMinutes()) + '.csv';
				a.href = url;
				a.download = fname;
				document.body.appendChild(a);
				a.click();
				document.body.removeChild(a);
				URL.revokeObjectURL(url);
			}

			function toDateBR(iso) {
				if (!iso || typeof iso !== 'string') return iso;
				const p = iso.split('-');
				return p.length === 3 ? p[2] + '/' + p[1] + '/' + p[0] : iso;
			}

			function renderCharts(data) {
				const labels = data.map(d => toDateBR(d.date)).reverse();
				const allModels = [...new Set(data.flatMap(d => Object.keys(d.models)))];

				// Calculate Totals for Headers
				let totalIn = 0, totalOut = 0, totalCache = 0;
				data.forEach(d => {
					totalIn += d.totals.inputTokens + d.totals.cacheReadTokens;
					totalOut += d.totals.outputTokens;
					totalCache += d.totals.cacheReadTokens;
				});
				const totalEff = totalIn > 0 ? Math.round((totalCache / totalIn) * 100) : 0;

				// Update Headers
				// Update Headers (Safe DOM manipulation)
				const inputHeader = document.getElementById('inputHeader');
				const outputHeader = document.getElementById('outputHeader');

				const createTotalTag = (text) => {
					const span = document.createElement('span');
					span.className = 'total-tag';
					span.textContent = text;
					return span;
				};

				inputHeader.textContent = '\u{1F4E5} Tokens de Entrada ';
				inputHeader.appendChild(createTotalTag(formatNumber(totalIn) + ' (Cache: ' + totalEff + '%)'));

				outputHeader.textContent = '\u{1F4E4} Tokens de Saída ';
				outputHeader.appendChild(createTotalTag(formatNumber(totalOut)));

				// Sort models by defined order
				allModels.sort((a, b) => {
					const ia = MODEL_ORDER.indexOf(a);
					const ib = MODEL_ORDER.indexOf(b);
					return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
				});

				const createDataset = (isInput) => allModels.map(model => {
					const values = data.map(d => {
						const m = d.models[model];
						return isInput
							? (m?.inputTokens || 0) + (m?.cacheReadTokens || 0)
							: (m?.outputTokens || 0);
					}).reverse();

					const cachePercentages = isInput ? data.map(d => {
						const m = d.models[model];
						if (!m) return 0;
						const total = (m.inputTokens || 0) + (m.cacheReadTokens || 0);
						return total > 0 ? Math.round((m.cacheReadTokens / total) * 100) : 0;
					}).reverse() : null;

					return {
						label: model,
						data: values,
						cacheData: cachePercentages,
						backgroundColor: MODEL_COLORS[model] || '#6B7280',
						borderWidth: 0,
					};
				});

				const chartConfig = {
					type: 'bar',
					options: {
						indexAxis: 'y', // Horizontal bars
						responsive: true,
						maintainAspectRatio: false,
						plugins: {
							legend: { position: 'bottom' },
							tooltip: {
								callbacks: {
									label: function (context) {
										let label = context.dataset.label || '';
										if (label) label += ': ';
										label += formatNumber(context.parsed.x);
										const cacheData = context.dataset.cacheData;
										if (cacheData && cacheData[context.dataIndex] !== undefined) {
											label += ' (Cache: ' + cacheData[context.dataIndex] + '%)';
										}
										return label;
									}
								}
							}
						},
						scales: {
							x: {
								stacked: true,
								beginAtZero: true,
								ticks: { callback: v => formatNumber(v) }
							},
							y: { stacked: true }
						}
					}
				};

				if (inputChart) inputChart.destroy();
				inputChart = new Chart(document.getElementById('inputChart'), {
					...chartConfig,
					data: { labels, datasets: createDataset(true) }
				});

				if (outputChart) outputChart.destroy();
				outputChart = new Chart(document.getElementById('outputChart'), {
					...chartConfig,
					data: { labels, datasets: createDataset(false) }
				});
			}

			function renderTables(data) {
				const container = document.getElementById('tablesContainer');
				container.innerHTML = '';
				const allModels = new Set();
				data.forEach(d => Object.keys(d.models).forEach(m => allModels.add(m)));

				// Sort models by defined order
				const sortedModels = Array.from(allModels).sort((a, b) => {
					const ia = MODEL_ORDER.indexOf(a);
					const ib = MODEL_ORDER.indexOf(b);
					return (ia === -1 ? 999 : ia) - (ib === -1 ? 999 : ib);
				});

				// 1. Model Tables
				sortedModels.forEach(model => {
					const rows = [];
					let totalIn = 0, totalOut = 0, totalCache = 0, totalCalls = 0;

					data.forEach(day => {
						if (day.models[model]) {
							const s = day.models[model];
							const input = s.inputTokens + s.cacheReadTokens;
							const eff = input > 0 ? Math.round((s.cacheReadTokens / input) * 100) : 0;

							rows.push({
								date: day.date,
								input,
								output: s.outputTokens,
								cache: eff,
								calls: s.calls
							});

							totalIn += input;
							totalOut += s.outputTokens;
							totalCache += s.cacheReadTokens;
							totalCalls += s.calls;
						}
					});

					if (rows.length > 0) {
						const totalEff = totalIn > 0 ? Math.round((totalCache / totalIn) * 100) : 0;
						createTable(container, \`<span style="color:\${MODEL_COLORS[model]}">\u25CF</span> \${model}\`, rows, {
							date: 'TOTAL',
							input: totalIn,
							output: totalOut,
							cache: totalEff,
							calls: totalCalls
						});
					}
				});

				// 2. Daily Totals Table
				const dailyRows = data.map(day => {
					const input = day.totals.inputTokens + day.totals.cacheReadTokens;
					const eff = input > 0 ? Math.round((day.totals.cacheReadTokens / input) * 100) : 0;
					// Calculate total calls for the day across all models
					const dayCalls = Object.values(day.models).reduce((sum, model) => sum + (model.calls || 0), 0);
					return {
						date: day.date,
						input,
						output: day.totals.outputTokens,
						cache: eff,
						calls: dayCalls
					};
				});

				// Calculate Grand Total
				let gIn = 0, gOut = 0, gCache = 0, gCalls = 0;
				data.forEach(d => {
					gIn += d.totals.inputTokens + d.totals.cacheReadTokens;
					gOut += d.totals.outputTokens;
					gCache += d.totals.cacheReadTokens;
					gCalls += Object.values(d.models).reduce((sum, model) => sum + (model.calls || 0), 0);
				});
				const gEff = gIn > 0 ? Math.round((gCache / gIn) * 100) : 0;

				createTable(container, '\u{1F4CA} TOTAIS DIÁRIOS (Consolidado)', dailyRows, {
					date: 'TOTAL GERAL',
					input: gIn,
					output: gOut,
					cache: gEff,
					calls: gCalls
				});
			}

			function createTable(container, title, rows, totalRow) {
				const section = document.createElement('div');
				section.className = 'table-section';
				const tableId = 'tbl-' + Math.random().toString(36).substr(2, 9);

				// Title (Handle potentially colored dot separately)
				const titleDiv = document.createElement('div');
				titleDiv.className = 'table-title';
				if (title.includes('<span')) {
					// Safe partial HTML for the dot only, if provided from trusted internal logic
					// Or parse strictly. Since 'title' comes from internal logic,
					// we can use innerHTML here IF we trust our internal strings.
					// However, robust way:
					if (title.includes('\u25CF')) {
						const parts = title.split('</span>');
						const colorPart = parts[0].replace(/.*style="(.*)".*/, '$1');
						const textPart = parts[1];

						const dot = document.createElement('span');
						dot.textContent = '\u25CF';
						dot.style.color = 'inherit'; // Fallback
						// Extract color
						const colorMatch = title.match(/color:(#[0-9a-fA-F]+)/);
						if (colorMatch) dot.style.color = colorMatch[1];

						titleDiv.appendChild(dot);
						titleDiv.appendChild(document.createTextNode(' ' + textPart.trim()));
					} else {
						titleDiv.textContent = title;
					}
				} else {
					titleDiv.textContent = title;
				}
				section.appendChild(titleDiv);

				const table = document.createElement('table');
				table.id = tableId;

				// Header
				const thead = document.createElement('thead');
				const headerRow = document.createElement('tr');
				['Data \u2195', 'Entrada \u2195', 'Saída \u2195', 'Cache % \u2195', 'Chamadas API \u2195'].forEach((text, i) => {
					const th = document.createElement('th');
					th.textContent = text;
					th.onclick = () => sortTable(tableId, i);
					headerRow.appendChild(th);
				});
				thead.appendChild(headerRow);
				table.appendChild(thead);

				// Body
				const tbody = document.createElement('tbody');

				const createCell = (text) => {
					const td = document.createElement('td');
					td.textContent = text;
					return td;
				}

				rows.forEach(row => {
					const tr = document.createElement('tr');
					tr.appendChild(createCell(toDateBR(row.date)));
					tr.appendChild(createCell(formatNumber(row.input)));
					tr.appendChild(createCell(formatNumber(row.output)));
					tr.appendChild(createCell(row.cache + '%'));
					tr.appendChild(createCell(formatNumber(row.calls || 0)));
					tbody.appendChild(tr);
				});

				// Total Row
				const totalTr = document.createElement('tr');
				totalTr.className = 'grand-total';
				totalTr.appendChild(createCell(totalRow.date));
				totalTr.appendChild(createCell(formatNumber(totalRow.input)));
				totalTr.appendChild(createCell(formatNumber(totalRow.output)));
				totalTr.appendChild(createCell(totalRow.cache + '%'));
				totalTr.appendChild(createCell(formatNumber(totalRow.calls || 0)));
				tbody.appendChild(totalTr);

				table.appendChild(tbody);
				section.appendChild(table);
				container.appendChild(section);
			}

			function sortTable(tableId, n) {
				var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
				table = document.getElementById(tableId);
				switching = true;
				dir = "asc";

				while (switching) {
					switching = false;
					rows = table.rows;
					// Loop through all table rows (except first, which contains table headers, and last which is total):
					for (i = 1; i < (rows.length - 2); i++) {
						shouldSwitch = false;
						x = rows[i].getElementsByTagName("TD")[n];
						y = rows[i + 1].getElementsByTagName("TD")[n];

						let xVal = x.innerHTML.toLowerCase();
						let yVal = y.innerHTML.toLowerCase();

						// Parse numbers tailored for K/M format if needed, but for now simple string compare might fail for K/M
						// Let's rely on simple string compare for Dates, but for numbers we should ideally parse.
						// Quick fix: remove K/M/% and parse float
						const parse = (val) => {
							let v = val.replace(/[KMB%]/g, '');
							let mult = 1;
							if (val.includes('K')) mult = 1000;
							if (val.includes('M')) mult = 1000000;
							return parseFloat(v) * mult;
						};

						if (n > 0) { // Number columns
							xVal = parse(x.innerText);
							yVal = parse(y.innerText);
						} else { // Date column: DD/MM/AAAA -> YYYYMMDD
							const pDate = (val) => {
								const p = val.trim().split('/');
								return p.length === 3 ? p[2] + p[1] + p[0] : val;
							};
							xVal = pDate(x.innerText);
							yVal = pDate(y.innerText);
						}

						if (dir == "asc") {
							if (xVal > yVal) { shouldSwitch = true; break; }
						} else if (dir == "desc") {
							if (xVal < yVal) { shouldSwitch = true; break; }
						}
					}
					if (shouldSwitch) {
						rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
						switching = true;
						switchcount++;
					} else {
						if (switchcount == 0 && dir == "asc") {
							dir = "desc";
							switching = true;
						}
					}
				}
			}

			function formatNumber(n) {
				if (n >= 1000000) return (n / 1000000).toFixed(2) + 'M';
				if (n >= 1000) return (n / 1000).toFixed(1) + 'K';
				return n.toString();
			}
		</script>
	</body>
</html>
`; var ca = class t { constructor(e, a) { this._disposables = []; this._panel = e, this._extensionUri = a, this._panel.webview.html = this._getHtmlForWebview(), this._panel.onDidDispose(() => this.dispose(), null, this._disposables), this._panel.webview.onDidReceiveMessage(n => this._onMessage(n), null, this._disposables) } static { this.viewType = "medidortokens.statisticsPanel" } static createOrShow(e) { let a = we.window.activeTextEditor ? we.window.activeTextEditor.viewColumn : void 0; if (t.currentPanel) { t.currentPanel._panel.reveal(a); return } let n = we.window.createWebviewPanel(t.viewType, "Estatísticas do MedidorTokens", a || we.ViewColumn.One, { enableScripts: !0, retainContextWhenHidden: !0 }); t.currentPanel = new t(n, e) } dispose() { for (t.currentPanel = void 0, this._panel.dispose(); this._disposables.length;) { let e = this._disposables.pop(); e && e.dispose() } } async _onMessage(e) { e.command === "generateStats" && await this._generateStatistics() } async _generateStatistics() { this._postMessage({ command: "loading", message: "Buscando trajetórias..." }); try { let e = await this._getClient(); if (!e) { this._postMessage({ command: "error", message: "Language server não conectado" }); return } let a = new it(e), n = await a.fetchSortedConversations(), i = Tt.filterByModifiedDate(n, 8); this._postMessage({ command: "loading", message: `Processando ${i.length} conversas...` }); let o = [], s = []; for (let u of i) try { let l = await a.fetchAllData(u.cascadeId); o.push(...l.metadata), s.push(...l.steps) } catch { } let r = Tt.aggregateByDay(o, s, 8); this._postMessage({ command: "statsReady", data: r }) } catch (e) { this._postMessage({ command: "error", message: e.message || "Erro desconhecido" }) } } async _getClient() { let a = await new qe().scanEnvironment(); return a ? new tt(a.connectPort, a.csrfToken) : null } _postMessage(e) { this._panel.webview.postMessage(e) } _getHtmlForWebview() { let e = this._panel.webview, a = e.asWebviewUri(we.Uri.joinPath(this._extensionUri, "resources", "libs", "chart.min.js")), n = JSON.stringify(ra()), i = JSON.stringify(fr()), o = `default-src 'none'; img-src https:; script-src ${e.cspSource} 'unsafe-inline'; style-src 'unsafe-inline';`; return ot.render(xr, { csp: o, chartJsUri: a.toString(), colorsJson: n, orderJson: i }) } }; var hr = `<!doctype html>
`; var ca = class t { constructor(e, a) { this._disposables = []; this._panel = e, this._extensionUri = a, this._panel.webview.html = this._getHtmlForWebview(), this._panel.onDidDispose(() => this.dispose(), null, this._disposables), this._panel.webview.onDidReceiveMessage(n => this._onMessage(n), null, this._disposables) } static { this.viewType = "medidortokens.statisticsPanel" } static createOrShow(e) { let a = we.window.activeTextEditor ? we.window.activeTextEditor.viewColumn : void 0; if (t.currentPanel) { t.currentPanel._panel.reveal(a); return } let n = we.window.createWebviewPanel(t.viewType, "Estatísticas do MedidorTokens", a || we.ViewColumn.One, { enableScripts: !0, retainContextWhenHidden: !0 }); t.currentPanel = new t(n, e) } dispose() { for (t.currentPanel = void 0, this._panel.dispose(); this._disposables.length;) { let e = this._disposables.pop(); e && e.dispose() } } async _onMessage(e) { e.command === "generateStats" && await this._generateStatistics(e.days || 8) } async _generateStatistics(days = 8) { this._postMessage({ command: "loading", message: "Buscando trajetórias..." }); try { let e = await this._getClient(); if (!e) { this._postMessage({ command: "error", message: "Language server não conectado" }); return } let a = new it(e), n = await a.fetchSortedConversations(), i = (days >= 9999) ? n : Tt.filterByModifiedDate(n, days); this._postMessage({ command: "loading", message: `Processando ${i.length} conversas...` }); let o = [], s = []; for (let u of i) try { let l = await a.fetchAllData(u.cascadeId); o.push(...l.metadata), s.push(...l.steps) } catch { } let r = Tt.aggregateByDay(o, s, (days >= 9999 ? 60 : days)); this._postMessage({ command: "statsReady", data: r, days: days }) } catch (e) { this._postMessage({ command: "error", message: e.message || "Erro desconhecido" }) } } async _getClient() { let a = await new qe().scanEnvironment(); return a ? new tt(a.connectPort, a.csrfToken) : null } _postMessage(e) { this._panel.webview.postMessage(e) } _getHtmlForWebview() { let e = this._panel.webview, a = e.asWebviewUri(we.Uri.joinPath(this._extensionUri, "resources", "libs", "chart.min.js")), n = JSON.stringify(ra()), i = JSON.stringify(fr()), o = `default-src 'none'; img-src https:; script-src ${e.cspSource} 'unsafe-inline'; style-src 'unsafe-inline';`; return ot.render(xr, { csp: o, chartJsUri: a.toString(), colorsJson: n, orderJson: i }) } }; var hr = `<!doctype html>
<html lang="en">
	<head>
		<meta charset="UTF-8" />
		<meta name="viewport" content="width=device-width, initial-scale=1.0" />
		<meta http-equiv="Content-Security-Policy" content="{{csp}}" />
		<script src="{{chartJsUri}}"></script>
		<style>
			body {
				font-family: var(--vscode-font-family);
				font-size: 11px;
				color: var(--vscode-foreground);
				padding: 8px 10px;
				margin: 0;
				overflow-x: hidden;
			}
			.center {
				display: flex;
				flex-direction: column;
				align-items: center;
				justify-content: center;
				min-height: 100px;
				gap: 8px;
			}
			.section {
				margin-bottom: 14px;
				padding-bottom: 8px;
				border-bottom: 1px solid var(--vscode-panel-border);
			}
			.section-title {
				font-size: 10px;
				opacity: 0.6;
				margin-bottom: 6px;
				font-weight: 600;
				text-transform: uppercase;
				letter-spacing: 0.5px;
			}
			.section-title.collapsible {
				cursor: pointer;
				display: flex;
				justify-content: space-between;
				align-items: center;
				padding: 4px 0;
				margin-top: -4px;
			}
			.section-title.collapsible:hover {
				opacity: 1;
				color: var(--vscode-textLink-foreground);
			}
			.chevron {
				font-size: 8px;
				transition: transform 0.1s;
				display: inline-block;
			}
			.hidden {
				display: none;
			}
			.chart-wrapper.pie-container {
				height: 160px;
				margin: 10px auto;
				display: flex;
				justify-content: center;
			}
			.session-select {
				width: 100%;
				background: var(--vscode-dropdown-background);
				color: var(--vscode-dropdown-foreground);
				border: 1px solid var(--vscode-dropdown-border, rgba(255, 255, 255, 0.2));
				padding: 5px 6px;
				font-size: 11px;
				border-radius: 4px;
				margin-bottom: 8px;
				outline: none;
				cursor: pointer;
				box-sizing: border-box;
			}
			.session-select:focus {
				border-color: var(--vscode-focusBorder);
			}
			.chart-legend {
				margin-top: 6px;
				margin-bottom: 14px;
				display: flex;
				flex-direction: column;
				gap: 4px;
			}
			.legend-row {
				display: flex;
				align-items: center;
				justify-content: space-between;
				font-size: 11px;
				color: var(--vscode-foreground, #ffffff);
				padding: 2px 0;
			}
			.legend-label {
				display: flex;
				align-items: center;
				gap: 6px;
				overflow: hidden;
				text-overflow: ellipsis;
				white-space: nowrap;
				max-width: 65%;
			}
			.legend-dot {
				width: 8px;
				height: 8px;
				border-radius: 50%;
				display: inline-block;
				flex-shrink: 0;
			}
			.legend-val {
				font-weight: 600;
				color: #ffffff;
				white-space: nowrap;
				font-size: 11px;
			}
			.legend-fmt {
				opacity: 0.8;
				font-weight: normal;
				color: #cccccc;
				font-size: 10px;
			}
			.row {
				display: flex;
				justify-content: space-between;
				padding: 2px 0;
			}
			.sub-row {
				display: flex;
				justify-content: space-between;
				padding: 1px 0 1px 10px;
				font-size: 10px;
				opacity: 0.8;
				color: var(--vscode-foreground);
			}
			.sub-value {
				font-weight: 500;
				color: var(--vscode-foreground);
			}
			.ctx-normal {
				color: var(--vscode-textLink-foreground, #4285F4);
			}
			.ctx-warn {
				color: #F9AB00 !important;
			}
			.ctx-danger {
				color: #FF6B35 !important;
			}
			.value {
				font-weight: 600;
				color: var(--vscode-textLink-foreground);
			}
			.model-row {
				display: flex;
				align-items: flex-start;
				gap: 6px;
				margin-bottom: 6px;
			}
			.dot {
				width: 7px;
				height: 7px;
				border-radius: 50%;
				margin-top: 3px;
				flex-shrink: 0;
			}
			.model-info {
				display: flex;
				flex-direction: column;
				min-width: 0;
				flex: 1;
			}
			.model-name {
				font-weight: 500;
				font-size: 11px;
			}
			.model-details {
				font-size: 9px;
				opacity: 0.7;
			}
			.model-meta {
				font-size: 9px;
				opacity: 0.5;
			}
			.header-row {
				display: flex;
				justify-content: space-between;
				align-items: center;
				margin-top: 8px;
			}
			.chart-label {
				font-size: 10px;
				font-weight: 500;
				opacity: 0.8;
			}
			.quota-header-bar {
				display: flex;
				justify-content: space-between;
				align-items: center;
				margin-bottom: 6px;
			}
			.quota-gear-btn {
				background: transparent;
				color: var(--vscode-textLink-foreground, #4285F4);
				border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.35));
				border-radius: 3px;
				padding: 1px 6px;
				font-size: 9px;
				cursor: pointer;
			}
			.quota-gear-btn:hover {
				background: var(--vscode-list-hoverBackground, rgba(255,255,255,0.06));
			}
			.quota-card {
				padding: 5px 6px;
				margin-bottom: 6px;
				border-radius: 4px;
				background: var(--vscode-editorWidget-background, rgba(128,128,128,0.08));
				border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.2));
			}
			.quota-card-title {
				font-size: 10px;
				font-weight: 600;
				margin-bottom: 3px;
				display: flex;
				justify-content: space-between;
				align-items: center;
			}
			.quota-win-row {
				font-size: 9.5px;
				margin-top: 3px;
			}
			.quota-win-top {
				display: flex;
				justify-content: space-between;
				align-items: center;
				gap: 4px;
			}
			.quota-win-label {
				opacity: 0.75;
				font-weight: 500;
			}
			.quota-win-vals {
				font-weight: 500;
				text-align: right;
			}
			.quota-bar-bg {
				width: 100%;
				height: 4px;
				background: rgba(128,128,128,0.25);
				border-radius: 2px;
				margin-top: 2px;
				overflow: hidden;
			}
			.quota-bar-fill {
				height: 100%;
				border-radius: 2px;
			}
			.quota-ref-meta {
				font-size: 8.5px;
				opacity: 0.78;
			}
			.quota-status-row {
				display: flex;
				justify-content: space-between;
				align-items: center;
				gap: 4px;
				margin-top: 3px;
				flex-wrap: wrap;
			}
			.quota-reset-badge {
				display: inline-flex;
				align-items: center;
				gap: 3px;
				font-size: 8.5px;
				font-weight: 600;
				padding: 1px 5px;
				border-radius: 999px;
				white-space: nowrap;
				letter-spacing: 0.2px;
			}
			.quota-reset-badge.reset-ready {
				background: rgba(74, 222, 128, 0.16);
				color: #4ade80;
				border: 1px solid rgba(74, 222, 128, 0.4);
			}
			.quota-reset-badge.reset-soon {
				background: rgba(56, 189, 248, 0.16);
				color: #38bdf8;
				border: 1px solid rgba(56, 189, 248, 0.4);
			}
			.quota-reset-badge.reset-later {
				background: rgba(249, 171, 0, 0.15);
				color: #F9AB00;
				border: 1px solid rgba(249, 171, 0, 0.4);
			}
			.quota-form {
				padding: 6px;
				margin-bottom: 8px;
				border-radius: 4px;
				background: var(--vscode-editorWidget-background, rgba(128,128,128,0.1));
				border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.3));
			}
			.quota-form-title {
				font-size: 9.5px;
				font-weight: 600;
				margin-bottom: 4px;
				opacity: 0.9;
			}
			.quota-input-row {
				display: flex;
				justify-content: space-between;
				align-items: center;
				margin-bottom: 4px;
				font-size: 9.5px;
			}
			.quota-input {
				width: 54px;
				padding: 2px 4px;
				font-size: 10px;
				background: var(--vscode-input-background, #1e1e1e);
				color: var(--vscode-input-foreground, #cccccc);
				border: 1px solid var(--vscode-input-border, rgba(128,128,128,0.4));
				border-radius: 2px;
				text-align: right;
			}
			.quota-btn-row {
				display: flex;
				gap: 6px;
				margin-top: 6px;
			}
			.quota-btn {
				flex: 1;
				background: var(--vscode-button-background);
				color: var(--vscode-button-foreground);
				border: none;
				border-radius: 2px;
				padding: 3px 6px;
				font-size: 9.5px;
				cursor: pointer;
			}
			.quota-btn-sec {
				background: var(--vscode-button-secondaryBackground, rgba(128,128,128,0.25));
				color: var(--vscode-button-secondaryForeground, var(--vscode-foreground));
			}
			.account-row-bar {
				display: flex;
				align-items: center;
				gap: 4px;
				margin-bottom: 6px;
			}
			.quota-mini-btn {
				background: var(--vscode-button-secondaryBackground, rgba(128,128,128,0.2));
				color: var(--vscode-foreground);
				border: 1px solid var(--vscode-panel-border, rgba(128,128,128,0.3));
				border-radius: 3px;
				width: 22px;
				height: 22px;
				display: inline-flex;
				align-items: center;
				justify-content: center;
				cursor: pointer;
				font-size: 13px;
				line-height: 1;
				padding: 0;
				user-select: none;
			}
			.quota-mini-btn:hover:not(:disabled) {
				background: var(--vscode-button-background);
				color: var(--vscode-button-foreground);
			}
			.plan-badge-btn {
				border-radius: 999px;
				font-size: 8.5px;
				font-weight: 600;
				padding: 2px 7px;
				cursor: pointer;
				white-space: nowrap;
				user-select: none;
			}
			.plan-badge-pro {
				background: rgba(74, 222, 128, 0.16);
				color: #4ade80;
				border: 1px solid rgba(74, 222, 128, 0.4);
			}
			.plan-badge-free {
				background: rgba(56, 189, 248, 0.16);
				color: #38bdf8;
				border: 1px solid rgba(56, 189, 248, 0.4);
			}
		</style>
	</head>
	<body>
		<script>
			window.vscode = acquireVsCodeApi();
		</script>
		{{content}} {{chartScript}}
		<script>
			document.addEventListener('click', (e) => {
				if (e.target.id === 'refresh' || e.target.id === 'retry') {
					window.vscode.postMessage({ command: 'refresh' });
				}
			});
			document.addEventListener('change', (e) => {
				if (e.target && e.target.id === 'session-select') {
					window.vscode.postMessage({ command: 'changeSession', session: e.target.value });
				}
			});
		</script>
		{{initialScript}}
	</body>
</html>
`; var Tn, pa = new oa, Rn = ra(), An = "#6B7280", statusBarItem, la = class {
	constructor(e, gState) {
		this._extensionUri = e;
		this._globalState = gState || null;
		this._chartsExpanded = !1;
		this._windowsExpanded = this._globalState ? this._globalState.get("medidortokens.windowsExpanded", !0) : !0;
		this._quotaFormOpen = !1;
		this._accounts = this._globalState ? this._globalState.get("medidortokens.accounts", ["Conta Local"]) : ["Conta Local"];
		this._activeAccount = this._globalState ? this._globalState.get("medidortokens.activeAccount", "Conta Local") : "Conta Local";
		if (!this._accounts.includes(this._activeAccount)) {
			this._accounts.push(this._activeAccount);
		}
		let defaultAccountPlans = {
			"Conta Local": "pro"
		};
		this._accountPlans = this._globalState ? this._globalState.get("medidortokens.accountPlans", defaultAccountPlans) : defaultAccountPlans;
		let defaultQuotaRefsByAccount = {
			"Conta Local": {}
		};
		this._quotaRefsByAccount = this._globalState ? this._globalState.get("medidortokens.quotaRefsByAccount", defaultQuotaRefsByAccount) : defaultQuotaRefsByAccount;
		let migrated = !1;
		for (let acc of Object.keys(this._quotaRefsByAccount || {})) {
			let accRefs = this._quotaRefsByAccount[acc] || {};
			for (let k of Object.keys(accRefs)) {
				let r = accRefs[k];
				if (r && typeof r.pct === "number" && r.nativeMode !== 2) {
					if (r.pct <= 45) r.pct = 100 - r.pct;
					r.nativeMode = 2;
					migrated = !0;
				}
			}
		}
		if (migrated && this._globalState) {
			this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
		}
		this._quotaRefs = this._quotaRefsByAccount[this._activeAccount] || {};
		this._lastAutoQuotaSyncAt = 0;
		this._manualAccountOverride = !1;
		this._manualPlanOverride = {};
		this._syncingQuotas = !1;
		this._detectedIdeAccount = null;
		this._rawStepsCache = new Map();
		this._windowStats = null;
		this._selectedSession = "latest";
		this._sessionsList = [];
		this._viewNeedsRender = !1;
		this._lastStats = null;
		this._chartsExpanded = Q.workspace.getConfiguration("medidortokens").get("chartsExpanded", !1);
		Q.workspace.onDidChangeConfiguration(a => {
			a.affectsConfiguration("medidortokens.chartsExpanded") && (this._chartsExpanded = Q.workspace.getConfiguration("medidortokens").get("chartsExpanded", !1), T.info(`[Sidebar] Configuration changed externally. _chartsExpanded: ${this._chartsExpanded}`), this._view && this._view.visible && this._fetchCurrentSessionStats(!1));
			a.affectsConfiguration("medidortokens.showStatusBar") && this._updateStatusBar(this._lastStats, this._selectedSession);
		});
		this._startAutoRefresh();
		this._fetchCurrentSessionStats(!1);
	}
	static { this.viewType = "medidortokens.mainView" }
	resolveWebviewView(e, a, n) {
		this._view = e;
		this._viewNeedsRender = !0;
		e.webview.options = { enableScripts: !0, localResourceRoots: [this._extensionUri] };
		this._updateHtml({ loading: !0 });
		e.webview.onDidReceiveMessage(async i => {
			if (i.command === "refresh") await this._fetchCurrentSessionStats(!0);
			else if (i.command === "toggleCharts") {
				this._chartsExpanded = i.expanded;
				await Q.workspace.getConfiguration("medidortokens").update("chartsExpanded", i.expanded, Q.ConfigurationTarget.Global);
				T.info(`[Sidebar] Charts expanded state updated synchronously to: ${i.expanded}`);
			} else if (i.command === "toggleWindows") {
				this._windowsExpanded = !!i.expanded;
				this._globalState && await this._globalState.update("medidortokens.windowsExpanded", this._windowsExpanded);
			} else if (i.command === "toggleQuotaForm") {
				this._quotaFormOpen = !!i.open;
			} else if (i.command === "changeAccount") {
				this._manualAccountOverride = !0;
				this._activeAccount = i.account || (this._accounts && this._accounts[0]) || "Conta Local";
				this._quotaRefs = this._quotaRefsByAccount[this._activeAccount] || {};
				this._globalState && await this._globalState.update("medidortokens.activeAccount", this._activeAccount);
				this._viewNeedsRender = !0;
				await this._fetchCurrentSessionStats(!0);
			} else if (i.command === "toggleAccountPlan") {
				let curPlan = this._getActivePlan();
				let nextPlan = curPlan === "pro" ? "free" : "pro";
				this._manualPlanOverride[this._activeAccount] = !0;
				this._accountPlans[this._activeAccount] = nextPlan;
				this._globalState && await this._globalState.update("medidortokens.accountPlans", this._accountPlans);
				this._viewNeedsRender = !0;
				await this._fetchCurrentSessionStats(!0);
			} else if (i.command === "syncQuotasNow") {
				this._manualAccountOverride = !1;
				if (this._manualPlanOverride) delete this._manualPlanOverride[this._activeAccount];
				await this._autoSyncQuotasFromIDE(!0);
				this._viewNeedsRender = !0;
				await this._fetchCurrentSessionStats(!0);
			} else if (i.command === "promptAddAccount") {
				let newAcc = await Q.window.showInputBox({
					prompt: "Adicionar Conta do Antigravity",
					placeHolder: "ex: usuario@gmail.com ou Conta Pessoal"
				});
				if (newAcc && newAcc.trim()) {
					newAcc = newAcc.trim();
					if (!this._accounts.includes(newAcc)) {
						this._accounts.push(newAcc);
						this._globalState && await this._globalState.update("medidortokens.accounts", this._accounts);
					}
					let pick = await Q.window.showQuickPick([
						{ label: "Antigravity Starter Quota (Free)", description: "Gemini 7d e Claude/GPT 7d apenas (sem 5h, sem Argon)", val: "free" },
						{ label: "Google AI Pro", description: "Gemini 7d/5h, Claude/GPT 7d/5h, Gemini 4 Argon", val: "pro" }
					], { placeHolder: "Selecione o plano para " + newAcc });
					let selectedPlan = pick ? pick.val : "free";
					this._accountPlans[newAcc] = selectedPlan;
					this._globalState && await this._globalState.update("medidortokens.accountPlans", this._accountPlans);
					this._activeAccount = newAcc;
					this._quotaRefs = this._quotaRefsByAccount[newAcc] || {};
					this._globalState && await this._globalState.update("medidortokens.activeAccount", this._activeAccount);
					this._viewNeedsRender = !0;
					await this._fetchCurrentSessionStats(!0);
				}
			} else if (i.command === "promptRemoveAccount") {
				if (this._accounts.length <= 1) {
					Q.window.showInformationMessage("É necessário manter pelo menos uma conta configurada.");
				} else {
					let ans = await Q.window.showWarningMessage("Deseja remover a conta '" + this._activeAccount + "'?", { modal: true }, "Remover");
					if (ans === "Remover") {
						let rem = this._activeAccount;
						this._accounts = this._accounts.filter(a => a !== rem);
						delete this._accountPlans[rem];
						delete this._quotaRefsByAccount[rem];
						this._activeAccount = this._accounts[0];
						this._quotaRefs = this._quotaRefsByAccount[this._activeAccount] || {};
						if (this._globalState) {
							await this._globalState.update("medidortokens.accounts", this._accounts);
							await this._globalState.update("medidortokens.accountPlans", this._accountPlans);
							await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
							await this._globalState.update("medidortokens.activeAccount", this._activeAccount);
						}
						this._viewNeedsRender = !0;
						await this._fetchCurrentSessionStats(!0);
					}
				}
			} else if (i.command === "saveQuotaRefs") {
				let now = Date.now();
				let parseDuration = str => {
					if (!str || typeof str !== "string") return null;
					let s = str.trim().toLowerCase();
					if (!s) return null;
					let totalMs = 0, matched = !1;
					let dm = s.match(/(\d+)\s*d/); if (dm) { totalMs += parseInt(dm[1], 10) * 864e5; matched = !0; }
					let hm = s.match(/(\d+)\s*h/); if (hm) { totalMs += parseInt(hm[1], 10) * 36e5; matched = !0; }
					let mm = s.match(/(\d+)\s*m/); if (mm) { totalMs += parseInt(mm[1], 10) * 6e4; matched = !0; }
					return matched && totalMs > 0 ? now + totalMs : null;
				};
				let fmtRemStr = resetAt => {
					if (!resetAt || resetAt <= now) return "";
					let mins = Math.floor((resetAt - now) / 6e4), d = Math.floor(mins / 1440), h = Math.floor((mins % 1440) / 60), m = mins % 60;
					return d > 0 ? `${d}d ${h}h` : `${h}h ${m}m`;
				};
				let prevRefs = this._quotaRefsByAccount[this._activeAccount] || {};
				let nextRefs = { ...prevRefs };
				let wsCur = this._windowStats || {};
				let sinceMap = {
					gemini_7d: wsCur.gemini?.sinceRef7d,
					gemini_5h: wsCur.gemini?.sinceRef5h,
					claude_gpt_7d: wsCur.claude_gpt?.sinceRef7d,
					claude_gpt_5h: wsCur.claude_gpt?.sinceRef5h,
					argon_7d: wsCur.argon?.sinceRef7d
				};
				for (let k of ["gemini_7d", "gemini_5h", "claude_gpt_7d", "claude_gpt_5h", "argon_7d"]) {
					let raw = i.values ? i.values[k] : "";
					let rawReset = (i.resets && typeof i.resets[k] === "string") ? i.resets[k].trim() : "";
					if (raw === "" || raw === null || raw === void 0) {
						delete nextRefs[k];
					} else {
						let num = Number(raw);
						if (!Number.isNaN(num)) {
							let pct = Math.max(0, Math.min(100, Math.round(num)));
							let prevResolved = this._resolveQuotaRef(k, prevRefs[k], now);
							let prevEst = prevResolved ? this._estimateLiveQuota(k, prevResolved, sinceMap[k]) : null;
							let displayedPct = prevEst ? prevEst.liveRem : prevResolved?.pct;
							let displayedResetStr = prevResolved ? fmtRemStr(prevResolved.resetAt) : "";
							let pctChanged = !prevResolved || pct !== displayedPct;
							let resetChanged = rawReset !== displayedResetStr;
							let newResetAt = resetChanged ? parseDuration(rawReset) : (prevResolved?.resetAt || parseDuration(rawReset));
							if (!pctChanged && !resetChanged && prevResolved) {
								nextRefs[k] = prevResolved;
							} else {
								let savedAt = pctChanged ? now : (prevResolved?.savedAt || now);
								nextRefs[k] = newResetAt ? { pct, savedAt, resetAt: newResetAt, rolledOver: !1, nativeMode: 2 } : { pct, savedAt, rolledOver: !1, nativeMode: 2 };
							}
						}
					}
				}
				this._quotaRefsByAccount[this._activeAccount] = nextRefs;
				this._quotaRefs = nextRefs;
				this._quotaFormOpen = !1;
				this._globalState && await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
				this._viewNeedsRender = !0;
				await this._fetchCurrentSessionStats(!0);
			} else if (i.command === "clearQuotaRefs") {
				this._quotaRefsByAccount[this._activeAccount] = {};
				this._quotaRefs = {};
				this._quotaFormOpen = !1;
				this._globalState && await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
				this._viewNeedsRender = !0;
				await this._fetchCurrentSessionStats(!0);
			} else if (i.command === "changeSession") {
				this._selectedSession = i.session;
				this._viewNeedsRender = !0;
				T.info(`[Sidebar] Changed session to: ${i.session}`);
				await this._fetchCurrentSessionStats(!0);
			}
		});
		e.onDidChangeVisibility(() => {
			if (e.visible) {
				this._viewNeedsRender = !0;
				this._fetchCurrentSessionStats(!1);
			}
		});
		this._viewNeedsRender = !0;
		this._fetchCurrentSessionStats(!1);
		e.onDidDispose(() => {
			this._view = void 0;
		});
	}
	_startAutoRefresh() {
		this._stopAutoRefresh();
		this._refreshInterval = setInterval(() => {
			this._fetchCurrentSessionStats(!1);
		}, 1e4);
	}
	_getActivePlan() {
		return (this._accountPlans && this._accountPlans[this._activeAccount]) || "pro";
	}
	_stopAutoRefresh() {
		this._refreshInterval && (clearInterval(this._refreshInterval), this._refreshInterval = void 0);
	}
	triggerRefresh() {
		T.info("[Sidebar] Manual refresh triggered");
		this._fetchCurrentSessionStats(!0);
	}
	_updateStatusBar(c, mode) {
		try {
			if (!statusBarItem) return;
			if (!c) {
				if (Q.workspace.getConfiguration("medidortokens").get("showStatusBar", !0)) statusBarItem.show();
				else statusBarItem.hide();
				return;
			}
			this._lastStats = c;
			if (Q.workspace.getConfiguration("medidortokens").get("showStatusBar", !0)) {
				let d = c.totalInput + c.totalCacheRead;
				let fmtIn = d >= 1e6 ? (d / 1e6).toFixed(1) + "M" : d >= 1e3 ? (d / 1e3).toFixed(1) + "K" : d;
				let fmtOut = c.totalOutput >= 1e6 ? (c.totalOutput / 1e6).toFixed(1) + "M" : c.totalOutput >= 1e3 ? (c.totalOutput / 1e3).toFixed(1) + "K" : c.totalOutput;
				let tag = (mode === "today") ? "Hoje" : (mode === "latest" || !mode) ? "Sess\xE3o" : "Hist\xF3rico";
				statusBarItem.text = `$(graph) ${fmtIn} in | ${fmtOut} out`;
				statusBarItem.tooltip = `MedidorTokens [${tag}]:\nEntrada: ${d.toLocaleString('pt-BR')} (Nova: ${c.totalInput.toLocaleString('pt-BR')} + Cache: ${c.totalCacheRead.toLocaleString('pt-BR')})\nSa\xEDda: ${c.totalOutput.toLocaleString('pt-BR')}\nChamadas: ${c.totalCalls}\nContexto: ${c.lastContextSize.toLocaleString('pt-BR')}\n\nClique para abrir as estat\xEDsticas completas.`;
				statusBarItem.show();
			} else {
				statusBarItem.hide();
			}
		} catch {}
	}
	_getQuotaGroup(modelName) {
		let s = String(modelName || "").toLowerCase();
		if (s.includes("argon") || s.includes("orchestrator")) return "argon";
		if (s.includes("claude") || s.includes("gpt") || s.includes("opus") || s.includes("sonnet")) return "claude_gpt";
		return "gemini";
	}
	_resolveQuotaRef(key, refObj, now = Date.now()) {
		if (!refObj) return null;
		if (!refObj.resetAt || now < refObj.resetAt) return { ...refObj, rolledOver: !!refObj.rolledOver };
		let periodMs = key.endsWith("_5h") ? 5 * 36e5 : 7 * 864e5;
		let cycles = Math.floor((now - refObj.resetAt) / periodMs) + 1;
		let cycleStart = refObj.resetAt + (cycles - 1) * periodMs;
		let nextResetAt = refObj.resetAt + cycles * periodMs;
		return { pct: 100, savedAt: cycleStart, resetAt: nextResetAt, rolledOver: !0, nativeMode: 2 };
	}
	async _checkAndPersistRollovers(now = Date.now()) {
		let changed = !1;
		let accRefs = this._quotaRefsByAccount[this._activeAccount] || {};
		for (let k of Object.keys(accRefs)) {
			let r = accRefs[k];
			if (r && r.resetAt && now >= r.resetAt) {
				accRefs[k] = this._resolveQuotaRef(k, r, now);
				changed = !0;
			}
		}
		if (changed) {
			this._quotaRefsByAccount[this._activeAccount] = accRefs;
			this._quotaRefs = accRefs;
			this._globalState && await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
		}
		return changed;
	}
	async _autoSyncQuotasFromIDE(force = !1) {
		let now = Date.now();
		if (!force && this._lastAutoQuotaSyncAt && (now - this._lastAutoQuotaSyncAt < 3e4)) return !1;
		if (this._syncingQuotas) return !1;
		this._syncingQuotas = !0;
		let changed = !1;
		try {
			let agyExt = Q.extensions && typeof Q.extensions.getExtension === "function" ? Q.extensions.getExtension("google.google-antigravity") : null;
			if (agyExt && !agyExt.isActive) {
				try { await agyExt.activate(); } catch {}
			}
			let exp = agyExt && agyExt.exports ? agyExt.exports : null;
			let port = exp?.port || exp?.client?.port || (Tn && Tn.port > 0 ? Tn.port : null);
			let csrfToken = exp?.csrfToken || exp?.client?.csrfToken || (Tn && (Tn.csrfToken || Tn.token)) || "";
			let serverUrl = exp?.client?.serverUrl || (port ? "http://127.0.0.1:" + port : null);
			let rpcClient = null;
			let getRpc = async () => {
				if (rpcClient) return rpcClient;
				if (exp && typeof exp.getRpcClient === "function") {
					try { rpcClient = await exp.getRpcClient(); } catch {}
				}
				return rpcClient;
			};
			let postLsJson = methodName => new Promise(resolve => {
				if (!serverUrl) return resolve(null);
				try {
					let targetUrl = new URL(`${serverUrl.replace(/\/$/, "")}/exa.language_server_pb.LanguageServerService/${methodName}`);
					let mod = targetUrl.protocol === "https:" ? require("https") : require("http");
					let bodyStr = "{}";
					let opts = {
						hostname: targetUrl.hostname,
						port: targetUrl.port || (targetUrl.protocol === "https:" ? 443 : 80),
						path: targetUrl.pathname,
						method: "POST",
						headers: {
							"Content-Type": "application/json",
							"Connect-Protocol-Version": "1",
							"x-codeium-csrf-token": csrfToken || "",
							"Content-Length": Buffer.byteLength(bodyStr)
						},
						timeout: 3500
					};
					if (targetUrl.protocol === "https:") opts.rejectUnauthorized = !1;
					let settled = !1;
					let done = val => { if (!settled) { settled = !0; resolve(val); } };
					let req = mod.request(opts, res => {
						let chunks = [];
						res.on("data", c => chunks.push(c));
						res.on("end", () => {
							if (res.statusCode !== 200) return done(null);
							try { done(JSON.parse(Buffer.concat(chunks).toString("utf8"))); } catch { done(null); }
						});
					});
					req.on("error", () => done(null));
					req.on("timeout", () => { try { req.destroy(); } catch {} done(null); });
					req.write(bodyStr);
					req.end();
				} catch { resolve(null); }
			});

			let statusRes = await postLsJson("GetUserStatus");
			if (!statusRes || (!statusRes.userStatus && !statusRes.email)) {
				let rpc = await getRpc();
				if (rpc && typeof rpc.getUserStatus === "function") {
					try { statusRes = await rpc.getUserStatus({}); } catch {}
				}
			}
			let us = statusRes?.userStatus || statusRes || null;
			let targetAcc = this._activeAccount;
			if (us && typeof us.email === "string" && us.email.trim()) {
				let email = us.email.trim();
				this._detectedIdeAccount = email;
				targetAcc = email;
				if (!this._accounts.includes(email)) {
					this._accounts.push(email);
					let ph = "Conta Local";
					if (this._accounts.includes(ph) && this._accounts.length > 1 && !Object.keys(this._quotaRefsByAccount[ph] || {}).length) {
						this._accounts = this._accounts.filter(a => a !== ph);
						delete this._quotaRefsByAccount[ph];
						delete this._accountPlans[ph];
						if (this._activeAccount === ph) {
							this._activeAccount = email;
							this._quotaRefs = this._quotaRefsByAccount[email] || {};
							this._globalState && await this._globalState.update("medidortokens.activeAccount", this._activeAccount);
						}
						this._globalState && await this._globalState.update("medidortokens.accountPlans", this._accountPlans);
						this._globalState && await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
					}
					this._globalState && await this._globalState.update("medidortokens.accounts", this._accounts);
					changed = !0;
				}
				if (!this._manualAccountOverride && this._activeAccount !== email) {
					this._activeAccount = email;
					this._quotaRefs = this._quotaRefsByAccount[this._activeAccount] || {};
					this._globalState && await this._globalState.update("medidortokens.activeAccount", this._activeAccount);
					changed = !0;
				}
			}
			if (us && us.userTier) {
				let tierText = String((us.userTier.description || "") + " " + (us.userTier.name || "") + " " + (us.userTier.id || "")).toLowerCase();
				let detectedPlan = null;
				if (tierText.includes("starter") || tierText.includes("free")) detectedPlan = "free";
				else if (tierText.includes("pro") || tierText.includes("ultra") || tierText.includes("standard") || tierText.includes("enterprise")) detectedPlan = "pro";
				if (detectedPlan && !this._manualPlanOverride[targetAcc] && this._accountPlans[targetAcc] !== detectedPlan) {
					this._accountPlans[targetAcc] = detectedPlan;
					this._globalState && await this._globalState.update("medidortokens.accountPlans", this._accountPlans);
					if (targetAcc === this._activeAccount) changed = !0;
				}
			}

			let quotaRes = await postLsJson("RetrieveUserQuotaSummary");
			if (!quotaRes || (!(quotaRes.response && Array.isArray(quotaRes.response.groups)) && !Array.isArray(quotaRes.groups))) {
				let rpc = await getRpc();
				if (rpc && typeof rpc.retrieveUserQuotaSummary === "function") {
					try { quotaRes = await rpc.retrieveUserQuotaSummary({}); } catch {}
				}
			}
			let groups = quotaRes?.response?.groups || quotaRes?.groups || [];
			let parseResetTime = (rt, desc, nowMs) => {
				if (typeof rt === "string" && rt) {
					let t = new Date(rt).getTime();
					if (!Number.isNaN(t) && t > 0) return t;
				} else if (rt instanceof Date) {
					let t = rt.getTime();
					if (!Number.isNaN(t) && t > 0) return t;
				} else if (rt && typeof rt === "object" && rt.seconds !== void 0) {
					let t = Number(rt.seconds) * 1e3 + Math.round(Number(rt.nanos || 0) / 1e6);
					if (!Number.isNaN(t) && t > 0) return t;
				}
				if (typeof desc === "string" && desc) {
					let s = desc.toLowerCase(), totalMs = 0, matched = !1;
					let dm = s.match(/(\d+)\s*d/); if (dm) { totalMs += parseInt(dm[1], 10) * 864e5; matched = !0; }
					let hm = s.match(/(\d+)\s*h/); if (hm) { totalMs += parseInt(hm[1], 10) * 36e5; matched = !0; }
					let mm = s.match(/(\d+)\s*m/); if (mm) { totalMs += parseInt(mm[1], 10) * 6e4; matched = !0; }
					if (matched && totalMs > 0) return nowMs + totalMs;
				}
				return null;
			};
			let extractRemPct = b => {
				if (!b || typeof b !== "object") return null;
				if (typeof b.remainingFraction === "number") return Math.max(0, Math.min(100, Math.round(b.remainingFraction * 100)));
				if (b.remaining && b.remaining.case === "remainingFraction" && typeof b.remaining.value === "number") return Math.max(0, Math.min(100, Math.round(b.remaining.value * 100)));
				if (b.remaining && typeof b.remaining.remainingFraction === "number") return Math.max(0, Math.min(100, Math.round(b.remaining.remainingFraction * 100)));
				if (typeof b.remainingPercentage === "number") return Math.max(0, Math.min(100, Math.round(b.remainingPercentage)));
				if ((b.resetTime || b.displayName || b.bucketId) && b.remainingFraction === void 0 && (!b.remaining || b.remaining.case === void 0)) return 0;
				return null;
			};
			let fetchedRefs = {};
			if (Array.isArray(groups) && groups.length > 0) {
				for (let g of groups) {
					let gName = String(g?.displayName || g?.groupId || "").toLowerCase();
					let gPrefix = null;
					if (gName.includes("argon") || gName.includes("orchestrator")) gPrefix = "argon";
					else if (gName.includes("claude") || gName.includes("gpt") || gName.includes("anthropic") || gName.includes("openai")) gPrefix = "claude_gpt";
					else if (gName.includes("gemini")) gPrefix = "gemini";
					if (!gPrefix) continue;
					let buckets = Array.isArray(g.buckets) ? g.buckets : [];
					let parsedKeys = new Set();
					for (let idx = 0; idx < buckets.length; idx++) {
						let b = buckets[idx];
						let pct = extractRemPct(b);
						if (pct === null) continue;
						let resetAt = parseResetTime(b.resetTime, b.description, now);
						let bLabel = String((b.displayName || "") + " " + (b.bucketId || "") + " " + (b.description || "")).toLowerCase();
						let winSuffix = "_7d";
						if (gPrefix === "argon") winSuffix = "_7d";
						else if (bLabel.includes("5h") || bLabel.includes("5-hour") || bLabel.includes("5 hour") || bLabel.includes("five") || bLabel.includes("hourly") || (bLabel.includes("hour") && !bLabel.includes("day") && !bLabel.includes("week"))) winSuffix = "_5h";
						else if (bLabel.includes("7d") || bLabel.includes("7-day") || bLabel.includes("7 day") || bLabel.includes("week") || bLabel.includes("semanal")) winSuffix = "_7d";
						else if (buckets.length > 1 && resetAt && (resetAt - now) <= 5.5 * 36e5) winSuffix = "_5h";
						else if (buckets.length > 1 && idx === 1 && parsedKeys.has(gPrefix + "_7d")) winSuffix = "_5h";
						let fullKey = gPrefix + winSuffix;
						parsedKeys.add(fullKey);
						fetchedRefs[fullKey] = resetAt
							? { pct, savedAt: now, resetAt, rolledOver: !1, nativeMode: 2, autoSynced: !0 }
							: { pct, savedAt: now, rolledOver: !1, nativeMode: 2, autoSynced: !0 };
					}
				}
			}
			if (Object.keys(fetchedRefs).length === 0 && Array.isArray(us?.cascadeModelConfigData?.clientModelConfigs)) {
				for (let cfg of us.cascadeModelConfigData.clientModelConfigs) {
					let qi = cfg?.quotaInfo;
					if (!qi) continue;
					let mName = cfg.label || cfg.modelOrAlias?.model || "";
					if (!mName) continue;
					let grp = this._getQuotaGroup(mName);
					let fullKey = grp + "_7d";
					let pct = typeof qi.remainingFraction === "number"
						? Math.max(0, Math.min(100, Math.round(qi.remainingFraction * 100)))
						: (qi.resetTime ? 0 : null);
					if (pct === null) continue;
					let resetAt = parseResetTime(qi.resetTime, "", now);
					if (!fetchedRefs[fullKey] || pct < fetchedRefs[fullKey].pct) {
						fetchedRefs[fullKey] = resetAt
							? { pct, savedAt: now, resetAt, rolledOver: !1, nativeMode: 2, autoSynced: !0 }
							: { pct, savedAt: now, rolledOver: !1, nativeMode: 2, autoSynced: !0 };
					}
				}
			}
			if (Object.keys(fetchedRefs).length > 0) {
				let prevAccRefs = this._quotaRefsByAccount[targetAcc] || {};
				let nextAccRefs = { ...prevAccRefs };
				let dataChanged = !1;
				for (let k of Object.keys(fetchedRefs)) {
					let prev = prevAccRefs[k];
					let cur = fetchedRefs[k];
					let prevReset = prev?.resetAt || 0;
					let curReset = cur.resetAt || 0;
					if (!prev || prev.pct !== cur.pct || Math.abs(prevReset - curReset) > 6e4 || !prev.autoSynced) {
						dataChanged = !0;
					}
					nextAccRefs[k] = cur;
				}
				this._quotaRefsByAccount[targetAcc] = nextAccRefs;
				if (targetAcc === this._activeAccount) this._quotaRefs = nextAccRefs;
				this._lastAutoQuotaSyncAt = now;
				this._globalState && await this._globalState.update("medidortokens.quotaRefsByAccount", this._quotaRefsByAccount);
				if (dataChanged && targetAcc === this._activeAccount) changed = !0;
				T.info(`[Sidebar] Auto quota sync OK for ${targetAcc}: ${Object.entries(fetchedRefs).map(([k, v]) => `${k}=${v.pct}%`).join(", ")}`);
			} else if (statusRes) {
				this._lastAutoQuotaSyncAt = now;
			}
		} catch (err) {
			T.error(`[Sidebar] Auto quota sync error: ${err.message}`);
		} finally {
			this._syncingQuotas = !1;
		}
		return changed;
	}
	_estimateLiveQuota(key, refObj, sinceObj, plan) {
		if (!refObj || typeof refObj.pct !== "number") return null;
		let curP = plan || this._getActivePlan();
		let baseRem = Math.max(0, Math.min(100, Math.round(refObj.pct)));
		let sinceIn = sinceObj?.input || 0, sinceCache = sinceObj?.cacheRead || 0, sinceOut = sinceObj?.output || 0, sinceCalls = sinceObj?.calls || 0;
		let weightedTok = (sinceIn * 0.5) + (sinceCache * 0.08) + (sinceOut * 4);
		let cfg;
		if (curP === "free") {
			cfg = {
				gemini_7d: { callPct: 1.5, tokDiv: 35000 },
				claude_gpt_7d: { callPct: 3.8, tokDiv: 15000 }
			}[key] || { callPct: 1.5, tokDiv: 35000 };
		} else {
			cfg = {
				argon_7d: { callPct: 0.36, tokDiv: 260000 },
				gemini_7d: { callPct: 0.002, tokDiv: 8000000 },
				gemini_5h: { callPct: 0.15, tokDiv: 400000 },
				claude_gpt_7d: { callPct: 0.03, tokDiv: 1800000 },
				claude_gpt_5h: { callPct: 0.45, tokDiv: 180000 }
			}[key] || { callPct: 0.10, tokDiv: 350000 };
		}
		let rawDelta = (sinceCalls * cfg.callPct) + (weightedTok > 0 ? (weightedTok / cfg.tokDiv) : 0);
		let deltaPct = Math.round(rawDelta);
		let liveRem = Math.max(0, Math.min(100, baseRem - deltaPct));
		let liveUsed = 100 - liveRem;
		return { baseRem, liveRem, liveUsed, deltaPct };
	}
	async _computeWindowStats(trajService, sessionsList) {
		let now = Date.now(), cut5h = now - 5 * 3600 * 1e3, cut7d = now - 7 * 86400 * 1e3, cutScan = now - 8 * 86400 * 1e3;
		let mkEmpty = () => ({ input: 0, cacheRead: 0, output: 0, calls: 0 });
		let groups = {
			gemini: { label: "Gemini Models", w5h: mkEmpty(), w7d: mkEmpty(), sinceRef5h: mkEmpty(), sinceRef7d: mkEmpty() },
			claude_gpt: { label: "Claude & GPT", w5h: mkEmpty(), w7d: mkEmpty(), sinceRef5h: mkEmpty(), sinceRef7d: mkEmpty() },
			argon: { label: "Gemini 4 Argon / Orchestrator", w5h: mkEmpty(), w7d: mkEmpty(), sinceRef5h: mkEmpty(), sinceRef7d: mkEmpty() }
		};
		let rawRefs = this._quotaRefs || {};
		let refs = {};
		for (let k of Object.keys(rawRefs)) refs[k] = this._resolveQuotaRef(k, rawRefs[k], now);
		let recent = (sessionsList || []).filter(c => c.lastModifiedTime && new Date(c.lastModifiedTime).getTime() >= cutScan);
		for (let c of recent) {
			try {
				let cached = this._rawStepsCache.get(c.cascadeId);
				let steps;
				if (cached && cached.lastModifiedTime === c.lastModifiedTime) {
					steps = cached.steps;
				} else {
					let metaResp = await trajService.fetchCascadeMetadata(c.cascadeId);
					let meta = metaResp?.generatorMetadata || [];
					steps = [];
					for (let m of meta) {
						let tsStr = m.timestamp || m.chatModel?.chatStartMetadata?.createdAt;
						let ts = tsStr ? new Date(tsStr).getTime() : 0;
						if (!ts || Number.isNaN(ts)) continue;
						let u = m.chatModel?.usage || {};
						let mod = at(u.model || m.chatModel?.model || "Unknown");
						steps.push({
							ts,
							group: this._getQuotaGroup(mod),
							input: parseInt(u.inputTokens, 10) || 0,
							output: parseInt(u.outputTokens, 10) || 0,
							cacheRead: parseInt(u.cacheReadTokens, 10) || 0
						});
					}
					this._rawStepsCache.set(c.cascadeId, { lastModifiedTime: c.lastModifiedTime, steps });
				}
				for (let st of steps) {
					if (st.ts < cut7d) continue;
					let g = groups[st.group] || groups.gemini;
					let r5 = refs[st.group + "_5h"];
					let r7 = refs[st.group + "_7d"];
					let start7d = (r7 && r7.resetAt) ? Math.max(cut7d, r7.resetAt - 7 * 864e5) : cut7d;
					let start5h = (r5 && r5.resetAt) ? Math.max(cut5h, r5.resetAt - 5 * 36e5) : cut5h;
					if (st.ts >= start7d) {
						g.w7d.input += st.input; g.w7d.cacheRead += st.cacheRead; g.w7d.output += st.output; g.w7d.calls++;
					}
					if (st.ts >= start5h) {
						g.w5h.input += st.input; g.w5h.cacheRead += st.cacheRead; g.w5h.output += st.output; g.w5h.calls++;
					}
					if (r5 && r5.savedAt && st.ts >= Math.max(start5h, r5.savedAt)) {
						g.sinceRef5h.input += st.input; g.sinceRef5h.cacheRead += st.cacheRead; g.sinceRef5h.output += st.output; g.sinceRef5h.calls++;
					}
					if (r7 && r7.savedAt && st.ts >= Math.max(start7d, r7.savedAt)) {
						g.sinceRef7d.input += st.input; g.sinceRef7d.cacheRead += st.cacheRead; g.sinceRef7d.output += st.output; g.sinceRef7d.calls++;
					}
				}
			} catch {}
		}
		return groups;
	}
	async _fetchCurrentSessionStats(e = !1) {
		try {
			if (!Tn) {
				let u = await new qe().scanEnvironment();
				if (!u) {
					if (this._view && this._view.visible) this._updateHtml({ error: "Language Server não encontrado" });
					return;
				}
				Tn = new tt(u.connectPort, u.csrfToken), T.info(`[Sidebar] Connected on port ${u.connectPort}`);
			}
			let a = new it(Tn), n = await a.fetchSortedConversations();
			this._sessionsList = n || [];
			if (this._sessionsList.length === 0) {
				T.error("[Sidebar] No conversations found");
				if (this._view && this._view.visible) this._updateHtml({ error: "Nenhuma sessão ativa encontrada" });
				return;
			}
			let mode = this._selectedSession || "latest", o, s = !1;
			if (mode === "today") {
				let todayKey = Tt.formatDate(new Date());
				let todayConvs = this._sessionsList.filter(c => c.lastModifiedTime && Tt.formatDate(new Date(c.lastModifiedTime)) === todayKey);
				let merged = nt.createEmptyStats();
				for (let c of todayConvs) {
					try {
						let i = pa.getEntry(c.cascadeId);
						if (i && i.lastModifiedTime === c.lastModifiedTime) {
							merged = nt.mergeStats(merged, i.stats);
						} else {
							let r = await a.fetchAllData(c.cascadeId);
							let st = nt.calculateStats(r.metadata, r.steps);
							pa.set(c.cascadeId, c.lastModifiedTime, st, r.nextMetaOffset || 0, r.nextStepOffset || 0);
							merged = nt.mergeStats(merged, st);
							s = !0;
						}
					} catch {}
				}
				o = merged;
			} else {
				let target = (mode === "latest") ? this._sessionsList[0] : (this._sessionsList.find(c => c.cascadeId === mode) || this._sessionsList[0]);
				let i = pa.getEntry(target.cascadeId);
				if (i && i.cascadeId === target.cascadeId) {
					if (!e && i.lastModifiedTime === target.lastModifiedTime) {
						o = i.stats;
					} else {
						let r = await a.fetchSessionData(target.cascadeId, i.nextMetaOffset, i.nextStepOffset);
						if (r.metadata.length > 0 || r.steps.length > 0) {
							s = !0;
							let u = nt.calculateStats(r.metadata, r.steps);
							o = nt.mergeStats(i.stats, u), pa.set(target.cascadeId, target.lastModifiedTime, o, r.nextMetaOffset, r.nextStepOffset);
						} else {
							o = i.stats, pa.set(target.cascadeId, target.lastModifiedTime, o, i.nextMetaOffset, i.nextStepOffset);
						}
					}
				} else {
					let r = await a.fetchSessionData(target.cascadeId, 0, 0);
					s = !0, o = nt.calculateStats(r.metadata, r.steps);
					pa.set(target.cascadeId, target.lastModifiedTime, o, r.nextMetaOffset, r.nextStepOffset);
				}
			}
			this._updateStatusBar(o, mode);
			let nowCheck = Date.now();
			let didAutoSync = await this._autoSyncQuotasFromIDE(e);
			if (didAutoSync) s = !0;
			let didRollover = await this._checkAndPersistRollovers(nowCheck);
			let curMin = Math.floor(nowCheck / 6e4);
			let minChanged = this._lastQuotaMin !== curMin;
			if (didRollover) s = !0;
			if (!s && !e && !this._viewNeedsRender && (!minChanged || this._quotaFormOpen)) return;
			this._lastQuotaMin = curMin;
			this._viewNeedsRender = !1;
			s && T.info(`[Sidebar] Stats Update: ${o.totalCalls} calls`);
			if (this._view && this._view.visible) {
				if (this._quotaFormOpen && !e && !didRollover) return;
				this._windowStats = await this._computeWindowStats(a, this._sessionsList);
				this._updateHtml({ stats: o, sessions: this._sessionsList, selectedSession: mode, windowStats: this._windowStats, quotaRefs: this._quotaRefs });
			}
		} catch (a) {
			T.error(`[Sidebar] Error: ${a.message}`);
			if (this._view && this._view.visible) this._updateHtml({ error: a.message });
		}
	} _updateHtml(e) {
		if (!this._view) return; let a = "", n = null; if (e.loading) a = '<div class="center">Carregando...</div>'; else if (e.error) a = `
				<div class="center">
					<p>${e.error}</p>
					<button id="retry">Tentar Novamente</button>
				</div>`; else if (e.stats) {
			let c = e.stats, d = c.totalInput + c.totalCacheRead, m = d > 0 ? (c.totalCacheRead / d * 100).toFixed(1) : "0", v = c.modelBreakdown.map(x => ({ label: x.displayName, value: x.input + x.cacheRead, color: Rn[x.displayName] || An })).filter(x => x.value > 0); v.reverse(); let f = c.modelBreakdown.map(x => ({ label: x.displayName, value: x.output, color: Rn[x.displayName] || An })).filter(x => x.value > 0); f.reverse(), n = { input: v, output: f };
			this._updateStatusBar(c, e.selectedSession);
			let h = c.modelBreakdown.map(x => {
				let S = x.input + x.cacheRead, k = S > 0 ? (x.cacheRead / S * 100).toFixed(1) : "0"; return `
					<div class="model-row">
						<span class="dot" style="background:${Rn[x.displayName] || An}"></span>
						<div class="model-info">
							<span class="model-name">${x.displayName}</span>
							<div class="model-details">
								${x.calls} chamadas \xB7 ${S.toLocaleString('pt-BR')} ent \xB7 ${x.output.toLocaleString('pt-BR')} sa\xEDda
							</div>
							<div class="model-meta">Cache: ${k}%</div>
						</div>
					</div>`}).join(""); let ctxCls = c.lastContextSize >= 180000 ? "ctx-danger" : c.lastContextSize >= 120000 ? "ctx-warn" : "ctx-normal"; let ctxWarn = c.lastContextSize >= 180000 ? " \u26A0" : "";
			let sel = e.selectedSession || "latest";
			let optHtml = `<option value="latest"${sel === "latest" ? " selected" : ""}>\u25CF Sess\xE3o Atual (Ativa)</option><option value="today"${sel === "today" ? " selected" : ""}>\u{1F4C5} Hoje (Todas as Sess\xF5es)</option>`;
			if (e.sessions && e.sessions.length > 0) {
				optHtml += `<optgroup label="Hist\xF3rico de Sess\xF5es">`;
				for (let sItem of e.sessions.slice(0, 20)) {
					optHtml += `<option value="${sItem.cascadeId}"${sel === sItem.cascadeId ? " selected" : ""}>${sItem.summary || sItem.cascadeId.substring(0, 10)}</option>`;
				}
				optHtml += `</optgroup>`;
			}
			let badgeText = sel === "today" ? "\u{1F4C5} Hoje Consolidado" : sel === "latest" ? "\u25CF Ao Vivo (10s)" : "\u{1F4C2} Hist\xF3rico";
			let badgeColor = sel === "today" ? "#38bdf8" : sel === "latest" ? "#4ade80" : "#a3a3a3";
			a = `
				<div class="section" style="padding-bottom: 6px; margin-bottom: 8px;">
					<select id="session-select" class="session-select">
						${optHtml}
					</select>
					<div style="display: flex; justify-content: space-between; align-items: center; padding: 1px 0;">
						<span style="font-size: 9px; opacity: 0.7; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px;">Visualiza\xE7\xE3o</span>
						<span style="font-size: 9px; opacity: 0.9; color: ${badgeColor}; font-weight: 500;">${badgeText}</span>
					</div>
				</div>

				<div class="section">
					<div class="row"><span>Tokens de Entrada</span><span class="value">${d.toLocaleString('pt-BR')}</span></div>
					<div class="sub-row"><span>\u21B3 Entrada Nova</span><span class="sub-value">${c.totalInput.toLocaleString('pt-BR')}</span></div>
					<div class="sub-row"><span>\u21B3 Lido do Cache</span><span class="sub-value">${c.totalCacheRead.toLocaleString('pt-BR')}</span></div>
					<div class="row" style="margin-top: 4px"><span>Tokens de Sa\xEDda</span><span class="value">${c.totalOutput.toLocaleString('pt-BR')}</span></div>
					<div class="row"><span>Chamadas de API</span><span class="value">${c.totalCalls.toLocaleString('pt-BR')}</span></div>
				</div>

				<div class="section">
					<div class="row"><span>Contexto</span><span class="value ${ctxCls}">${c.lastContextSize.toLocaleString('pt-BR')}${ctxWarn}</span></div>
					<div class="row"><span>Efici\xEAncia de Cache</span><span class="value">${m}%</span></div>
				</div>
				
				<div class="section">
					<div class="section-title">Modelos</div>
					${h}
				</div>`;
			let ws = e.windowStats || {}, qRefs = e.quotaRefs || {};
			let fmtTok = n => n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(1) + "K" : String(n || 0);
			let fmtRefTime = ts => {
				try {
					return new Intl.DateTimeFormat("pt-BR", { timeZone: "America/Sao_Paulo", day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }).format(new Date(ts)).replace(",", "");
				} catch { return ""; }
			};
			let fmtResetBadge = ts => {
				if (!ts) return "";
				let diff = ts - Date.now();
				let exact = fmtRefTime(ts);
				if (diff <= 0) return `<span class="quota-reset-badge reset-ready" title="Renovou em ${exact}">\u2713 Renovada</span>`;
				let mins = Math.floor(diff / 6e4), d = Math.floor(mins / 1440), h = Math.floor((mins % 1440) / 60), m = mins % 60;
				let label = d > 0 ? `${d}d ${h}h` : `${h}h ${m}m`;
				let cls = diff <= 2 * 36e5 ? "reset-soon" : "reset-later";
				return `<span class="quota-reset-badge ${cls}" title="Renova em ${exact}">\u23F1 ${label}</span>`;
			};
			let renderWinRow = (winKey, label, winObj, refObj, sinceObj) => {
				let ent = (winObj?.input || 0) + (winObj?.cacheRead || 0);
				let out = winObj?.output || 0;
				let ch = winObj?.calls || 0;
				let barHtml = "";
				if (refObj && typeof refObj.pct === "number") {
					let est = this._estimateLiveQuota(winKey, refObj, sinceObj);
					let remPct = est ? est.liveRem : refObj.pct;
					let usedPct = 100 - remPct;
					let deltaPct = est ? est.deltaPct : 0;
					let barColor = remPct > 50 ? "#4ade80" : remPct > 20 ? "#F9AB00" : "#FF6B35";
					let sinceTot = (sinceObj?.input || 0) + (sinceObj?.cacheRead || 0) + (sinceObj?.output || 0);
					let sinceCalls = sinceObj?.calls || 0;
					let resetBadge = fmtResetBadge(refObj.resetAt);
					let approxPrefix = deltaPct > 0 ? "~" : "";
					let rollIcon = refObj.rolledOver ? ` <span title="Ciclo reciclado automaticamente em ${fmtRefTime(refObj.savedAt)}">\u21BB</span>` : "";
					let syncIcon = (!refObj.rolledOver && refObj.autoSynced) ? ` <span title="Sincronizado automaticamente do Antigravity IDE em ${fmtRefTime(refObj.savedAt)}" style="color:#38bdf8;">\u2713</span>` : "";
					let pctLabel = `<b>${approxPrefix}${remPct}% rest.</b> <span style="opacity:0.8">(${usedPct}% usado)</span>${rollIcon}${syncIcon}`;
					let refTitle = refObj.rolledOver
						? `Ciclo reciclado automaticamente em ${fmtRefTime(refObj.savedAt)}${deltaPct > 0 ? ` (-${deltaPct}% estimado pelo uso no novo ciclo)` : ""}`
						: refObj.autoSynced
							? (deltaPct > 0
								? `Sincronizado automaticamente do Antigravity IDE: ${refObj.pct}% rest. em ${fmtRefTime(refObj.savedAt)} (-${deltaPct}% estimado pelo uso)`
								: `Sincronizado automaticamente do Antigravity IDE em ${fmtRefTime(refObj.savedAt)}`)
							: deltaPct > 0
								? `Refer\xEAncia salva: ${refObj.pct}% rest. em ${fmtRefTime(refObj.savedAt)} (-${deltaPct}% estimado pelo uso)`
								: `Refer\xEAncia salva em ${fmtRefTime(refObj.savedAt)}`;
					barHtml = `<div class="quota-bar-bg"><div class="quota-bar-fill" style="width:${remPct}%;background:${barColor}"></div></div>` +
						`<div class="quota-status-row">` +
						`<span class="quota-ref-meta" title="${refTitle}">${pctLabel} \xB7 +${fmtTok(sinceTot)} (${sinceCalls} ch)</span>` +
						`${resetBadge}` +
						`</div>`;
				}
				return `<div class="quota-win-row">
					<div class="quota-win-top"><span class="quota-win-label">${label}</span><span class="quota-win-vals">${fmtTok(ent)} ent \xB7 ${fmtTok(out)} sa\xEDda \xB7 ${ch} ch</span></div>
					${barHtml}
				</div>`;
			};
			let curPlan = this._getActivePlan();
			let groupKeys;
			if (curPlan === "free") {
				groupKeys = [
					{ key: "gemini", title: "Gemini Models", has5h: !1 },
					{ key: "claude_gpt", title: "Claude and GPT models", has5h: !1 }
				];
			} else {
				groupKeys = [
					{ key: "gemini", title: "Gemini Models", has5h: !0 },
					{ key: "claude_gpt", title: "Claude and GPT models", has5h: !0 },
					{ key: "argon", title: "Gemini 4 Argon / Orchestrator", has5h: !1 }
				];
			}
			let nowRender = Date.now();
			let cardsHtml = groupKeys.map(gk => {
				let g = ws[gk.key] || { w5h: {}, w7d: {}, sinceRef5h: {}, sinceRef7d: {} };
				let ref7 = this._resolveQuotaRef(gk.key + "_7d", qRefs[gk.key + "_7d"], nowRender);
				let ref5 = this._resolveQuotaRef(gk.key + "_5h", qRefs[gk.key + "_5h"], nowRender);
				let row7 = renderWinRow(gk.key + "_7d", "Semanal (7d)", g.w7d, ref7, g.sinceRef7d);
				let row5 = gk.has5h ? renderWinRow(gk.key + "_5h", "5 Horas (5h)", g.w5h, ref5, g.sinceRef5h) : "";
				return `<div class="quota-card"><div class="quota-card-title"><span>${gk.title}</span></div>${row7}${row5}</div>`;
			}).join("");
			let valOrEmpty = (k, sinceObj) => {
				let r = this._resolveQuotaRef(k, qRefs[k], nowRender);
				if (!r || typeof r.pct !== "number") return "";
				let est = this._estimateLiveQuota(k, r, sinceObj, curPlan);
				return String(est ? est.liveRem : r.pct);
			};
			let resetOrEmpty = k => {
				let r = this._resolveQuotaRef(k, qRefs[k], nowRender);
				if (!r || !r.resetAt) return "";
				let diff = r.resetAt - nowRender;
				if (diff <= 0) return "";
				let mins = Math.floor(diff / 6e4), d = Math.floor(mins / 1440), h = Math.floor((mins % 1440) / 60), m = mins % 60;
				return d > 0 ? `${d}d ${h}h` : `${h}h ${m}m`;
			};
			let formRows = curPlan === "free" ? `
					<div class="quota-input-row"><span>Gemini Semanal (7d)</span><span style="display:flex;gap:4px;"><input id="q-gemini-7d" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("gemini_7d", ws.gemini?.sinceRef7d)}"><input id="r-gemini-7d" class="quota-input" style="width:54px;" type="text" placeholder="4d 10h" value="${resetOrEmpty("gemini_7d")}"></span></div>
					<div class="quota-input-row"><span>Claude/GPT Semanal (7d)</span><span style="display:flex;gap:4px;"><input id="q-claude-7d" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("claude_gpt_7d", ws.claude_gpt?.sinceRef7d)}"><input id="r-claude-7d" class="quota-input" style="width:54px;" type="text" placeholder="6d 22h" value="${resetOrEmpty("claude_gpt_7d")}"></span></div>
			` : `
					<div class="quota-input-row"><span>Gemini Semanal (7d)</span><span style="display:flex;gap:4px;"><input id="q-gemini-7d" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("gemini_7d", ws.gemini?.sinceRef7d)}"><input id="r-gemini-7d" class="quota-input" style="width:54px;" type="text" placeholder="4d 10h" value="${resetOrEmpty("gemini_7d")}"></span></div>
					<div class="quota-input-row"><span>Gemini 5 Horas (5h)</span><span style="display:flex;gap:4px;"><input id="q-gemini-5h" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("gemini_5h", ws.gemini?.sinceRef5h)}"><input id="r-gemini-5h" class="quota-input" style="width:54px;" type="text" placeholder="3h 44m" value="${resetOrEmpty("gemini_5h")}"></span></div>
					<div class="quota-input-row"><span>Claude/GPT Sem. (7d)</span><span style="display:flex;gap:4px;"><input id="q-claude-7d" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("claude_gpt_7d", ws.claude_gpt?.sinceRef7d)}"><input id="r-claude-7d" class="quota-input" style="width:54px;" type="text" placeholder="6d 22h" value="${resetOrEmpty("claude_gpt_7d")}"></span></div>
					<div class="quota-input-row"><span>Claude/GPT 5h</span><span style="display:flex;gap:4px;"><input id="q-claude-5h" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("claude_gpt_5h", ws.claude_gpt?.sinceRef5h)}"><input id="r-claude-5h" class="quota-input" style="width:54px;" type="text" placeholder="3h 44m" value="${resetOrEmpty("claude_gpt_5h")}"></span></div>
					<div class="quota-input-row"><span>Argon Semanal (7d)</span><span style="display:flex;gap:4px;"><input id="q-argon-7d" class="quota-input" style="width:42px;" type="number" min="0" max="100" placeholder="%" value="${valOrEmpty("argon_7d", ws.argon?.sinceRef7d)}"><input id="r-argon-7d" class="quota-input" style="width:54px;" type="text" placeholder="4d 11h" value="${resetOrEmpty("argon_7d")}"></span></div>
			`;
			let formHtml = `
				<div id="quota-form" class="quota-form hidden">
					<div class="quota-form-title">% Restante (${curPlan === 'free' ? 'Starter Free' : 'Google AI Pro'}) e renovação</div>
					${formRows}
					<div class="quota-btn-row">
						<button id="save-quotas" class="quota-btn">Salvar Cotas</button>
						<button id="clear-quotas" class="quota-btn quota-btn-sec">Limpar</button>
					</div>
				</div>`;
			let accOptions = (this._accounts || []).map(acc => `<option value="${acc}"${this._activeAccount === acc ? " selected" : ""}>${acc}</option>`).join("");
			let planBadgeLabel = curPlan === "pro" ? "★ Pro" : "⚡ Starter (Free)";
			let planBadgeClass = curPlan === "pro" ? "plan-badge-pro" : "plan-badge-free";
			let planTitle = curPlan === "pro" ? "Plano: Google AI Pro (Clique para alternar para Starter Free)" : "Plano: Antigravity Starter Quota (Clique para alternar para Pro)";
			a += `
				<div class="section">
					<div class="quota-header-bar">
						<div class="section-title collapsible" id="toggle-windows" style="margin-bottom:0;flex:1;">
							<span>Janelas &amp; Cotas ${curPlan === 'free' ? '(7d)' : '(5h / 7d)'}</span>
							<span id="windows-chevron" class="chevron">▼</span>
						</div>
						<button id="sync-quotas-now" class="quota-gear-btn" style="margin-right:3px;" title="${this._lastAutoQuotaSyncAt ? 'Sincronizado automaticamente \xE0s ' + fmtRefTime(this._lastAutoQuotaSyncAt) + ' (Clique para sincronizar agora)' : 'Sincronizar cotas automaticamente do Antigravity IDE'}">\u{1F504} Auto</button>
						<button id="toggle-quota-form" class="quota-gear-btn" title="Informar ou ajustar cotas manualmente">⚙ Cotas</button>
					</div>
					<div class="account-row-bar">
						<div style="display:flex;align-items:center;gap:3px;flex:1;min-width:0;">
							<select id="account-select" class="session-select" style="margin-bottom:0;flex:1;min-width:0;">
								${accOptions}
							</select>
							<button id="add-account-btn" class="quota-mini-btn" title="Adicionar nova conta">+</button>
							<button id="del-account-btn" class="quota-mini-btn" title="Remover conta selecionada" ${this._accounts.length <= 1 ? "disabled style='opacity:0.3;cursor:not-allowed;'" : ""}>&times;</button>
						</div>
						<button id="toggle-plan-btn" class="plan-badge-btn ${planBadgeClass}" title="${planTitle}">${planBadgeLabel}</button>
					</div>
					${formHtml}
					<div id="windows-container">
						${cardsHtml}
					</div>
				</div>

				<div class="section" style="border-bottom: none">
					<div class="section-title collapsible" id="toggle-charts">
						<span>Distribuição de Tokens</span>
						<span id="chevron" class="chevron">▶</span>
					</div>
					
					<div id="charts-container" class="hidden">
						<div class="header-row">
							<span class="chart-label">Distribuição de Entrada</span>
						</div>
						<div class="chart-wrapper pie-container">
							<canvas id="inputPieChart"></canvas>
						</div>
						<div id="inputLegend" class="chart-legend"></div>

						<div class="header-row" style="margin-top: 16px">
							<span class="chart-label">Distribuição de Saída</span>
						</div>
						<div class="chart-wrapper pie-container">
							<canvas id="outputPieChart"></canvas>
						</div>
						<div id="outputLegend" class="chart-legend"></div>
					</div>
				</div>`} let i = this._chartsExpanded; T.info(`[Sidebar] Rendering with chartsExpanded: ${i}`); let o = this._view.webview, s = o.asWebviewUri(Q.Uri.joinPath(this._extensionUri, "resources", "libs", "chart.min.js")), r = `default-src 'none'; style-src 'unsafe-inline'; script-src ${o.cspSource} 'unsafe-inline'; img-src https:;`, u = n ? `
			<script>
				const ctxIn = document.getElementById('inputPieChart').getContext('2d');
				const ctxOut = document.getElementById('outputPieChart').getContext('2d');
				
				const pieConfig = (title, data) => {
					const total = data.reduce((sum, d) => sum + d.value, 0);
					return {
						type: 'pie',
						data: {
							labels: data.map(d => d.label),
							datasets: [{
								data: data.map(d => d.value),
								backgroundColor: data.map(d => d.color),
								borderWidth: 1,
								borderColor: 'rgba(255,255,255,0.1)'
							}]
						},
						options: {
							animation: false,
							responsive: true,
							maintainAspectRatio: true,
							plugins: {
								legend: { display: false },
								tooltip: {
									displayColors: false,
									callbacks: {
										title: function() { return ""; },
										label: function(ctx) {
											var val = ctx.raw;
											var pct = total > 0 ? ((val / total) * 100).toFixed(1) : "0";
											return ctx.label + ": " + pct + "% (" + val.toLocaleString('pt-BR') + ")";
										}
									}
								}
							}
						}
					};
				};

				new Chart(ctxIn, pieConfig('Input', ${JSON.stringify(n.input)}));
				new Chart(ctxOut, pieConfig('Output', ${JSON.stringify(n.output)}));

				function renderHtmlLegend(id, data) {
					var el = document.getElementById(id);
					if (!el) return;
					var tot = data.reduce(function(s, d) { return s + d.value; }, 0);
					el.innerHTML = data.map(function(d) {
						var pct = tot > 0 ? ((d.value / tot) * 100).toFixed(1) : "0";
						var fmt = d.value >= 1e6 ? (d.value / 1e6).toFixed(1) + "M" : d.value >= 1e3 ? (d.value / 1e3).toFixed(1) + "K" : d.value;
						return '<div class="legend-row">' +
							'<div class="legend-label"><span class="legend-dot" style="background:' + d.color + '"></span><span title="' + d.label + '">' + d.label + '</span></div>' +
							'<div class="legend-val">' + pct + '% <span class="legend-fmt">(' + fmt + ')</span></div>' +
						'</div>';
					}).join("");
				}
				renderHtmlLegend("inputLegend", ${JSON.stringify(n.input)});
				renderHtmlLegend("outputLegend", ${JSON.stringify(n.output)});
			</script>`: "", l = `
			<script>
				(function() {
					const chartsContainer = document.getElementById('charts-container');
					const chevron = document.getElementById('chevron');
					const toggleCharts = document.getElementById('toggle-charts');
					let isExpanded = ${i};

					function updateUI() {
						if (!chartsContainer || !chevron) return;
						if (isExpanded) {
							chartsContainer.classList.remove('hidden');
							chevron.innerText = '\u25BC';
						} else {
							chartsContainer.classList.add('hidden');
							chevron.innerText = '\u25B6';
						}
					}

					// Apply initial state
					updateUI();

					if (toggleCharts) {
						toggleCharts.addEventListener('click', () => {
							isExpanded = !isExpanded;
							updateUI();
							console.log('Sidebar toggleCharts:', isExpanded);
							window.vscode.postMessage({ 
								command: 'toggleCharts', 
								expanded: isExpanded 
							});
						});
					}

					const windowsContainer = document.getElementById('windows-container');
					const windowsChevron = document.getElementById('windows-chevron');
					const toggleWindows = document.getElementById('toggle-windows');
					const quotaForm = document.getElementById('quota-form');
					const toggleQuotaForm = document.getElementById('toggle-quota-form');
					const saveQuotasBtn = document.getElementById('save-quotas');
					const clearQuotasBtn = document.getElementById('clear-quotas');
					let isWindowsExpanded = ${this._windowsExpanded};

					function updateWindowsUI() {
						if (!windowsContainer || !windowsChevron) return;
						if (isWindowsExpanded) {
							windowsContainer.classList.remove('hidden');
							windowsChevron.innerText = '\u25BC';
						} else {
							windowsContainer.classList.add('hidden');
							windowsChevron.innerText = '\u25B6';
						}
					}
					updateWindowsUI();

					if (toggleWindows) {
						toggleWindows.addEventListener('click', () => {
							isWindowsExpanded = !isWindowsExpanded;
							updateWindowsUI();
							window.vscode.postMessage({ command: 'toggleWindows', expanded: isWindowsExpanded });
						});
					}
					if (toggleQuotaForm && quotaForm) {
						toggleQuotaForm.addEventListener('click', (ev) => {
							ev.stopPropagation();
							quotaForm.classList.toggle('hidden');
							window.vscode.postMessage({ command: 'toggleQuotaForm', open: !quotaForm.classList.contains('hidden') });
						});
					}
					const syncQuotasNowBtn = document.getElementById('sync-quotas-now');
					if (syncQuotasNowBtn) {
						syncQuotasNowBtn.addEventListener('click', (ev) => {
							ev.stopPropagation();
							syncQuotasNowBtn.innerText = '\u23F3...';
							window.vscode.postMessage({ command: 'syncQuotasNow' });
						});
					}
					if (saveQuotasBtn) {
						saveQuotasBtn.addEventListener('click', () => {
							window.vscode.postMessage({
								command: 'saveQuotaRefs',
								values: {
									gemini_5h: document.getElementById('q-gemini-5h')?.value ?? '',
									gemini_7d: document.getElementById('q-gemini-7d')?.value ?? '',
									claude_gpt_5h: document.getElementById('q-claude-5h')?.value ?? '',
									claude_gpt_7d: document.getElementById('q-claude-7d')?.value ?? '',
									argon_7d: document.getElementById('q-argon-7d')?.value ?? ''
								},
								resets: {
									gemini_5h: document.getElementById('r-gemini-5h')?.value ?? '',
									gemini_7d: document.getElementById('r-gemini-7d')?.value ?? '',
									claude_gpt_5h: document.getElementById('r-claude-5h')?.value ?? '',
									claude_gpt_7d: document.getElementById('r-claude-7d')?.value ?? '',
									argon_7d: document.getElementById('r-argon-7d')?.value ?? ''
								}
							});
						});
					}
					if (clearQuotasBtn) {
						clearQuotasBtn.addEventListener('click', () => {
							window.vscode.postMessage({ command: 'clearQuotaRefs' });
						});
					}
					const accountSelect = document.getElementById('account-select');
					if (accountSelect) {
						accountSelect.addEventListener('change', (ev) => {
							window.vscode.postMessage({ command: 'changeAccount', account: ev.target.value });
						});
					}
					const addAccBtn = document.getElementById('add-account-btn');
					if (addAccBtn) {
						addAccBtn.addEventListener('click', () => {
							window.vscode.postMessage({ command: 'promptAddAccount' });
						});
					}
					const delAccBtn = document.getElementById('del-account-btn');
					if (delAccBtn) {
						delAccBtn.addEventListener('click', () => {
							window.vscode.postMessage({ command: 'promptRemoveAccount' });
						});
					}
					const togglePlanBtn = document.getElementById('toggle-plan-btn');
					if (togglePlanBtn) {
						togglePlanBtn.addEventListener('click', () => {
							window.vscode.postMessage({ command: 'toggleAccountPlan' });
						});
					}
				})();
			</script>`; this._view.webview.html = ot.render(hr, { csp: r, chartJsUri: s.toString(), content: a, chartScript: u || "", initialScript: l })
	}
}; function Ru(t) {
	T.init(), T.info("MedidorTokens is now active!");
	try {
		statusBarItem = Q.window.createStatusBarItem(Q.StatusBarAlignment.Right, 100);
		statusBarItem.command = "medidortokens.openStatistics";
		statusBarItem.text = "$(graph) MedidorTokens";
		statusBarItem.tooltip = "MedidorTokens: Estat\xEDsticas de Tokens\nClique para abrir as estat\xEDsticas.";
		if (Q.workspace.getConfiguration("medidortokens").get("showStatusBar", !0)) statusBarItem.show();
		t.subscriptions.push(statusBarItem);
	} catch { }
	let e = new la(t.extensionUri, t.globalState);
	t.subscriptions.push({ dispose: () => e._stopAutoRefresh() }),
	t.subscriptions.push(Q.window.registerWebviewViewProvider(la.viewType, e)),
	t.subscriptions.push(Q.commands.registerCommand("medidortokens.refresh", () => { e.triggerRefresh() })),
	t.subscriptions.push(Q.commands.registerCommand("medidortokens.openStatistics", () => { ca.createOrShow(t.extensionUri) }))
} function Au() { } 0 && (module.exports = { activate, deactivate });
/*! Bundled license information:

mime-db/index.js:
  (*!
   * mime-db
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015-2022 Douglas Christopher Wilson
   * MIT Licensed
   *)

mime-types/index.js:
  (*!
   * mime-types
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015 Douglas Christopher Wilson
   * MIT Licensed
   *)
*/
