var http = require('http');
var libdtrace = require('libdtrace');
var express = require('express');
var path = require('path');
var exec = require('child_process').exec;

/* create our express & http server and prepare to serve javascript files in ./public
*/
var app = express()
  , http = require('http')
  , server = http.createServer(app)
  , io = require('socket.io').listen(server);

app.use(express.static(path.join(__dirname, 'public')));

/* Before we go any further we must realize that each time a user connects we're going to want to
   them send them dtrace aggregation every second. We can do so using 'setInterval', but we must
   keep track of both the intervals we set and the dtrace consumers that are created as we'll need
   them later when the client disconnects.
*/
var interval_id_by_session_id = {};
var dtp_by_session_id = {};

/* In order to effecienctly send packets we're going to use the Socket.IO library which seemlessly
   integrates with express.
*/

/* Now that we have a web socket server, we need to create a handler for connection events. These
   events represet a client connecting to our server */
io.sockets.on('connection', function(socket) {

    /* Like the web server object, we must also define handlers for various socket events that
       will happen during the lifetime of the connection. These will define how we interact with
       the client. The first is a message event which occurs when the client sends something to
       the server. */
    socket.on( 'message', function(message) {
        /* The only message the client ever sends will be sent right after connecting.
           So it will happen only once during the lifetime of a socket. This message also
           contains a d script which defines an agregation to walk.
        */
        var dtp = new libdtrace.Consumer();
        var dscript = message['dscript'];
        dtp.strcompile(dscript);
        dtp.go();
        dtp_by_session_id[socket.sessionId] = dtp;


         /* All that's left to do is send the aggration data from the dscript.  */
         interval_id_by_session_id[socket.sessionId] = setInterval(function () {
             var aggdata = {};
             try {
                 dtp.aggwalk(function (id, key, val) {
                     for( index in val ) {
                         aggdata[key] = val;
                     }
                 });
                 socket.emit( 'message', aggdata );
             } catch( err ) {
                 console.log(err);
             }

         }, 1101);
    });


    /* Not so fast. If a client disconnects we don't want their respective dtrace consumer to
       keep collecting data any more. We also don't want to try to keep sending anything to them
       period. So clean up. */
    socket.on('disconnect', function() {
        clearInterval(clearInterval(interval_id_by_session_id[socket.sessionId]));
        var dtp = dtp_by_session_id[socket.sessionId];
        if (dtp) {
            dtp.stop();
        }
        delete dtp_by_session_id[socket.sessionId];
        console.log('disconnected');
    });
});

app.get('/healthcheck', function(req, res, next) {
    res.send('ok');
});

app.get('/process-list', function (req, res, next) {

    exec('ps -e', function(error, stdout, stderr) {
        if (stderr || error) {
            res.send(500, stderr ? stderr.toString() : error);
            return;
        }
        var lines = stdout.toString().trim().split('\n');
        var results = [];
        for (var i = 1; i < lines.length; i++) {
            var parts = lines[i].trim().replace(/\s{2,}/g, ' ');
            results.push({pid: parts.slice(0, parts.indexOf(' ')), cmd: parts.replace(/([^\s]*\s){3}/, '')})
        }
        res.send(results);
    });
});


server.listen(8000);