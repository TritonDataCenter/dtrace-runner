var http = require('http');
var express = require('express');
var path = require('path');
var childProcess = require('child_process');
var exec = childProcess.exec;
var fs = require('fs');
/* create our express & http server and prepare to serve javascript files in ./public
 */
var app = express()
  , server = http.createServer(app)
  , WebSocketServer = require('ws').Server
  , wss = new WebSocketServer({server: server});

app.use(express.static(path.join(__dirname, 'public')));

/* Now that we have a web socket server, we need to create a handler for connection events. These
 events represet a client connecting to our server */
wss.on('connection', function(ws) {
    var consumer;

    /* Like the web server object, we must also define handlers for various socket events that
     will happen during the lifetime of the connection. These will define how we interact with
     the client. The first is a message event which occurs when the client sends something to
     the server. */
    ws.on('message', function(message) {
        message = JSON.parse(message);
        if (message.type === 'flamegraph') {
            message.id = Math.round(Math.random() * 1000000);
            var dtraceScript = message.message;
            var deleteDtraceOut = function () {
                return fs.unlink(__dirname + '/dtrace' + message.id + '.out', function (err) {
                    if (err) {
                        console.log(err);
                    };
                });
            };
            var errorCallback = function (error) {
                try {
                    ws.send('Error: ' + error.toString());
                    deleteDtraceOut();
                } catch (err) {
                    console.log(err);
                }
                return;
            };
            
            exec('dtrace ' + dtraceScript + ' > dtrace' + message.id + '.out',
                function (error, stdout, stderr) {
                    if (error) {
                      return errorCallback(error);
                    }
                    exec(__dirname + '/node_modules/stackvis/cmd/stackvis dtrace flamegraph-svg < dtrace' + message.id + '.out',
                        function (error, stdout, stderr) {
                            if (error) {
                                return errorCallback(error);
                            }
                            try {
                                ws.send(JSON.stringify(stdout));
                                deleteDtraceOut();
                            } catch (err) {
                                console.log(err);
                            }
                        });
                    });
        } else {
            consumer = childProcess.fork('dtrace-consumer.js');
            consumer.send({type: message.type, message: message.message});
            consumer.on('message', function (msg) {
              try {
                  ws.send(msg);
              }
              catch (e) {
                  console.log('websocket error happens');
              }
          });
        }
    });
    /* Not so fast. If a client disconnects we don't want their respective dtrace consumer to
     keep collecting data any more. We also don't want to try to keep sending anything to them
     period. So clean up. */
    ws.on('close', function() {
        if (consumer) {
            consumer.disconnect();
            consumer.on('exit', function(code, signal) {
                console.log('dtrace process exit with code %s and signal %s', code, signal);
            });
            consumer.kill();
        }
        console.log('disconnected');
    });
});

app.get('/healthcheck', function (req, res, next) {
    res.send('ok');
});

app.get('/process-list', function (req, res, next) {

    exec('ps -ef', function(error, stdout, stderr) {
        if (stderr || error) {
            res.send(500, stderr ? stderr.toString() : error);
            return;
        }
        var lines = stdout.toString().trim().split('\n');
        var results = [];
        for (var i = 1; i < lines.length; i++) {
            var parts = lines[i].trim().replace(/\s{2,}/g, ' ');
            var positionPid = parts.indexOf(' ') + 1;
            results.push({pid: parts.slice(positionPid, parts.indexOf(' ', positionPid)), cmd: parts.replace(/([^\s]*\s){3}/, '')});
        }
        res.send(results);
    });
});


server.listen(8000);
