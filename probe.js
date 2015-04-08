var http = require('http');
var express = require('express');
var path = require('path');
var childProcess = require('child_process');
var exec = childProcess.exec;
var fs = require('fs');
var vasync = require('vasync');

/* create our express & http server and prepare to serve javascript files in ./public
 */
var app = express();
var server = http.createServer(app);
var WebSocket = require('ws');

server.setMaxListeners(0);

app.use(express.static(path.join(__dirname, 'public')));

var wsCache = {};
var processCache = {};

var PROCESS_KILLED_MESSAGE = 'process has been killed';

/* Now that we have a web socket server, we need to create a handler for connection events. These
 events represet a client connecting to our server */
function createWebSocketServer(uuid, callback) {
    var path = '/' + uuid;
    var wss = new WebSocket.Server({server: server, path: path, autoAcceptConnections: false});

    wss.on('connection', function (socket) {
        wsCache[uuid] = socket;
        var consumer;
        socket.pingssent = 0;
        var ping = setInterval(function () {
            if (socket.pingssent >= 2) {
                socket.close();
            } else {
                socket.ping();
                socket.pingssent++;
            }
        }, 20 * 1000);

        socket.on("pong", function () {
            socket.pingssent = 0;
        });

        /* Like the web server object, we must also define handlers for various socket events that
         will happen during the lifetime of the connection. These will define how we interact with
         the client. The first is a message event which occurs when the client sends something to
         the server. */
        socket.on('message', function (message) {
            try {
                message = JSON.parse(message);
            } catch (ex) {
                return send(uuid, ex);
            }

            if (message.type === 'flamegraph') {
                var dtraceScript = message.message;
                var deleteDtraceOut = function (id) {
                    return fs.unlink(__dirname + '/dtrace' + id + '.out', function (err) {
                        if (err) {
                            console.log(err);
                        };
                    });
                };

                vasync.waterfall([
                    function (callback) {
                        processCache[uuid] = exec('dtrace ' + dtraceScript + ' > dtrace' + uuid + '.out', function (error, stdout, stderr) {
                            if (process.killed) {
                                error = new Error(PROCESS_KILLED_MESSAGE);
                            }
                            callback(error);
                        });
                    },
                    function (callback) {
                        processCache[uuid] = exec(__dirname + '/node_modules/stackvis/cmd/stackvis dtrace flamegraph-svg < dtrace' + uuid + '.out',
                            function (error, stdout, stderr) {
                                var svg;
                                if (!error) {
                                    if (process.killed) {
                                        error = new Error(PROCESS_KILLED_MESSAGE);
                                    } else {
                                        try {
                                            svg = JSON.stringify(stdout);
                                        } catch (ex) {
                                            error = ex;
                                        }
                                    }
                                }
                                callback(error, uuid, svg);
                            });
                    }
                ], function (err, id, svg) {
                    if (err) {
                        return send(id, err.toString());
                    } else {
                        send(id, svg);
                    }
                    deleteDtraceOut(id);
                });
            } else {
                consumer = childProcess.fork('dtrace-consumer.js');
                consumer.send({type: message.type, message: message.message});
                consumer.on('message', function (msg) {
                    send(uuid, msg);
              });
            }
        });
        /* Not so fast. If a client disconnects we don't want their respective dtrace consumer to
         keep collecting data any more. We also don't want to try to keep sending anything to them
         period. So clean up. */
        socket.on('close', function () {
            if (consumer) {
                consumer.disconnect();
                consumer.on('exit', function (code, signal) {
                    console.log('dtrace process exited with code %s and signal %s', code, signal);
                });
                consumer.kill();
            }
            if (ping) {
                clearInterval(ping);
            }
            close(uuid);
            console.log('disconnected');
        });
    });
    callback(null, path);
}

function killProcess(uuid) {
    var process = processCache[uuid];
    if (process) {
        process.kill();
        delete processCache[uuid];
    }
}

function send(uuid, data) {
    var socket = wsCache[uuid];
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(data);
    } else {
        console.log('Error: no websocket');
    }
    killProcess(uuid);
}

function close(uuid) {
    if (wsCache[uuid]) {
        wsCache[uuid].close();
        delete wsCache[uuid];
    }
    killProcess(uuid);
}

app.get('/setup/:uuid', function (req, res, next) {
    var uuid = req.params.uuid;
    if (uuid) {
        createWebSocketServer(uuid, function (err, path) {
            res.send(path);
        });
    } else {
        res.status(404).end();
    }
});

app.get('/close/:uuid', function (req, res, next) {
    var uuid = req.params.uuid;
    if (uuid) {
        close(uuid);
        res.send('close');
    } else {
        res.status(404).end();
    }
});

app.get('/healthcheck', function (req, res, next) {
    res.send('ok');
});

app.get('/process-list', function (req, res, next) {

    exec('ps -ef -o pid,args', function(error, stdout, stderr) {
        if (stderr || error) {
            res.send(500, stderr ? stderr.toString() : error);
            return;
        }
        var lines = stdout.toString().trim().split('\n');
        var results = [];
        for (var i = 1; i < lines.length; i++) {
            var parts = lines[i].trim().replace(/\s{2,}/g, ' ');
            var positionPid = parts.indexOf(' ');
            var cmd = parts.slice(positionPid + 1);
            var execname = cmd.replace('sudo ', '').split(' ')[0];
            results.push({pid: parts.slice(0, positionPid), cmd: cmd, execname: execname.slice(execname.lastIndexOf('/') + 1)});
        }
        res.send(results);
    });
});


server.listen(8000);
