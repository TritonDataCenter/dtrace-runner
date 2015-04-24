'use strict';
var path = require('path');
var childProcess = require('child_process');
var exec = childProcess.exec;
var fs = require('fs');
var vasync = require('vasync');
var url = require('url');

var httpsServer = require('https').createServer({
    ca: fs.readFileSync('./ca.pem'),
    cert: fs.readFileSync('./server-cert.pem'),
    key: fs.readFileSync('./server-key.pem'),
    rejectUnauthorized: false,
    requestCert: true
});

var WebSocket = require('ws');
var wss = new WebSocket.Server({noServer: true});

var cache = {};

var PROCESS_KILLED_MESSAGE = 'process has been killed';

/* Now that we have a web socket server, we need to create a handler for connection events. These
 events represet a client connecting to our server */

function handleWSConnection(connection) {
    return function (socket) {
        connection.socket = socket;
        var uuid = connection.uuid;
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

        socket.on('pong', function () {
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
                        }
                    });
                };

                vasync.waterfall([
                    function (callback) {
                        connection.process = exec('dtrace ' + dtraceScript + ' > dtrace' + uuid + '.out', function (error) {
                            if (connection.process && connection.process.killed) {
                                error = new Error(PROCESS_KILLED_MESSAGE);
                            }
                            callback(error);
                        });
                    },
                    function (callback) {
                        connection.process = exec(__dirname + '/node_modules/stackvis/cmd/stackvis dtrace flamegraph-svg < dtrace' + uuid + '.out',
                            function (error, stdout) {
                                var svg;
                                if (!error) {
                                    if (connection.process && connection.process.killed) {
                                        error = new Error(PROCESS_KILLED_MESSAGE);
                                    } else {
                                        svg = JSON.stringify(stdout);
                                    }
                                }
                                callback(error, uuid, svg);
                            });
                    }
                ], function (err, uuid, svg) {
                    if (err) {
                        return send(uuid, err.toString());
                    } else {
                        send(uuid, svg);
                    }
                    killProcess(uuid);
                    deleteDtraceOut(uuid);
                });
                send(uuid, 'started');
            } else {
                connection.process = childProcess.fork('dtrace-consumer.js');
                connection.process.send({type: message.type, message: message.message});
                connection.process.on('message', function (msg) {
                    send(uuid, msg);
                });
            }
        });
        /* Not so fast. If a client disconnects we don't want their respective dtrace consumer to
         keep collecting data any more. We also don't want to try to keep sending anything to them
         period. So clean up. */
        socket.on('close', function () {
            if (ping) {
                clearInterval(ping);
            }
            close(uuid);
            console.log('disconnected');
        });
    }
}

function killProcess(uuid) {
    var connection = cache[uuid];
    if (connection && connection.process) {
        connection.process.on('exit', function (code, signal) {
            console.log('dtrace process exited with code %s and signal %s', code, signal);
        });
        connection.process.kill();
        delete connection.process;
    }
}

function send(uuid, data) {
    var connection = cache[uuid];
    if (!connection) {
        return;
    }

    var socket = connection.socket;
    if (socket && socket.readyState === WebSocket.OPEN) {
        socket.send(data);
    } else {
        console.log('Error: no websocket');
        close(uuid);
    }
}

function close(uuid) {
    var connection = cache[uuid];
    if (connection && connection.socket) {
        connection.socket.close();
    }

    killProcess(uuid);
    delete cache[uuid];
}

function sendError(socket, code, message) {
    var response = [
        'HTTP/1.1 ' + code + ' ' + message,
        'Content-type: text/html',
        '', ''
    ];
    socket.write(response.join('\r\n'));

}

httpsServer.on('upgrade', function (req, socket, upgradeHead) {
    var uuid = req.url.split('/')[1];
    var connection = cache[uuid];
    if (!req.connection.authorized) {
        sendError(socket, 401, 'Unauthorized');
        return;
    }
    if (!connection) {
        if (!uuid) {
            return sendError(socket, 404, 'Not Found');
        } else {
            connection = cache[uuid] = {uuid: uuid};
        }
    }
    connection.socket = socket;
    wss.handleUpgrade(req, socket, upgradeHead, handleWSConnection(connection));
});

httpsServer.on('request', function (req, res) {
    var pathname = url.parse(req.url).pathname;

    if (!req.client.authorized) {
        res.statusCode = 401;
        return res.end();
    } else if (pathname === '/healthcheck') {
        res.writeHead(200, {'Content-Type': 'text/plain'});
        return res.end('ok');
    } else if (pathname === '/process-list') {
        exec('ps -ef -o pid,args', function (error, stdout, stderr) {
            if (stderr || error) {
                res.statusCode = 500;
                return res.end(stderr ? stderr.toString() : error);
            }
            var lines = stdout.toString().trim().split('\n');
            var results = [];
            for (var i = 1; i < lines.length; i++) {
                var parts = lines[i].trim().replace(/\s{2,}/g, ' ');
                var positionPid = parts.indexOf(' ');
                var cmd = parts.slice(positionPid + 1);
                var execname = cmd.replace('sudo ', '').split(' ')[0];
                results.push({
                    pid: parts.slice(0, positionPid),
                    cmd: cmd,
                    execname: execname.slice(execname.lastIndexOf('/') + 1)
                });
            }
            res.writeHead(200, {'Content-Type': 'application/json'}); 
            res.write(JSON.stringify(results));
            res.end();
        });
    } else {
        res.writeHead(404)
        res.end();
    }
});

httpsServer.listen(8000);
