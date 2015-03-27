var http = require('http');
var express = require('express');
var path = require('path');
var childProcess = require('child_process');
var exec = childProcess.exec;
var fs = require('fs');
var manta = require('manta');
var MemoryStream = require('memorystream');
var async = require('async');

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

    ws.pingssent = 0;
    var ping = setInterval(function() {
        if (ws.pingssent >= 2) {
            ws.close();
        } else {
            ws.ping();
            ws.pingssent++;
        }
    }, 20 * 1000);

    ws.on("pong", function() {
        ws.pingssent = 0;
    });

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
        } else if (message.type === 'coreDump') {
            var pid = message.message;
            var sendError = function (error) {
                try {
                    ws.send('Error: ' + error.toString());
                } catch (err) {
                    console.log(err);
                }
            }

            exec('gcore ' + pid, function (err, stdout, stderr) {
                if (err) {
                    return sendError(err);
                }

                exec("ssh-keygen -lf /root/.ssh/user_id_rsa.pub | awk '{print $2}'", function (error, key, stderr) {

                    var mantaConf = {};

                    var mapMdataKeys = {
                        MANTA_USER: 'manta-account',
                        MANTA_URL: 'manta-url',
                        MANTA_SUBUSER: 'manta-subuser'
                    };

                    function getMetadata(key, callback) {
                        exec('/usr/sbin/mdata-get ' + key, callback);
                    }

                    mantaConf.MANTA_KEY_ID = key.replace('\n', '');

                    var funcs = [];                    
                    Object.keys(mapMdataKeys).forEach(function (key) {
                        funcs.push(function (callback) {
                            getMetadata(mapMdataKeys[key], function (error, mdata) {
                                mantaConf[key] = mdata.replace('\n', '');
                                callback(error);
                            });
                        });
                    });

                    async.parallel(funcs, function (err) {
                        if (err) {
                            return sendError(err);
                        }
                        var mantaClient = manta.createClient({
                            sign: manta.privateKeySigner({
                                key: fs.readFileSync('/root/.ssh/user_id_rsa', 'utf8'),
                                keyId: mantaConf.MANTA_KEY_ID,
                                user: mantaConf.MANTA_USER,
                                subuser: mantaConf.MANTA_SUBUSER
                            }),
                            user: mantaConf.MANTA_USER,
                            subuser: mantaConf.MANTA_SUBUSER,
                            url: mantaConf.MANTA_URL
                        });

                        var filePath = '~~/stor/.joyent/dtrace/coreDump/core.' + pid;

                        var putFileInManta = fs.createReadStream(__dirname + '/core.' + pid)
                            .pipe(mantaClient.createWriteStream(filePath));

                        putFileInManta.on('end', function() {
                            exec("pgrep 'node'", function (err, stdout, stderr) {
                                if (err) {
                                    return sendError(err);
                                }

                                var nodePidList = stdout.split('\n');
                                var nodePid = nodePidList.filter(function (nodePid) {
                                    return nodePid === pid;
                                });

                                var result = {path: filePath};
                                result.node = nodePid.length > 0;

                                ws.send(JSON.stringify(result));
                            });
                        });
                    });
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
        if (ping) {
            clearInterval(ping);
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
