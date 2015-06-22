'use strict';
var childProcess = require('child_process');
var exec = childProcess.exec;
var fs = require('fs');
var vasync = require('vasync');
var url = require('url');
var manta = require('manta');
var Readable = require('stream').Readable;

var httpsServer = require('https').createServer({
    ca: fs.readFileSync('./ca.pem'),
    cert: fs.readFileSync('./server-cert.pem'),
    key: fs.readFileSync('./server-key.pem'),
    rejectUnauthorized: false,
    requestCert: true
});

var PROCESS_KILLED_MESSAGE = 'process has been killed';
var DTRACE_ERROR = 'DTrace error: ';
var FLAMEGRAPH_PATH = '~~/stor/.joyent/devtools/flameGraph';
var COREDUMP_PATH = '~~/stor/.joyent/devtools/coreDump';
var DTRACE_DIRECTORY_ERROR = 'Check whether directory exists and has "dtrace" role tag: ';

var WebSocket = require('ws');
var wss = new WebSocket.Server({noServer: true});
var cache = {};
var runningTasks = {};
var taskConnections = {};
var IS_DEBUG = false;
var sshKey = '';
var sshPrivateKey = '';
var mantaOptions;

function loadMantaOptions() {
    exec("ssh-keygen -lf /root/.ssh/user_id_rsa.pub | awk '{print $2}'", function (err, key) {
        sshKey = !err ? key : {error: err};

        if (sshKey.error) {
            return;
        }

        sshPrivateKey = fs.readFileSync('/root/.ssh/user_id_rsa', 'utf8');
        mantaOptions = {
            user: process.env['MANTA_USER'],
            url: process.env['MANTA_URL'],
            subuser: process.env['MANTA_SUBUSER']
        };
        mantaOptions.sign = manta.privateKeySigner({
            key: sshPrivateKey,
            keyId: sshKey.replace('\n', ''),
            user: mantaOptions.user,
            subuser: mantaOptions.subuser
        });
    });
}

function log(message) {
    if (IS_DEBUG) {
        console.log(message);
    }
}

function deleteFile(filePath) {
    return fs.unlink(filePath, function (err) {
        if (err) {
            log(err);
        }
    });
}

function getParentDirectory(path) {
    return path.substring(0, path.lastIndexOf('/'));
}

/**
 * Writes a given file to the Manta storage
 * @param inputPath Local file path
 * @param outputPath Output file path in Manta
 * @param callback Callback function
 */
function putFileToManta(inputPath, outputPath, callback) {
    if (sshKey.error) {
        return callback(sshKey.error);
    }

    var mantaClient = manta.createClient(mantaOptions);
    var putSubDirectory = getParentDirectory(outputPath);
    var putDirectory = getParentDirectory(putSubDirectory);

    mantaClient.mkdir(putDirectory, function (err) {
        if (err) {
            return callback(new Error(DTRACE_DIRECTORY_ERROR + putDirectory));
        }
        mantaClient.mkdir(putSubDirectory, function (err) {
            if (err) {
                return callback(new Error(DTRACE_DIRECTORY_ERROR + putSubDirectory));
            }
            try {
                var putFileInManta = fs.createReadStream(inputPath)
                    .pipe(mantaClient.createWriteStream(outputPath));

                putFileInManta.on('end', function (err) {
                    callback(null, outputPath)
                });
            } catch (ex) {
                return callback(new Error(DTRACE_DIRECTORY_ERROR + putDirectory));
            }
        });
    });
}

function putFileContentsToManta(outputPath, data, callback) {
    if (sshKey.error) {
        return callback(sshKey.error);
    }

    var mantaClient = manta.createClient(mantaOptions);
    var stream = new Readable();
    stream._read = function noop() {
    };
    stream.push(JSON.stringify(data));
    stream.push(null);

    try {
        var putFileInManta = stream
            .pipe(mantaClient.createWriteStream(outputPath));

        putFileInManta.on('end', function () {
            callback(null, outputPath)
        });
    } catch (ex) {
        return callback(new Error(DTRACE_DIRECTORY_ERROR + outputPath));
    }
}

function deleteTempFiles(task) {
    deleteFile(__dirname + '/' + task.id + '.out');
    return deleteFile(__dirname + '/' + task.id + '.svg');
}

function runTask(task) {
    var process;
    vasync.waterfall([
        function (callback) {
            log('Running script');
            process = exec('dtrace ' + task.script + ' > ' + task.id + '.out', function (error) {
                if (process && process.killed) {
                    error = new Error(PROCESS_KILLED_MESSAGE);
                }
                callback(error);
            });
        },
        function (callback) {
            log('generating SVG');
            process = exec(__dirname + '/node_modules/stackvis/cmd/stackvis dtrace flamegraph-svg < ' + task.id + '.out > ' + task.id + '.svg',
                function (error) {
                    var filePath;
                    if (!error) {
                        if (process && process.killed) {
                            error = new Error(PROCESS_KILLED_MESSAGE);
                        } else {
                            filePath = task.id + '.svg';
                        }
                    }
                    callback(error, filePath);
                });
        },
        function (filePath, callback) {
            if (!task.notSaveResults) {
                log('putFileToManta');
                var mantaTaskFloder = FLAMEGRAPH_PATH + '/' + task.hostId + '/' + task.id + '/';
                var mantaTaskPath = mantaTaskFloder + new Date().toISOString() + '.svg';
                putFileToManta(__dirname + '/' + filePath, mantaTaskPath, function (err) {
                    if (!err && task.doneCount === 0) {
                        var taskInfo = {
                            id: task.id,
                            totalCount: task.totalCount,
                            startDate: task.startDate,
                            execname: task.execname,
                            pid: task.pid,
                            probeTime: task.probeTime,
                            processName: task.processName
                        };
                        log(task.ProcessName);
                        return putFileContentsToManta(mantaTaskFloder + 'info.json', taskInfo, function (err) {
                            callback(err, mantaTaskPath);
                        })
                    }
                    callback(err, mantaTaskPath);
                });
            } else {
                log('No saving .svg to the manta, sending it directly via websocket');
                fs.readFile(__dirname + '/' + filePath, {encoding: 'UTF8'}, function (err, data) {
                    if (err || !data) {
                        return callback(err, filePath);
                    }
                    sendFlameGraphProgress(task, {task: task, action: 'progress', svg: data});
                    callback(err, filePath);
                });
            }
        }
    ], function (err, filePath) {
        log('Done waterfall, file path:' + filePath);
        log('Error: ' + err);
        deleteTempFiles(task);
        if (process && process.kill) {
            process.kill();
        }
        if (err) {
            task.status = 'finished';
            task.error = DTRACE_ERROR + err.toString();
            return sendFlameGraphProgress(task, {task: task, action: 'progress'});
        }

        if (task.notSaveResults && taskConnections[task.id]) {
            return runTask(task);
        }

        task.doneCount += 1;
        if (task.doneCount >= task.totalCount) {
            task.status = 'finished';
        }
        sendFlameGraphProgress(task, {task: task, action: 'progress'});
        if (task.status === 'running') {
            runTask(task);
        }

    });
}

function startFlameGraphTask(task, connection) {
    if (!task.notSaveResults) {
        runningTasks[task.id] = task;
    }
    taskConnections[task.id] = connection;
    connection.taskId = task.id;

    log('Starting task');
    sendFlameGraphProgress(task, {task: task, action: 'progress'});
    runTask(task);
}

function toArray(data) {
    var result = [];
    for (var item in data) {
        result.push(runningTasks[item]);
    }
    return result;
}

function sendFlameGraphProgress(task, data) {
    if (typeof data === 'object') {
        data.type = 'flamegraph';
        data.tasks = toArray(runningTasks);
        data = JSON.stringify(data);
    }
    if (taskConnections[task.id]) {
        send(taskConnections[task.id].uuid, data);
        // Removing task from runnig if it is finished and were sent to the client
        if (task.status === 'finished') {
            delete runningTasks[task.id];
        }
    }
}

function getTaskById(id) {
    return runningTasks[id];
}

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
                log('Incoming flamegraph message: ' + message);
                if (message.action === 'start') {
                    var isoDate = new Date().toISOString();
                    var taskToStart = {
                        id: message.task.pid + '_' + isoDate,
                        execname: message.task.execname,
                        pid: message.task.pid,
                        totalCount: message.task.totalCount,
                        doneCount: 0,
                        probeTime: message.task.probeTime,
                        status: 'running',
                        script: message.task.script,
                        hostId: message.hostId,
                        processName: message.task.processName,
                        startDate: isoDate,
                        notSaveResults: message.task.notSaveResults
                    };
                    startFlameGraphTask(taskToStart, connection);
                } else {
                    var task = getTaskById(message.task.id);
                    if (!task) {
                        return;
                    }

                    if (message.action === 'stop') {
                        task.status = 'finished';
                        sendFlameGraphProgress(task,  {task: task, action: 'progress'});
                    } else if (message.action === 'observe') {
                        taskConnections[task.id] = connection;
                        connection.taskId = task.id;
                        sendFlameGraphProgress(task, {task: task, action: 'progress'});
                    }
                }
            } else if (message.type === 'coreDump') {
                var pid = message.message;

                vasync.waterfall([
                    function (callback) {
                        connection.process = exec('gcore ' + pid, callback);
                        send(uuid, 'started');
                    },
                    function (stdout, stderr, callback) {
                        if (stderr) {
                            return callback(stderr);
                        }
                        connection.process = putFileToManta(__dirname + '/core.' + pid, COREDUMP_PATH + '/core.' + pid, function (err, filePath) {
                            deleteFile(__dirname + '/core.' + pid);
                            callback(err, filePath)
                        });
                    },
                    function (filePath, callback) {
                        connection.process = exec("pgrep 'node'", function (err, stdout) {
                            if (err) {
                                return callback(err);
                            }

                            var nodePidList = stdout.split('\n');
                            var nodePid = nodePidList.filter(function (nodePid) {
                                return nodePid === pid;
                            });

                            var result = {path: filePath};
                            result.node = nodePid.length > 0;
                            callback(null, result);
                        });
                    }
                ], function (err, result) {
                    if (err) {
                        result = {error: err.toString()};
                    }
                    send(uuid, JSON.stringify(result));
                    killProcess(uuid);
                });
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
            delete taskConnections[connection.taskId];
            close(uuid);
            log('disconnected');
        });
    }
}

function killProcess(uuid) {
    var connection = cache[uuid];
    if (connection && connection.process) {
        connection.process.on('exit', function (code, signal) {
            log('dtrace process exited with code %s and signal %s', code, signal);
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
        log('Error: no websocket');
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
    } else if (pathname === '/tasks-list') {
        res.writeHead(200, {'Content-Type': 'application/json'});
        res.write(JSON.stringify(toArray(runningTasks)));
        res.end();
    } else {
        res.writeHead(404);
        res.end();
    }
});

httpsServer.listen(8000);

loadMantaOptions();
