var libdtrace = require('libdtrace');

process.on('message', function (data) {
    var type = data.type;
    var message = data.message;
    if (type === 'heatmap') {
        var dtp = new libdtrace.Consumer();
        dtp.strcompile(message);
        dtp.go();

        /* All that's left to do is send the aggration data from the dscript. */
        setInterval(function () {
            var aggdata = {};
            try {
                dtp.aggwalk(function (id, key, val) {
                    for( index in val ) {
                        aggdata[key] = val;
                    }
                });
                process.send(JSON.stringify(aggdata));
            } catch(err) {
                console.log('dtp aggwalk error: ', err);
            }
        }, 1101);
    }
});

