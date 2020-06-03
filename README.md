# Rach Server

[![NPM](https://nodei.co/npm/rach-server.png)](https://nodei.co/npm/rach-server/)

## Install from NPM
```
npm install rach-server
```
## Usage
Sample code. Run using `node app.js`
```
// app.js

const RachServer = require('rach-server');

// Modify internal actions used by Rach
const actions = {
    'authTest': function (cred) {
        return cred["username"] === "su" && cred["password"] === "pass";
    },
};

// Analogous to Remote Procedure Calls
const services = {
    '/version':
        function (rach, client, on_err, on_result) {
            on_result('1.0');
        },
    '/client.public_id':
        function (rach, client, on_err, on_result) {
            on_result(client.public_id);
        },
    '/remainder':
        function (rach, client, on_err, on_result, x, y) {
            try {
                on_result(x % y);
            } catch (e) {
                on_err(e.message || 'Generic service error');
            }
        },
};

const rachServer = new RachServer(actions, services, console, 8080);

// Add a local subscription
rachServer.addSub('/one/two', function (data) {
    console.info(data);
}, []);

rachServer.start();
```