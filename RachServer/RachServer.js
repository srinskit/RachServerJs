const WebSocket = require('ws');
const Url = require('url');
const uuid_v1 = require('uuid/v1');

function getIP(req) {
    const temp = (req.headers['x-forwarded-for'] || req.connection.remoteAddress).split(':');
    return temp[temp.length - 1];
}

class Node {
    constructor() {
        this.subscribers = {};
        this.publishers = {};
        this.children = {};
    }

    addSubscriber(client) {
        this.subscribers[client.id] = true;
    }

    addPublisher(client) {
        this.publishers[client.id] = true;
    }

    removeSubscriber(client) {
        delete this.subscribers[client.id];
    }

    removePublisher(client) {
        delete this.publishers[client.id];
    }

    addChild(path) {
        this.children[path] = new Node();
    }

    getChild(path) {
        return this.children[path];
    }

    publish(send) {
        for (let id in this.subscribers)
            if (this.subscribers.hasOwnProperty(id))
                send(id);
    }
}

class Tree {
    constructor() {
        this.root = new Node();
    }

    findNode(topic) {
        let path = topic.split('/');
        let root = this.root;
        for (let i = 1; i < path.length; ++i) {
            if (path[i].length === 0)
                continue;
            if (!(path[i] in root.children))
                root.addChild(path[i]);
            root = root.getChild(path[i]);
        }
        return root;
    }

    addSubscriber(client, topic, cb) {
        let root = this.findNode(topic);
        root.addSubscriber(client);
        cb(false);
    }

    addPublisher(client, topic, cb) {
        let root = this.findNode(topic);
        root.addPublisher(client);
        cb(false);
    }

    removeSubscriber(client, topic, cb) {
        let root = this.findNode(topic);
        root.removeSubscriber(client);
        cb(false);
    }

    removePublisher(client, topic, cb) {
        let root = this.findNode(topic);
        root.removePublisher(client);
        cb(false);
    }

    publish(topic, send) {
        let path = topic.split('/');
        let root = this.root;
        root.publish((id) => send('/', id));
        let current_topic = '';
        for (let i = 1; i < path.length; ++i) {
            if (path[i].length === 0)
                continue;
            if (!(path[i] in root.children))
                root.addChild(path[i]);
            root = root.getChild(path[i]);
            current_topic += '/' + path[i];
            root.publish((id) => send(current_topic, id));
        }
    }
}

class RachClient {
    constructor(id, ws) {
        this.id = id;
        this.ws = ws;
        if (this.id === '-1')
            return;
        this.ws.on('message', (msg) => {
            RachClient.actions['onMessage'](this, msg);
        });
        this.ws.on('close', () => {
            RachClient.actions['onClose'](this);
        });
        this.ws.on('error', (err) => {
            RachClient.actions['onError'](this, err);
        });
    }

    send(msg, callback) {
        if (this.ws == null) return;
        try {
            this.ws.send(msg, callback);
        } catch (err) {
            // console.error(err);
            // RachClient.actions['removeClient'](this);
        }
    }
}

class RachServer {

    constructor(actions, logger) {
        this.actions = actions;
        this.logger = logger;
        RachClient.actions = {
            onMessage: (client, msg) => this.onMessage(client, msg),
            onClose: (client) => this.onClose(client),
            onError: (client, err) => this.onError(client, err),
        };
        this.wss = null;
        this.tree = new Tree();
        this.services = actions['services'];
        this.local_callbacks = {};
        this.local_client = this.makeLocalClient();
        this.clients = {[this.local_client.id]: this.local_client};
    }

    start() {
        this.wss = new WebSocket.Server({
            port: 8080
        });
        this.wss.on('connection', (ws, req) => this.onConnectionCallback(ws, req));
        this.logger.info('Starting RachServer');
    }

    stop() {
        this.logger.info('Stopping RachServer');
        this.wss.close();
    }

    onConnectionCallback(ws, req) {
        if (!('authTest' in this.actions)) {
            this.logger.info('authTest action not found');
            ws.close();
            return;
        }
        let cred = Url.parse(req.url, true).query;
        cred['ip'] = getIP(req);
        if (this.actions['authTest'](cred)) {
            let id = uuid_v1();
            let msg = {type: 'auth', verbose: 'Passed auth test', data: {success: true, id: id}};
            this.logger.info(msg.verbose);
            let client = new RachClient(id, ws);
            client.send(JSON.stringify(msg));
            this.clients[id] = client;
        } else {
            let msg = {type: 'auth', verbose: 'Failed auth test', data: {success: false}};
            this.logger.warn(msg.verbose);
            ws.send(JSON.stringify(msg));
            ws.close();
        }
    }

    onMessage(client, msg) {
        try {
            msg = JSON.parse(msg);
        } catch (e) {
            client.send(JSON.stringify({type: 'err', verbose: 'Invalid JSON received'}));
            return;
        }
        if ('matcher' in msg && 'type' in msg && 'data' in msg) {
            this.process(client, msg);
        } else {
            client.send(JSON.stringify({type: 'err', verbose: 'Invalid msg'}));
        }
    }

    onClose(client) {

    }

    onError(client, err) {

    }

    process(client, req) {
        // Todo: Check for injection attacks
        switch (req.type) {
            case 'service': {
                if ('topic' in req.data && 'args' in req.data) {
                    if (req.data.topic in this.services) {
                        this.services[req.data.topic].apply(null, [this,
                            (err) => {
                                let res = {
                                    matcher: req.matcher,
                                    type: 'err',
                                    verbose: `Service error: ${err}`
                                };
                                client.send(JSON.stringify(res));
                            },
                            (result) => {
                                let res = {
                                    matcher: req.matcher,
                                    type: 'service',
                                    data: {result: result},
                                    verbose: 'Service response'
                                };
                                client.send(JSON.stringify(res));
                            }].concat(req.data.args));
                    } else {
                        let res = {matcher: req.matcher, type: 'err', verbose: 'Service unavailable'};
                        client.send(JSON.stringify(res));
                    }
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Service topic missing'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            case 'addSub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep subs
                    this.tree.addSubscriber(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of subscription failed'};
                            client.send(JSON.stringify(res));
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Added subscription'};
                            client.send(JSON.stringify(res));
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of subscription failed'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            case 'rmSub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep subs
                    this.tree.removeSubscriber(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of subscription failed'};
                            client.send(JSON.stringify(res));
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Removed subscription'};
                            client.send(JSON.stringify(res));
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of subscription failed'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            case 'addPub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep pubs
                    this.tree.addPublisher(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of publication failed'};
                            client.send(JSON.stringify(res));
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Added publication'};
                            client.send(JSON.stringify(res));
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of publication failed'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            case 'rmPub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep pubs
                    this.tree.removePublisher(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of publication failed'};
                            client.send(JSON.stringify(res));
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Removed publication'};
                            client.send(JSON.stringify(res));
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of publication failed'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            case 'pub': {
                if ('topic' in req.data) {
                    this.tree.publish(req.data.topic, (topic, id) => {
                        let res = {
                            type: 'pub',
                            verbose: 'A publish',
                            data: {
                                data: req.data.data,
                                source_topic: req.data.topic,
                                topic: topic
                            },
                        };
                        this.clients[id].send(JSON.stringify(res));
                    });

                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Publish failed'};
                    client.send(JSON.stringify(res));
                }
            }
                break;
            default:
                let res = {matcher: req.matcher, type: 'err', verbose: 'Invalid request type '};
                client.send(JSON.stringify(res));
                break;
        }
    }

    static format_topic(topic) {
        if (topic[0] !== '/')
            topic = '/' + topic;
        if (topic[topic.length - 1] === '/')
            topic = topic.substr(0, topic.length - 1);
        return topic;
    }

    addSub(topic, cb, args) {
        topic = RachServer.format_topic(topic);
        this.local_callbacks[topic] = [cb, args];
        this.process(this.local_client, {data: {topic: topic}, matcher: '0', type: 'addSub'});
    }

    rmSub(topic) {
        topic = RachServer.format_topic(topic);
        this.process(this.local_client, {data: {topic: topic}, matcher: '0', type: 'rmSub'});
        delete this.local_callbacks[topic];
    }

    addPub(topic) {

    }

    rmPub(topic) {

    }

    pub(topic, data) {
        topic = RachServer.format_topic(topic);
        this.process(this.local_client, {data: {topic: topic, data: data}, matcher: '0', type: 'pub'});
    }

    makeLocalClient() {
        return new RachClient('-1', {
            send: (msg, cb) => {
                let req = JSON.parse(msg);
                if (cb != null)
                    cb();
                if (req.type === 'pub') {
                    if (req.data.topic in this.local_callbacks) {
                        let tmp = this.local_callbacks[req.data.topic];
                        let cb = tmp[0];
                        let args = tmp[1];
                        if (args != null)
                            cb.apply(null, [req.data.data].concat(args));
                        else
                            cb.apply(null, [req.data.data]);
                    }
                }
            }
        });
    }
}

module.exports = RachServer;