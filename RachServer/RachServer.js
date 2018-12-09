/** @module RachServer */

const WebSocket = require('ws');
const Url = require('url');
const uuid_v1 = require('uuid/v1');

/**
 * Get IP of a HTTP/HTTPS request
 * @param {object} req The request
 * @returns {string, undefined} IP of request
 */
function getIP(req) {
    const temp = (req.headers['x-forwarded-for'] || req.connection.remoteAddress).split(':');
    try {
        return temp[temp.length - 1];
    } catch (e) {
        return undefined;
    }
}

/** Class representing a Node in the Topic Tree */
class Node {
    /**
     * Create a node
     */
    constructor() {
        this.subscribers = {};
        this.publishers = {};
        this.children = {};
    }

    /**
     * Add client as subscriber at this node
     * @param {RachClient} client - The client
     */
    addSubscriber(client) {
        this.subscribers[client.id] = true;
    }

    /**
     * Add client as publisher at this node
     * @param {RachClient} client - The client
     */
    addPublisher(client) {
        this.publishers[client.id] = true;
    }

    /**
     * Remove client as subscriber if subscribed
     * @param {RachClient} client - The client
     */
    removeSubscriber(client) {
        delete this.subscribers[client.id];
    }

    /**
     * Remove client as publisher if publicised
     * @param {RachClient} client - The client
     */
    removePublisher(client) {
        delete this.publishers[client.id];
    }

    /**
     * Create new child node at given branch name
     * @param {string} path - The branch name
     */
    addChild(path) {
        this.children[path] = new Node();
    }

    /**
     * Get child node at given branch
     * @param {string} path - The branch name
     * @return {Node, undefined} - The Child node
     */
    getChild(path) {
        return this.children[path];
    }

    /**
     * Call the provided callback on all of the node's subscribers with client id as argument
     * @param {function(string)} callback - The callback accepting the client's id
     */
    publish(callback) {
        for (let id in this.subscribers)
            if (this.subscribers.hasOwnProperty(id))
                callback(id);
    }
}

/** Class representing the Topic Tree */
class Tree {
    /**
     * Create a Topic tree with just root node
     */
    constructor() {
        this.root = new Node();
    }

    /**
     * Get the tree node representing the given topic
     * @param {string} topic - The topic
     * @return {Node} The node representing given topic
     */
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

    /**
     * Add subscriber to Topic Tree
     * @param {RachClient} client - The subscribing client
     * @param {string} topic - The topic to subscribe client to
     * @param {function(boolean)} callback - The callback is called with the error
     */
    addSubscriber(client, topic, callback) {
        let root = this.findNode(topic);
        root.addSubscriber(client);
        callback(false);
    }

    /**
     * Add publisher to Topic Tree
     * @param {RachClient} client - The publicising client
     * @param {string} topic - The topic which client will publish to
     * @param {function(boolean)} callback - The callback is called with the error
     */
    addPublisher(client, topic, callback) {
        let root = this.findNode(topic);
        root.addPublisher(client);
        callback(false);
    }


    /**
     * Remove subscriber from Topic Tree
     * @param {RachClient} client - The subscribed client
     * @param {string} topic - The topic to which client is subscribed
     * @param {function(boolean)} callback - The callback is called with the error
     */
    removeSubscriber(client, topic, callback) {
        let root = this.findNode(topic);
        root.removeSubscriber(client);
        callback(false);
    }


    /**
     * Remove publisher from Topic Tree
     * @param {RachClient} client - The publicising client
     * @param {string} topic - The topic which client is publishing to
     * @param {function(boolean)} callback - The callback is called with the error
     */
    removePublisher(client, topic, callback) {
        let root = this.findNode(topic);
        root.removePublisher(client);
        callback(false);
    }


    /**
     * Call a callback on all subscribers of the topic and it's parent topics
     * @param {string} topic - The topic to publish to
     * @param {function(string, string)} callback - The callback to be called on subscribers
     */
    publish(topic, callback) {
        let path = topic.split('/');
        let root = this.root;
        root.publish((id) => callback('/', id));
        let current_topic = '';
        for (let i = 1; i < path.length; ++i) {
            if (path[i].length === 0)
                continue;
            if (!(path[i] in root.children))
                root.addChild(path[i]);
            root = root.getChild(path[i]);
            current_topic += '/' + path[i];
            root.publish((id) => callback(current_topic, id));
        }
    }
}

/** Class representing Rach client */
class RachClient {
    /**
     * Create a Rach client
     * @param {string} id - Unique id identifying a client
     * @param {object} ws - Websocket used to connect to client
     */
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

    /**
     * Send a message to the Rach client
     * @param {string} msg - The message to send
     * @param {function} callback - The callback called when done
     */
    send(msg, callback) {
        if (this.ws == null) return;
        try {
            this.ws.send(msg, callback);
        } catch (err) {
        }
    }
}

/** Class representing Rach server */
class RachServer {

    /**
     * Create a Rach Server
     * @param {object} actions - The map mapping actions to callbacks
     * @param {object} services - The map mapping services to callbacks
     * @param {object} logger - The logger instance to be used for logging
     */
    constructor(actions, services, logger) {
        this.actions = actions;
        this.services = services;
        this.logger = logger;
        RachClient.actions = {
            onMessage: (client, msg) => this.onMessage(client, msg),
            onClose: (client) => this.onClose(client),
            onError: (client, err) => this.onError(client, err),
        };
        this.wss = null;
        this.tree = new Tree();
        this.local_callbacks = {};
        this.local_client = this.makeLocalClient();
        this.clients = {[this.local_client.id]: this.local_client};
    }

    /**
     * Start the Rach server
     */
    start() {
        this.wss = new WebSocket.Server({
            port: 8080
        });
        this.wss.on('connection', (ws, req) => this.onConnectionCallback(ws, req));
        this.logger.info('Starting RachServer');
    }

    /**
     * Stop the Rach server
     */
    stop() {
        this.logger.info('Stopping RachServer');
        this.wss.close();
    }

    /**
     * Handle incoming websocket connection and creates Rach clients
     * @param {object} ws - The websocket client
     * @param {object} req - The request used by websocket client
     */
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
            client.send(JSON.stringify(msg), undefined);
            this.clients[id] = client;
        } else {
            let msg = {type: 'auth', verbose: 'Failed auth test', data: {success: false}};
            this.logger.warn(msg.verbose);
            ws.send(JSON.stringify(msg));
            ws.close();
        }
    }

    /**
     * Handle incoming messages
     * @param {RachClient} client - The Rach client representing source of message
     * @param {string} msg - The raw message received
     */
    onMessage(client, msg) {
        try {
            msg = JSON.parse(msg);
        } catch (e) {
            client.send(JSON.stringify({type: 'err', verbose: 'Invalid JSON received'}), undefined);
            return;
        }
        if ('matcher' in msg && 'type' in msg && 'data' in msg) {
            this.process(client, msg);
        } else {
            client.send(JSON.stringify({type: 'err', verbose: 'Invalid msg'}), undefined);
        }
    }

    /**
     * Handle closing of connection with Rach client
     * @param {object} client - The Rach client
     */
    onClose(client) {

    }

    /**
     * Handle error in connection with Rach client
     * @param {object} client - The Rach client
     * @param {object} err - The error
     */
    onError(client, err) {

    }

    /**
     * Process a Rach client's request to Rach Server
     * @param {RachClient} client - The Rach client
     * @param {object} req - The request in Rach Request Format
     */
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
                                client.send(JSON.stringify(res), undefined);
                            },
                            (result) => {
                                let res = {
                                    matcher: req.matcher,
                                    type: 'service',
                                    data: {result: result},
                                    verbose: 'Service response'
                                };
                                client.send(JSON.stringify(res), undefined);
                            }].concat(req.data.args));
                    } else {
                        let res = {matcher: req.matcher, type: 'err', verbose: 'Service unavailable'};
                        client.send(JSON.stringify(res), undefined);
                    }
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Service topic missing'};
                    client.send(JSON.stringify(res), undefined);
                }
            }
                break;
            case 'addSub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep subs
                    this.tree.addSubscriber(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of subscription failed'};
                            client.send(JSON.stringify(res), undefined);
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Added subscription'};
                            client.send(JSON.stringify(res), undefined);
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of subscription failed'};
                    client.send(JSON.stringify(res), undefined);
                }
            }
                break;
            case 'rmSub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep subs
                    this.tree.removeSubscriber(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of subscription failed'};
                            client.send(JSON.stringify(res), undefined);
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Removed subscription'};
                            client.send(JSON.stringify(res), undefined);
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of subscription failed'};
                    client.send(JSON.stringify(res), undefined);
                }
            }
                break;
            case 'addPub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep pubs
                    this.tree.addPublisher(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of publication failed'};
                            client.send(JSON.stringify(res), undefined);
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Added publication'};
                            client.send(JSON.stringify(res), undefined);
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Addition of publication failed'};
                    client.send(JSON.stringify(res), undefined);
                }
            }
                break;
            case 'rmPub': {
                if ('topic' in req.data) {
                    // Todo: Gate-keep pubs
                    this.tree.removePublisher(client, req.data.topic, (err) => {
                        if (err) {
                            let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of publication failed'};
                            client.send(JSON.stringify(res), undefined);
                        } else {
                            let res = {matcher: req.matcher, type: 'ack', verbose: 'Removed publication'};
                            client.send(JSON.stringify(res), undefined);
                        }
                    });
                } else {
                    let res = {matcher: req.matcher, type: 'err', verbose: 'Removal of publication failed'};
                    client.send(JSON.stringify(res), undefined);
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
                    client.send(JSON.stringify(res), undefined);
                }
            }
                break;
            default:
                let res = {matcher: req.matcher, type: 'err', verbose: 'Invalid request type '};
                client.send(JSON.stringify(res), undefined);
                break;
        }
    }

    /**
     * Format a topic to fully-qualified form
     * @param {string} topic - The topic
     * @return {string} The fully-qualified form of given topic
     */
    static format_topic(topic) {
        if (topic[0] !== '/')
            topic = '/' + topic;
        if (topic[topic.length - 1] === '/')
            topic = topic.substr(0, topic.length - 1);
        return topic;
    }

    /**
     * Add a local subscriber
     * @param {string} topic - The topic to subscribe to
     * @param {function} callback - The callback to call when event occurs at subscribed topic
     * @param {Array} args - The arguments to pass to callback in addition to event data
     */
    addSub(topic, callback, args) {
        topic = RachServer.format_topic(topic);
        this.local_callbacks[topic] = [callback, args];
        this.process(this.local_client, {data: {topic: topic}, matcher: '0', type: 'addSub'});
    }

    /**
     * Remove a local subscriber
     * @param {string} topic - The topic to remove subscription from
     */
    rmSub(topic) {
        topic = RachServer.format_topic(topic);
        this.process(this.local_client, {data: {topic: topic}, matcher: '0', type: 'rmSub'});
        delete this.local_callbacks[topic];
    }

    /**
     * Add a local publisher
     * @param {string} topic - The topic to publicise tp
     */
    addPub(topic) {

    }

    /**
     * Remove a local publisher
     * @param {string} topic - The topic to stop publishing to
     */
    rmPub(topic) {

    }

    /**
     * Publish as the local publisher
     * @param {string} topic - The topic to publish to
     * @param {object} data - The data to publish
     */
    pub(topic, data) {
        topic = RachServer.format_topic(topic);
        this.process(this.local_client, {data: {topic: topic, data: data}, matcher: '0', type: 'pub'});
    }

    /**
     * Make a local client
     * @return {RachClient} The local client
     */
    makeLocalClient() {
        return new RachClient('-1', {
            send: (msg, callback) => {
                let req = JSON.parse(msg);
                if (callback != null)
                    callback();
                if (req.type === 'pub') {
                    if (req.data.topic in this.local_callbacks) {
                        let tmp = this.local_callbacks[req.data.topic];
                        let callback = tmp[0];
                        let args = tmp[1];
                        if (args != null)
                            callback.apply(null, [req.data.data].concat(args));
                        else
                            callback.apply(null, [req.data.data]);
                    }
                }
            }
        });
    }
}

module.exports = RachServer;