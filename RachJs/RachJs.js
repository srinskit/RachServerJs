/** @module Rach */

/** Class representing Rach client */
class Rach {
    /**
     * Create Rach client
     * @param {string} path - The URL of Rach Server
     * @param {object} cred - The credentials used for authentication
     */
    constructor(path, cred) {
        this.sock = null;
        this.cred = cred;
        this.server_path = path;
        this.debug = false;
        this.ns = '/';
        this.matcher = 0;
        this.callbacks = {};
        this.request_map = {};
        this.pub_topics = {};
        this.sub_topics = {};
    }

    /**
     * Add parameters to the Rach Server path
     * @param {string} path - The URL of Rach Server
     * @param {object} cred - The credentials used for authentication
     * @return {string} The parameterized path to Rach Server
     */
    static prep_path(path, cred) {
        return path + `?type=${cred['type']}&username=${cred['username']}&password=${cred['password']}`;
    }

    /**
     * Connect to the Rach Server
     * @param {function, optional} on_start - The callback to call when connection established
     */
    start(on_start) {
        let path = Rach.prep_path(this.server_path, this.cred);
        this.sock = new WebSocket(path);
        this.sock.onopen = (event) => {
            this.ws_on_open(event);
            if (on_start)
                on_start();
        };
        this.sock.onmessage = (event) => this.ws_on_message(event);
        this.sock.onerror = (event) => this.ws_on_error(event);
        this.sock.onclose = (event) => this.ws_on_close(event);
    }

    /**
     * Disconnect from the Rach Server
     */
    stop() {
        this.rm_all_sub();
        this.rm_all_pub();
        if (this.sock != null)
            this.sock.close();
    }

    /**
     * Send msg to Rach Server
     * @param {object} msg - The message to send
     */
    send(msg) {
        let str_msg = JSON.stringify(msg);
        this.log('ws send msg: ' + str_msg);
        this.sock.send(str_msg);
    }

    /**
     * Callback to handle websocket connection open
     * @param {object} event - The websocket event
     */
    ws_on_open(event) {
        this.log('ws opened');
    }

    /**
     * Callback to handle message from websocket
     * @param {object} event - The websocket event
     */
    ws_on_message(event) {
        let str_msg = event.data;
        try {
            let msg = JSON.parse(str_msg);
            this.process_msg(msg);
            this.log(`ws msg: ${str_msg}`);
        } catch (e) {
            this.log(`ws msg decode error: ${str_msg}, ${e}`);
        }
    }

    /**
     * Callback to handle websocket error
     * @param {object} event - The websocket event
     */
    ws_on_error(event) {
        this.log('ws error');
    }

    /**
     * Callback to handle websocket close
     * @param {object} event - The websocket event
     */
    ws_on_close(event) {
        this.log('ws closed');
    }

    /**
     * Print debug messages
     * @param {string} msg - The debug message
     */
    log(msg) {
        if (this.debug)
            console.log(msg);
    }

    /**
     * Enable printing of debug messages
     */
    enable_debug() {
        this.debug = true;
    }

    /**
     * Disable printing of debug messages
     */
    disable_debug() {
        this.debug = false;
    }

    /**
     * Process received message
     * @param {object} msg - Process received message
     */
    process_msg(msg) {
        let typ = msg.type;
        let matcher = msg.matcher;

        if (typ == null) {
            this.log('process_msg: msg lacks type');
            return;
        }
        if (typ === 'auth') {
            if (!((msg.data || {}).success || false))
                this.stop();
        } else if (typ === 'err') {
            if (matcher != null && matcher in this.request_map) {
                let tmp = this.request_map[matcher];
                delete this.request_map[matcher];
                let cb = tmp[2];
                let args = tmp[3];
                let err_msg = msg['verbose'];
                if (args != null) {
                    cb.apply(null, [err_msg].concat(args));
                } else {
                    cb(err_msg);
                }
            }
        } else if (typ === 'ack') {
            if (matcher != null && matcher in this.request_map) {
                let tmp = this.request_map[matcher];
                delete this.request_map[matcher];
                let cb = tmp[0];
                let args = tmp[1];
                cb.apply(null, args);
            }
        } else if (typ === 'pub') {
            this.process_pub(msg.data || null);
        } else if (typ === 'service') {
            if (matcher != null && matcher in this.request_map) {
                let data = msg['data'];
                let tmp = this.request_map[matcher];
                delete this.request_map[matcher];
                let cb = tmp[0];
                let args = tmp[1];
                if (data != null) {
                    cb.apply(null, [data].concat(args));
                } else {
                    cb.apply(null, args);
                }
            }
        }
    }

    /**
     * Generate unique string every call
     * @return (string) The unique string
     */
    make_req_matcher() {
        this.matcher += 1;
        return String(this.matcher);
    }

    /**
     * Set namespace of Rach Client
     * @param {string} ns - The namespace
     */
    set_namespace(ns) {
        if (ns.length > 1 && ns[ns.length - 1] === '/')
            ns = ns.substr(0, ns.length - 1);
        if (ns[0] !== '/')
            ns = '/' + ns;
        this.ns = ns;
    }

    /**
     * Get fully qualified topic name
     * @param {string} topic - The topic
     */
    get_fully_qualified_topic(topic) {
        if (topic.length > 1 && topic[topic.length - 1] === '/')
            topic = topic.substr(0, topic.length - 1);
        if (topic[0] === '/')
            return topic;
        else
            return this.ns + (this.ns === '/' ? '' : '/') + topic;
    }

    /**
     * Callback for addition of subscription callback and it's args
     * @param {Rach} rach - The Rach client
     * @param {string} topic - The subscription topic
     * @param {function} callback - The subscription callback
     * @param {[]} args - The additional arguments to be passed to the subscription callback
     */
    static add_callback(rach, topic, callback, args) {
        rach.callbacks[topic] = [callback, args];
        rach.sub_topics[topic] = true;
    }

    /**
     * Callback for removal of subscription callback and it's args
     * @param {Rach} rach - The Rach client
     * @param {string} topic - The subscription topic
     */
    static rm_callback(rach, topic) {
        delete rach.callbacks[topic];
        delete rach.sub_topics[topic];
    }

    /**
     * Callback for approval of publicise
     * @param {Rach} rach - The Rach client
     * @param {string} topic - The publish topic
     */
    static add_pub_callback(rach, topic) {
        rach.pub_topics[topic] = true;
    }

    /**
     * Callback for removal of publicise
     * @param {Rach} rach - The Rach client
     * @param {string} topic - The publish topic
     */
    static rm_pub_callback(rach, topic) {
        delete rach.pub_topics[topic];
    }

    /**
     * Callback that does nothing
     * @param {Rach} rach - The Rach client
     */
    static empty_callback(rach) {

    }

    /**
     * Request to subscribe to a topic
     * @param {string} topic - The subscription topic
     * @param {function} callback - The callback to call when event occurs at <topic>
     * @param {[]} args - The arguments to pass to the callback in addition to data
     */
    add_sub(topic, callback, args) {
        // Todo: Support for multiple sub requests
        topic = this.get_fully_qualified_topic(topic);
        if (topic in this.sub_topics) {
            this.log(`Already subscribed to ${topic}`);
            return;
        }
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'addSub',
            'matcher': matcher,
            'data': {
                'topic': topic
            }
        };
        this.request_map[matcher] = [
            Rach.add_callback, [this, topic, callback, args], Rach.empty_callback, [this]
        ];
        this.send(msg);
    }

    /**
     * Request to remove a subscription to a topic
     * @param {string} topic - The topic
     */
    rm_sub(topic) {
        topic = this.get_fully_qualified_topic(topic);
        if (!(topic in this.sub_topics)) {
            this.log(`Not subscribed to ${topic}`);
            return;
        }
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'rmSub',
            'matcher': matcher,
            'data': {
                'topic': topic
            }
        };
        this.request_map[matcher] = [
            Rach.rm_callback, [this, topic], Rach.empty_callback, [this]
        ];
        this.send(msg);
    }

    /**
     * Request to publish to a topic
     * @param {string} topic - The topic
     */
    add_pub(topic) {
        topic = this.get_fully_qualified_topic(topic);
        if (topic in this.pub_topics) {
            this.log(`Already registered to publish to ${topic}`);
            return;
        }
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'addPub',
            'matcher': matcher,
            'data': {
                'topic': topic
            }
        };
        this.request_map[matcher] = [
            Rach.add_pub_callback, [this, topic], Rach.empty_callback, [this]
        ];
        this.send(msg);
        return new Rach.Publisher(topic, this);
    }

    /**
     * Request to stop publishing to a topic
     * @param {string} topic - The topic
     */
    rm_pub(topic) {
        topic = this.get_fully_qualified_topic(topic);
        if (!(topic in this.pub_topics)) {
            this.log(`Not publishing to ${topic}`);
            return;
        }
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'rmPub',
            'matcher': matcher,
            'data': {
                'topic': topic
            }
        };
        this.request_map[matcher] = [
            Rach.rm_pub_callback, [this, topic], Rach.empty_callback, [this]
        ];
        this.send(msg);
    }

    /**
     * Request to publish to a topic
     * @param {string} topic - The topic
     * @param {object} data - The data to publish
     */
    pub(topic, data) {
        topic = this.get_fully_qualified_topic(topic);
        if (!(topic in this.pub_topics)) {
            this.log(`Not publishing to ${topic}`);
            return;
        }
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'pub',
            'matcher': matcher,
            'data': {
                'topic': topic,
                'data': data
            }
        };
        this.send(msg);
    }

    /**
     * Process data received from subscription
     * @param {object} data - The data received
     */
    process_pub(data) {
        if (data == null || data.topic == null) {
            return;
        }
        let tmp = this.callbacks[data.topic] || [null, null];
        let cb = tmp[0];
        let args = tmp[1];
        if (cb == null)
            return;
        if (args == null)
            cb(data);
        else
            cb.apply(null, [data].concat(args));
    }

    /**
     * Request removal of all subscription
     */
    rm_all_sub() {
        for (let topic in this.sub_topics)
            if (this.sub_topics.hasOwnProperty(topic))
                this.rm_sub(topic);
    }

    /**
     * Request withdrawal of all publicise requests
     */
    rm_all_pub() {
        for (let topic in this.pub_topics)
            if (this.pub_topics.hasOwnProperty(topic))
                this.rm_pub(topic);
    }

    /**
     * Call a service
     * @param {string} topic - The service topic
     * @param {[]} args - The service arguments
     * @param {function} on_result - The callback to call when service call returns
     * @param {[]} on_result_args - The parameters to pass to on_result
     * @param {function} on_err - The callback to call when service call throws exception
     * @param {[]} on_err_args - The parameters to pass to on_err
     */
    service_call(topic, args, on_result, on_result_args, on_err, on_err_args) {
        let matcher = this.make_req_matcher();
        let msg = {
            'type': 'service',
            'matcher': matcher,
            'data': {
                'topic': topic,
                'args': args,
            }
        };
        this.request_map[matcher] = [
            on_result, on_result_args, on_err, on_err_args
        ];
        this.send(msg);
    }
}

/** Class used to represent a publisher to a topic */
Rach.Publisher = class {
    /**
     * Create a publisher
     * @param {string} topic - The topic the publisher publishes at
     * @param {Rach} rach - The Rach client to publish through
     */
    constructor(topic, rach) {
        this.rach = rach;
        this.topic = topic;
    }

    /**
     * Publish data to Rach server
     * @param {object} data - The data to publish
     */
    pub(data) {
        this.rach.pub(this.topic, data);
    }

    /**
     * Destroy the publisher
     */
    close() {
        this.rach.rm_pub(this.topic);
    }
};

module.exports = Rach;