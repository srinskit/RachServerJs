class Rach {
    constructor(path, cred) {
        this.sock = null;
        this.cred = cred;
        this.server_path = path;
        this.debug = false;
        this.killed = false;
        this.ns = '/';
        this.matcher = 0;
        this.callbacks = {};
        this.request_map = {};
        this.pub_topics = {};
        this.sub_topics = {};
    }

    static prep_path(path, cred) {
        return path + `?type=${cred['type']}&username=${cred['username']}&password=${cred['password']}`;
    }

    start() {
        this.killed = false;
        let path = Rach.prep_path(this.server_path, this.cred);
        this.sock = new WebSocket(path);
        this.sock.onopen = (event) => this.ws_on_open(event);
        this.sock.onmessage = (event) => this.ws_on_message(event);
        this.sock.onerror = (event) => this.ws_on_error(event);
        this.sock.onclose = (event) => this.ws_on_close(event);
    }

    stop() {
        this.killed = true;
        this.rm_all_sub();
        this.rm_all_pub();
        if (this.sock != null)
            this.sock.close();
    }

    send(msg) {
        let str_msg = JSON.stringify(msg);
        this.log('ws send msg: ' + str_msg);
        this.sock.send(str_msg);
    }

    ws_on_open(event) {
        this.log('ws opened');
    }

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

    ws_on_error(event) {
        this.log('ws error');
    }

    ws_on_close(event) {
        this.log('ws closed');
    }

    log(msg) {
        if (this.debug)
            console.log(msg);
    }

    enable_debug() {
        this.debug = true;
    }

    disable_debug() {
        this.debug = false;
    }

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
                cb.apply(null, args);
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
                let tmp = this.request_map[matcher];
                delete this.request_map[matcher];
                let cb = tmp[0];
                let args = tmp[1];
                cb.apply(null, [msg.data].concat(args));
            }
        }
    }


    sock_manager() {

    }

    make_req_matcher() {
        this.matcher += 1;
        return String(this.matcher);
    }

    set_namespace(ns) {
        if (ns[ns.length - 1] !== '/')
            this.ns = ns + '/';
        else
            this.ns = ns;
    }

    get_fully_qualified_topic(topic) {
        if (topic[0] === '/')
            return topic;
        else
            return this.ns + topic;
    }

    static add_callback(rach, topic, callback, args) {
        rach.callbacks[topic] = [callback, args];
        rach.sub_topics[topic] = true;
    }

    static rm_callback(rach, topic) {
        delete rach.callbacks[topic];
        delete rach.sub_topics[topic];
    }

    static add_pub_callback(rach, topic) {
        rach.pub_topics[topic] = true;
    }

    static rm_pub_callback(rach, topic) {
        delete rach.pub_topics[topic];
    }

    static empty_callback(rach) {

    }

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

    rm_all_sub() {
        for (let topic in this.sub_topics)
            this.rm_sub(topic);
    }

    rm_all_pub() {
        for (let topic in this.pub_topics)
            this.rm_pub(topic);
    }

    service_call(topic, args, on_result, on_err) {
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
            on_result, [], on_err, []
        ];
        this.send(msg);
    }
}

Rach.Publisher = class {
    constructor(topic, rach) {
        this.rach = rach;
        this.topic = topic;
    }

    pub(data) {
        this.rach.pub(this.topic, data);
    }

    close() {
        this.rach.rm_pub(this.topic);
    }
};

module.exports = Rach;
