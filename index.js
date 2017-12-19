const async = require('async'),
      { Client, KeyedMessage, Producer, ConsumerGroup } = require('kafka-node'),
      { Connection } = require("topological");

const KEYED_PARTITIONER = 3;

class KafkaConnection extends Connection {
    constructor(config) {
        super(config);

        // this.config.maxMessagesOutstanding = this.config.maxMessagesOutstanding || 5;
        this.supportsStreaming = true;
    }

    start(callback) {
        this.client = new Client(this.config.endpoint);
        this.producer = new Producer(this.client, {
            requireAcks: 1,
            partitionerType: KEYED_PARTITIONER
        });

        this.producer.on('ready', () => {
            this.client.refreshMetadata([this.config.topic], callback);
        });
    }

    enqueue(messages, callback) {
        console.log(`${this.id}: enqueuing ${JSON.stringify(messages)}`);
        if (!messages || messages.length === 0) return callback();

        let key;
        let keyedMessages = messages.map(message => {
            key = message.body[this.config.keyField];
            return new KeyedMessage(key, JSON.stringify(message.body));
        });

        this.producer.send([{
            topic: this.config.topic,
            key,
            messages: keyedMessages
        }], callback);
    }

    stream(callback) {
        this.consumerGroup = new ConsumerGroup({
            id: this.config.id,
            host: this.config.endpoint,
            groupId: this.config.groupId,
            sessionTimeout: this.config.sessionTimeout || 15000,
            protocol: [this.config.protocol || 'roundrobin'],
            fromOffset: this.config.startFromOffset || 'earliest'
        }, [this.config.topic]);

        this.consumerGroup.on('message', message => {
            try {
                message.body = JSON.parse(message.value);
                return callback(null, message);
            } catch (e) {
                console.log('invalid JSON: ' + message.value);
            }
        });
        this.consumerGroup.on('error', callback);
    }
}

module.exports = KafkaConnection;
