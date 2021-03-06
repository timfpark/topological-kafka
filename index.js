const async = require('async'),
    { Client, KeyedMessage, Producer, ConsumerGroup } = require('kafka-node'),
    { Connection } = require('topological');

const CYCLIC_PARTITIONER = 2;
const KEYED_PARTITIONER = 3;

class KafkaConnection extends Connection {
    constructor(config) {
        super(config);
    }

    start(callback) {
        this.log.debug(`Kafka client connecting to ${this.config.endpoint}`);
        this.client = new Client(this.config.endpoint);

        this.producer = new Producer(this.client, {
            requireAcks: 1,
            partitionerType: this.config.keyField
                ? KEYED_PARTITIONER
                : CYCLIC_PARTITIONER
        });

        this.producer.on('ready', () => {
            this.client.refreshMetadata([this.config.topic], callback);
        });
    }

    resume(callback) {
        if (!this.consumerGroup)
            return callback(new Error('Not yet streaming, cannot resume'));

        super.resume(err => {
            if (err) return callback(err);

            this.consumerGroup.resume();
            if (callback) return callback();
        });
    }

    pause(callback) {
        if (!this.consumerGroup)
            return callback(new Error('Not yet streaming, cannot pause'));

        super.pause(err => {
            if (err) return callback(err);

            this.consumerGroup.pause();
            if (callback) return callback();
        });
    }

    enqueue(messages, callback) {
        if (!messages || messages.length === 0) return callback();

        async.each(
            messages,
            (message, messageCallback) => {
                let messageToSend;
                let key;
                if (this.config.keyField) {
                    key = message.body[this.config.keyField];
                    messageToSend = new KeyedMessage(
                        key,
                        JSON.stringify(message.body)
                    );
                } else {
                    messageToSend = JSON.stringify(message.body);
                }

                this.producer.send(
                    [
                        {
                            topic: this.config.topic,
                            key,
                            messages: [messageToSend]
                        }
                    ],
                    err => {
                        if (err) {
                            this.log.error(`failed to enqueue message: ${err}`);
                        } else {
                            this.log.debug(
                                `enqueued message: ${JSON.stringify(
                                    message.body
                                ).substr(0, 50)}`
                            );
                        }
                        return messageCallback(err);
                    }
                );
            },
            callback
        );
    }

    stream(callback) {
        this.consumerGroup = new ConsumerGroup(
            {
                id: this.config.id,
                host: this.config.endpoint,
                autoCommit: true,
                autoCommitIntervalMs: 1000,
                fetchMaxBytes: this.config.fetchMaxBytes || 20 * 1024 * 1024,
                groupId: this.config.groupId || this.parentNode.id,
                sessionTimeout: this.config.sessionTimeout || 15000,
                protocol: [this.config.protocol || 'roundrobin'],
                fromOffset: this.config.startFromOffset || 'earliest'
            },
            [this.config.topic]
        );

        this.consumerGroup.on('message', message => {
            try {
                message.body = JSON.parse(message.value);

                this.log.debug(
                    `dequeued message: ${JSON.stringify(message.body).substr(
                        0,
                        50
                    )}`
                );

                return callback(null, message);
            } catch (e) {
                return callback(e);
            }
        });
        this.consumerGroup.on('error', callback);
    }
}

module.exports = KafkaConnection;
