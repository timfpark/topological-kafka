const KafkaConnection = require('../index');

module.exports = {
    connection: new KafkaConnection({
        id: 'kafkaConnection',
        config: {
            id: 'topological-kafka',
            groupId: 'topological-kafka',
            endpoint: 'localhost:2181',
            keyField: 'userId',
            topic: 'topological-kafka'
        }
    })
};
