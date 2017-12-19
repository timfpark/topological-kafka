const KafkaConnection = require('../index');

module.exports = {
    connection: new KafkaConnection({
        id: "kafkaConnection",
        config: {
            id: "worker1",
            groupId: "featureAugmentation",
            endpoint: "localhost:2181",
            keyField: "userId",
            topic: "test2",
        }
     })
};
