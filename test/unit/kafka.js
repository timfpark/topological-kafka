const assert = require('assert');
const fixtures = require('../fixtures');

describe('KafkaConnection', function() {
    it('can queue and dequeue messages', done => {
         fixtures.connection.start(err => {
            assert(!err);

            fixtures.connection.stream((err, message) => {
                assert(!err);
                assert(message);

                assert(message.body.number, 1);
                return done();
            });

            fixtures.connection.enqueue([{
                body: {
                    userId: "user1",
                    number: 1
                }
            }], err => {
                assert(!err);
            });
         });
    });
});
