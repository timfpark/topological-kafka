const assert = require('assert');
const fixtures = require('../fixtures');

describe('KafkaConnection', function() {
    it('can enqueue, pause, resume, and stream messages', done => {
        let resumed = false;
        fixtures.connection.start(err => {
            assert(!err);

            fixtures.connection.stream((err, message) => {
                assert(!err);
                assert(message);

                assert(message.body.number, 1);
                assert(resumed);
                return done();
            });

            setTimeout(() => {
                fixtures.connection.pause(err => {
                    assert(!err);
                    setTimeout(() => {
                        fixtures.connection.enqueue([{
                            body: {
                                userId: "user1",
                                number: 1
                            }
                        }], err => {
                            assert(!err);
                            setTimeout(() => {
                                fixtures.connection.resume(err => {
                                    assert(!err);
                                    resumed = true;
                                });
                            }, 1000);
                        });
                    }, 1000);
                });
            }, 1000);
        });
    });
});
