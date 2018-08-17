const assert = require('assert');
const fixtures = require('../fixtures');

describe('KafkaConnection', function() {
    it('can enqueue, pause, resume, and stream messages', done => {
        fixtures.connection.start(err => {
            assert(!err);

            fixtures.connection.stream((err, message) => {
                if (err) console.log(err);
                assert(!err);
                assert(message);

                assert(message.body.number, 1);
                setTimeout(done, 4000);
            });

            setTimeout(() => {
                fixtures.connection.pause(err => {
                    assert(!err);
                    setTimeout(() => {
                        fixtures.connection.enqueue(
                            [
                                {
                                    body: {
                                        userId: 'user1',
                                        number: 1
                                    }
                                }
                            ],
                            err => {
                                assert(!err);
                                setTimeout(() => {
                                    fixtures.connection.resume(err => {
                                        assert(!err);
                                    });
                                }, 1000);
                            }
                        );
                    }, 1000);
                });
            }, 1000);
        });
    });
});
