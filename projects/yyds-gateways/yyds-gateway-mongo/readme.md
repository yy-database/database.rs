# yyds-gateway-mongo

MongoDB wire disguise work for YYDS. `service::MongoService` attaches a loopback OP_MSG listener to an existing node owner and stops its client workers with the node lifecycle. The current surface is a handshake probe: `hello`/`isMaster` and `ping` answer a bounded BSON document, while every other command answers `{ ok: 0, errmsg: "unsupported" }`.

Query and CRUD execution, authentication, TLS, compression, sessions and replication are not implemented. It is not yet a MongoDB-compatible database server, so drivers can open a connection and inspect the wire version but must not expect `find`, `insert` or any other collection command to work.
