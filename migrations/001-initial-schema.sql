-- Up
CREATE TABLE blocks(block_index INTEGER UNIQUE,
                    block_hash TEXT UNIQUE,
                    block_time INTEGER,
                    previous_block_hash TEXT UNIQUE,
                    difficulty INTEGER, ledger_hash TEXT, txlist_hash TEXT, messages_hash TEXT,
                    PRIMARY KEY (block_index, block_hash));
CREATE TABLE messages(message_index INTEGER PRIMARY KEY,
                    block_index INTEGER,
                    command TEXT,
                    category TEXT,
                    bindings TEXT,
                    timestamp INTEGER);

-- Down
DROP TABLE messages;
DROP TABLE blocks;
