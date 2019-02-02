require('dotenv').config()
const jayson = require('jayson/promise')
const sqlite = require('sqlite')
const url = require('url')
const crypto = require('crypto')
const BigNumber = require('bignumber.js')
const JSON5 = require('json5-bignumber')
const SPVNode = require('bcoin/lib/node/spvnode')

const protocolChanges = require('./protocol_changes.json')

const BLOCK_READ_CHUNK = 10
const MESSAGE_READ_CHUNK = 10
const BLOCK_FIRST_MAINNET = 278270
const CONSENSUS_HASH_VERSION_MAINNET = 2

function checkProtocolChange(name, blockIndex) {
  if (name in protocolChanges) {
    return protocolChanges[name].block_index <= blockIndex
  } else {
    return false
  }
}

async function start() {
  const node = new SPVNode({
    file: true,
    argv: true,
    env: true,
    logFile: true,
    logConsole: true,
    logLevel: 'debug',
    db: 'leveldb',
    memory: false,
    persistent: true,
    workers: true,
    listen: true,
    loader: require
  })

  process.on('SIGINT', async () => {
    await node.close();
  })

  function createXcpConnection(uri) {
    const conn = url.parse(uri)
    return jayson.client[conn.protocol.slice(0, -1)](conn)
  }

  const xcpConns = new Array(parseInt(process.env.XCPURL_COUNT)).fill(0).map((x, idx) => createXcpConnection(process.env['XCPURL_'+idx]))
  let lastXcpConnUsed = -1
  const xcp = async (method, params) => {
    while (true) {
      lastXcpConnUsed = (lastXcpConnUsed + 1) % xcpConns.length
      try {
        let result = await xcpConns[lastXcpConnUsed].request(method, params)

        return result
      } catch(e) {
        console.log('Host ' + process.env['XCPURL_' + lastXcpConnUsed] + ' couldn\'t be contacted, trying another one')
      }
    }
  }

  const db = await sqlite.open(process.env.DB)
  let migrateOpts = {}
  if (process.env.FORCE_LAST_DB) {
    migrateOpts.force = 'last'
  }
  try {
    await db.migrate(migrateOpts)
  } catch (e) {
    console.log('No need to migrate DB')
  }

  async function updateFromFednode() {
    let timeoutTime = 500
    let rinf = await xcp('get_running_info', {})

    if (rinf.result && rinf.result.last_block) {
      let dbinf = await db.get('SELECT max(block_index) as mx FROM blocks')
      let lastDbBlockIndex = dbinf.mx
      if (lastDbBlockIndex === null) {
        lastDbBlockIndex = BLOCK_FIRST_MAINNET - 1
      }

      dbinf = await db.get('SELECT max(message_index) as mx FROM messages')
      let lastDbMsgIndex = dbinf.mx
      if (lastDbMsgIndex === null) {
        lastDbMsgIndex = -1
      }

      await syncBlocks(lastDbBlockIndex, rinf.result.last_block.block_index)
      timeoutTime = 30000
    }

    setTimeout(updateFromFednode, timeoutTime)
  }

  node.on('connect', async (entry, block) => {
    //console.log(entry, block)
  })

  node.on('reorganize', async (tip, competitor) => {
    //console.log(entry, block)
  })

  async function checkBlocks(blks) {
    let result = await Promise.all(blks.map(async (blk) => {
      let entry = await node.chain.db.getEntryByHash(Buffer.from(blk.block_hash, 'hex').reverse().toString('hex'))

      if (entry) {
        return blk
      } else {
        return null
      }
    }))

    return result.filter(x => x !== null)
  }

  async function syncBlocks(lastDb, lastXcp) {
    while (lastDb < lastXcp) {
      console.time('blocks')
      let min = lastDb
      let max = Math.min(lastDb + BLOCK_READ_CHUNK, lastXcp)
      let cnt = max - min
      console.log('Syncing blocks from ' + lastDb + ' to ' + lastXcp + ' (' + cnt + ' blocks)')
      if (cnt > 0) {
        console.time('blocks.download')
        let idxs = new Array(cnt).fill(0).map((x, idx) => idx + min + 1)
        let blks = await xcp('get_blocks', {block_indexes: idxs})
        console.timeEnd('blocks.download')

        console.time('blocks.write')
        try {
          await db.exec('BEGIN')

          //await node.chain.db.getEntryByHash('045d94a1c33354c3759cc0512dcc49fd81bf4c3637fb24000000000000000000')

          blks = await checkBlocks(blks.result)

          await Promise.all(blks.map(({block_index, block_hash, block_time, previous_block_hash, difficulty, ledger_hash, txlist_hash, messages_hash}) => db.run(
            'INSERT OR REPLACE INTO blocks(block_index, block_hash, block_time, previous_block_hash, difficulty, ledger_hash, txlist_hash, messages_hash) ' +
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?)', [block_index, block_hash, block_time, previous_block_hash, difficulty, ledger_hash, txlist_hash, messages_hash])))
          lastDb = max

          await Promise.all(blks.map(x => appendMessages(x._messages)))

          await db.exec('COMMIT')
        } catch (e) {
          console.log('ERROR:', e.message)
          await db.exec('ROLLBACK')
        }
        console.timeEnd('blocks.write')
      }

      console.timeEnd('blocks')
    }
  }

  async function appendMessages(msgs) {
    await Promise.all(msgs.map(({message_index, block_index, command, category, bindings, timestamp}) => db.run(
      'INSERT OR REPLACE INTO messages(message_index, block_index, command, category, bindings, timestamp) ' +
      'VALUES (?, ?, ?, ?, ?, ?)', [message_index, block_index, command, category, bindings, timestamp])))
  }

  /*await node.ensure()
  await node.open()
  await node.connect()
  node.startSync()

  await updateFromFednode()*/

  function dhash(txt) {
    let hsh = crypto.createHash('sha256')
    let hsh2 = crypto.createHash('sha256')
    return hsh2.update(hsh.update(Buffer.from(txt, 'utf8')).digest()).digest('hex')
  }

  function pruneBindings(bindings, blockIndex) {
    if (!checkProtocolChange('subassets')) {
      delete bindings['asset_longname']
    }

    if (!checkProtocolChange('enhanced_sends')) {
      delete bindings['memo']
    }

    return bindings
  }

  function pythonizeBindings(bindings) {
    if ('call_price' in bindings) {
      bindings.call_price = parseFloat(bindings.call_price)
    }

    return bindings
  }

  function capitalize(v) {
    return v.slice(0, 1).toUpperCase() + v.slice(1)
  }

  function pythonicStringify(v, fieldName, funcName) {
    let floatingPointFields = {
      'call_price': 1, 'value': 1, 'target_value': 1, 'fee_fraction_int': 1, 'initial_value': 1
    }

    let noQuoteFields = {
      'quantity': 1, 'block_index': 1, 'tx_index': 1, 'fee_fraction_int': 1, 'timestamp': 1,
      'give_remaining': 1, 'give_quantity': 1, 'get_remaining': 1, 'get_quantity': 1, 'fee_provided_remaining': 1,
      'fee_provided': 1, 'expire_index': 1, 'expiration': 1, 'quantity_per_unit': 1, 'order_index': 1, 'fee_paid': 1,
      'btc_amount': 1, 'fee_required': 1, 'fee_required_remaining': 1, 'backward_quantity': 1, 'forward_quantity': 1,
      'match_expire_index': 1, 'tx0_block_index': 1, 'tx0_expiration': 1, 'tx0_index': 1, 'tx1_block_index': 1,
      'tx1_expiration': 1, 'tx1_index': 1, 'counterwager_remaining': 1, 'deadline': 1, 'counterwager_quantity': 1,
      'wager_remaining': 1, 'wager_quantity': 1, 'leverage': 1, 'bet_type': 1, 'initial_value': 1,
      'tx0_bet_type': 1, 'tx1_bet_type': 1, 'bet_index': 1, 'escrow_less_fee': 1, 'bet_match_type_id': 1
    }

    const removeFloatingPointFieldsByFunc = {
      "broadcasts": ['fee_fraction_int'],
      "bet_matches": ['fee_fraction_int']
    }

    if (funcName in removeFloatingPointFieldsByFunc) {
      removeFloatingPointFieldsByFunc[funcName].forEach(x => delete floatingPointFields[x])
    }

    const removeNoQuoteFieldsByFunc = {
      "burns": ['block_index']
    }

    if (funcName in removeNoQuoteFieldsByFunc) {
      removeNoQuoteFieldsByFunc[funcName].forEach(x => delete noQuoteFields[x])
    }

    if (v === null) {
      return 'None'
    } else {
      return ({
        "string": (x) => {
          if (fieldName in noQuoteFields) {
            return v
          } else {
            if (x.indexOf('\'') >= 0) {
              let ret = x.replace(/\"/gi, '\\\"')

              return "\"" + ret + "\""
            } else {
              return "\'" + x + "\'"
            }
          }
        },
        "boolean": (x) => capitalize(x.toString()),
        "number": (x) => {
          if (fieldName in floatingPointFields) {
            let result = "" + x

            if (result.indexOf('.') < 0) {
              result = result + '.0'
            }

            return result
          } else {
            return "" + x
          }
        }
      })[typeof(v)](v)
    }
  }

  async function calcMessageHash(block, verbose=false) {
    let prevBlock = await db.get('SELECT * FROM blocks WHERE block_index=' + (block.block_index - 1))
    let msgs = await db.all('SELECT * FROM messages WHERE block_index=' + block.block_index)

    let content = msgs.map(m => {
      let bindings = JSON5.parse(m.bindings/*, function (k, v, n) {
        if (k === 'quantity') {
          console.log(k, typeof(v), v, n)
        }

        return v
      }*/)
      bindings = pythonizeBindings(pruneBindings(bindings, block.block_index))
      let entries = Object.entries(bindings).sort((a, b) => a[0].localeCompare(b[0]))
      return m.command + m.category + '[' + entries.map(e => '(\'' + e[0] + '\', ' + pythonicStringify(e[1], e[0], m.category) + ')').join(', ') + ']'
    })/*.map(x => x.replace(/\'/gi, '\\\''))*/.join('') // Emulate Python result

    let hashText = prevBlock.messages_hash + CONSENSUS_HASH_VERSION_MAINNET + content
    let calculatedHash = dhash(hashText)

    if (verbose) {
      console.log('prevBlock:', prevBlock)
      console.log('hashText:', hashText)
      console.log('messages:', msgs)
      console.log('content:', content)
      console.log('calculatedHash:', calculatedHash)
      console.log('realHash:', block.messages_hash)
    }

    return calculatedHash
  }

  async function test() {
    let start = /**/284054 /*/BLOCK_FIRST_MAINNET + 1/**/
    for (let i = start; i < 550000; i++) {
      let testBlock = await db.get('SELECT * FROM blocks WHERE block_index=' + i)

      let chash = await calcMessageHash(testBlock)

      if (testBlock.messages_hash !== chash) {
        console.log(testBlock)
        await calcMessageHash(testBlock, true)
        throw new Error('invalid messages_hash')
      }
    }
    console.log("\033[s\033[1;", i)
  }

  ////////////////

  function indexMessage(msg) {
    const addressFields = ['address', 'source', 'destination', 'issuer', 'owner']
    let ob = {}

    let bindings = JSON.parse(msg.bindings)

    addressFields.forEach(f => {
      if (f in bindings) {
        ob[bindings[f]] = 1
      }
    })

    return ob
  }

  async function indexChain(startBlockIndex) {
    console.time('indexing')

    let start = startBlockIndex || BLOCK_FIRST_MAINNET
    let indices = []

    for (let i = start; i < 560000; i++) {
      let msgs = await db.all('SELECT * FROM messages WHERE block_index=' + i)
      let ob = {}

      if (msgs) {
        try {
          msgs.forEach(m => { ob = { ...ob, ...indexMessage(m)} })
          let addrs = Object.keys(ob)

          if (addrs.length > 0) {
            indices.push({ block_index: i, addresses: addrs })
            console.log('Indexed block ' + i)
          }
        } catch(e) {
          console.log(msgs)
          throw e
        }
      }/* else {
        break
      }*/
    }
    console.timeEnd('indexing')
    console.log(indices, indices.toString().length)

  }

  ////////////////

  try {
    //await indexChain()
    await test()
  } catch(e) {
    console.log(e)
    process.exit(1)
  }
}

start()
