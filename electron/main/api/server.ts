import * as dotenv from 'dotenv'
dotenv.config()
import cors from 'cors'
import express from 'express'
import mountRoutes from './routes'
import { updFlagCOM1, updFlagCOM2, resetFlagCOM1, resetFlagCOM2, updFlagTCP1, resetFlagTCP1, updFlagConn, resetFlagConn } from './routes'
import db from '../db'
import * as bcrypt from 'bcrypt';
import createTableText from './createdb'
import rulocale from './rulocale'
import enlocale from './enlocale'
import trlocale from './trlocale'
import eslocale from './eslocale'
import network from 'network'
import { SerialPort } from 'serialport'
import ModbusRTU from 'modbus-serial'
const client1 = new ModbusRTU();
const client2 = new ModbusRTU();
const client3 = new ModbusRTU();
const api = express()
api.use(express.json())
api.use(express.urlencoded({ extended: true }))
api.use(cors())
mountRoutes(api)
api.post('/tags/writeTagRTU', async (req, res) => {
  const { name, value } = req.body;
  if (value != null) {
    writeTag.name = name;
    writeTag.val = value;
    writeTrig = true;
    res.status(200).send({
      message: "Writing data to RTU",
      body: { writeTag },
    })
  } else {
    res.status(500).send({
      error: "Null value",
      body: { name, value },
    })
  }
})

api.listen(process.env['EXPRESS_SERVER_PORT'] || 3000, () => {
  console.log(`API Server listening on port `, process.env['EXPRESS_SERVER_PORT'] || 3000)
})


let mbsStatus = "Initializing...";    // holds a status of Modbus
// Modbus 'state' constants
const MBS_STATE_STEADY = "State steady";
const MBS_STATE_INIT = "State init";
const MBS_STATE_IDLE = "State idle";
const MBS_STATE_NEXT = "State next";
const MBS_STATE_GOOD_READ = "State good (read)";
const MBS_STATE_FAIL_READ = "State fail (read)";
const MBS_STATE_GOOD_CONNECT = "State good (port)";
const MBS_STATE_FAIL_CONNECT = "State fail (port)";
const MBS_STATE_GOOD_WRITE = "State good (write)";
const MBS_STATE_FAIL_WRITE = "State fail (write)";


// Modbus configuration values
let com1 = { self: 'com1', path: '', conf: {}, scan: 0, timeout: 0, mbsState: MBS_STATE_STEADY, slaves: Array() };
let com2 = { self: 'com2', path: '', conf: {}, scan: 0, timeout: 0, mbsState: MBS_STATE_STEADY, slaves: Array() };
let ip1 = { self: 'ip1', ip: '', port: '', path: '', scan: 0, timeout: 1000, mbsState: MBS_STATE_STEADY, slaves: Array() };

//const arrayToObject = (arr, keyField) =>
//  Object.assign({}, ...arr.map(item => ({ [item[keyField]]: item })))
let contype = ''

const dbConf = async () => {
  // create table
  const conn = await db.query('SELECT * FROM hwconfig WHERE name = $1', ['connConf']);
  contype = conn.rows[0].data.conn
  if (contype == 'ip') {
    const tcpRows = await db.query('SELECT * FROM hwconfig WHERE name = $1', ['ipConf']);
    ip1 = Object.assign(ip1, tcpRows.rows[0].data.tcp1, { act: 0, path: tcpRows.rows[0].data.tcp1.ip + ':' + tcpRows.rows[0].data.tcp1.port });
    ip1.slaves = []
    const tcpTags = [
      { tag: { name: "stopAngle", group: "monitoring", dev: "tcp1", addr: "6", type: "word", reg: "r", min: 0, max: 359, dec: 0 }, link: false },
      { tag: { name: "orderLength", group: "monitoring", dev: "tcp1", addr: "4", type: "float", reg: "r", min: 0, max: 1000, dec: 2 }, link: false },
      { tag: { name: "speedMainDrive", group: "monitoring", dev: "tcp1", addr: "2", type: "float", reg: "r", min: 0, max: 600, dec: 1 }, link: false },
      { tag: { name: "modeCode", group: "monitoring", dev: "tcp1", addr: "8", type: "word", reg: "r", min: 0, max: 6, dec: 0 }, link: false },
      { tag: { name: "picksLastRun", group: "monitoring", dev: "tcp1", addr: "0", type: "dword", reg: "r", min: 0, max: 4294967295, dec: 0 }, link: false },
      { tag: { name: "modeControl", group: "setting", dev: "tcp1", addr: "12", type: "word", reg: "rw", min: 0, max: 65535, dec: 0 }, link: false },
      { tag: { name: "planClothDensity", group: "setting", dev: "tcp1", type: "float", addr: "10", reg: "rw", min: 0.5, max: 1000, dec: 2 }, link: false },
      { tag: { name: "planOrderLength", group: "setting", dev: "tcp1", type: "float", addr: "14", reg: "rw", min: 0, max: 1000, dec: 2 }, link: false },
      //{ tag: { name: "planSpeedMainDrive", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 600, dec: 1 }, val: 200.0 },
      { tag: { name: "warpShrinkage", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 100, dec: 1 }, val: 1.0 },
      { tag: { name: "fullWarpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
      { tag: { name: "warpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
    ]
    await db.query('DELETE FROM tags WHERE tag->>$1~$2', ['dev', 'rtu']);
    await db.query('INSERT INTO tags(tag,val,link) SELECT * FROM jsonb_to_recordset($1) as x(tag jsonb, val numeric, link boolean) ON CONFLICT (tag) DO NOTHING;', [JSON.stringify(tcpTags)])
    const ip1t = await db.query('SELECT tag->$3 as name, tag->$4 as addr, tag->$5 as type, tag->$6 as reg FROM tags WHERE tag->>$1=$2 ORDER BY tag->>$4 DESC ', ['dev', 'tcp1', 'name', 'addr', 'type', 'reg']);
    ip1.slaves.push(Object.assign({ name: 'tcp1' }, tcpRows.rows[0].data['tcp1'], { tags: ip1t.rows }));
    if (ip1.mbsState == MBS_STATE_STEADY && ip1.slaves.length > 0) {
      ip1.mbsState = MBS_STATE_INIT;
      runModbus(client3, ip1)
    }
  }

  if (contype == 'com') {
    const comRows = await db.query('SELECT * FROM hwconfig WHERE name = $1', ['comConf']);
    com1 = Object.assign(com1, comRows.rows[0].data.opCOM1, { act: 0 });
    com2 = Object.assign(com2, comRows.rows[0].data.opCOM2, { act: 0 });
    com1.slaves = []
    com2.slaves = []
    const rtuConf = { rtu1: { com: 'opCOM1', sId: 1, swapBytes: true, swapWords: true } }
    const tags = [
      //{ tag: { name: "stopAngle", group: "monitoring", dev: "rtu1", addr: "6", type: "word", reg: "r", min: 0, max: 359, dec: 0 }, link: false },
      //{ tag: { name: "orderLength", group: "monitoring", dev: "rtu1", addr: "4", type: "float", reg: "r", min: 0, max: 1000, dec: 2 }, link: false },
      //{ tag: { name: "speedMainDrive", group: "monitoring", dev: "rtu1", addr: "2", type: "float", reg: "r", min: 0, max: 600, dec: 1 }, link: false },
      //{ tag: { name: "modeCode", group: "monitoring", dev: "rtu1", addr: "8", type: "word", reg: "r", min: 0, max: 6, dec: 0 }, link: false },
      //{ tag: { name: "picksLastRun", group: "monitoring", dev: "rtu1", addr: "0", type: "dword", reg: "r", min: 0, max: 4294967295, dec: 0 }, link: false },
      //{ tag: { name: "modeControl", group: "setting", dev: "rtu1", addr: "12", type: "word", reg: "rw", min: 0, max: 65535, dec: 0 }, link: false },
      { tag: { name: "planSpeedMainDrive", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 600, dec: 1 }, val: 200.0 },
      //{ tag: { name: "planClothDensity", group: "setting", dev: "rtu1", type: "float", addr: "10", reg: "rw", min: 0.5, max: 1000, dec: 2 }, link: false },
      //{ tag: { name: "planOrderLength", group: "setting", dev: "rtu1", type: "float", addr: "14", reg: "rw", min: 0, max: 1000, dec: 2 }, link: false },
      //{ tag: { name: "warpShrinkage", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 100, dec: 1 }, val: 1.0 },
      //{ tag: { name: "fullWarpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
      //{ tag: { name: "warpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
    ]
    await db.query('DELETE FROM tags WHERE tag->>$1~$2', ['dev', 'tcp']);
    await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['rtuConf', rtuConf])
    await db.query('INSERT INTO tags(tag,val,link) SELECT * FROM jsonb_to_recordset($1) as x(tag jsonb, val numeric, link boolean) ON CONFLICT (tag) DO NOTHING;', [JSON.stringify(tags)])
    const rtuRows = await db.query('SELECT * FROM hwconfig WHERE name = $1', ['rtuConf']);
    for (let prop in rtuRows.rows[0].data) {
      switch (rtuRows.rows[0].data[prop].com) {
        case "opCOM1":
          //const com1t = await db.query('SELECT tag->$5 as name, tag->$6 as addr, tag->$7 as type, tag->$8 as reg FROM tags WHERE tag->>$1=$2 AND tag->>$3=$4', ['dev', prop, 'group', 'monitoring', 'name', 'addr', 'type', 'reg']);
          const com1t = await db.query('SELECT tag->$3 as name, tag->$4 as addr, tag->$5 as type, tag->$6 as reg FROM tags WHERE tag->>$1=$2 ORDER BY tag->>$4 DESC ', ['dev', prop, 'name', 'addr', 'type', 'reg']);
          com1.slaves.push(Object.assign({ name: prop }, rtuRows.rows[0].data[prop], { tags: com1t.rows }));
          break;
        case "opCOM2":
          //const com2t = await db.query('SELECT tag->$5 as name, tag->$6 as addr, tag->$7 as type, tag->$8 as reg FROM tags WHERE tag->>$1=$2 AND tag->>$3=$4', ['dev', prop, 'group', 'monitoring', 'name', 'addr', 'type', 'reg']);
          const com2t = await db.query('SELECT tag->$3 as name, tag->$4 as addr, tag->$5 as type, tag->$6 as reg FROM tags WHERE tag->>$1=$2 ORDER BY tag->>$4 DESC ', ['dev', prop, 'name', 'addr', 'type', 'reg']);
          com2.slaves.push(Object.assign({ name: prop }, rtuRows.rows[0].data[prop], { tags: com2t.rows }));
          break;
        default:
        // nothing to do
      }
    }

    if (com1.mbsState == MBS_STATE_STEADY && com1.slaves.length > 0) {
      com1.mbsState = MBS_STATE_INIT;
      runModbus(client1, com1)
    }
    if ((com2.mbsState == MBS_STATE_STEADY && com2.slaves.length > 0)) {
      com2.mbsState = MBS_STATE_INIT;
      runModbus(client2, com2)
    }
  }
}

const dbInit = async () => {
  await db.query(createTableText);
  await db.query('INSERT INTO locales SELECT UNNEST($1::text[]), UNNEST($2::jsonb[]), UNNEST($3::boolean[]) ON CONFLICT (locale) DO NOTHING;', [['en', 'es', 'ru', 'tr'], [enlocale, eslocale, rulocale, trlocale], [false, false, true, false]])
  const { rows } = await db.query('SELECT COUNT(*) FROM clothlog');
  if (rows[0].count == 0) {
    await db.query('INSERT INTO lifetime VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (serialno) DO NOTHING;', ['СТБУТТ1-280Кр', '00000001', new Date('2022-12-31T12:00:00.000Z'), 0, 0, '0H']);
    await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['connConf', { 'conn': 'com' }])
    await bcrypt.hash('123456', 10, async (err, hash) => {
      await db.query(`INSERT INTO users (id, name, password, role) VALUES(1,'Admin',$1,'admin') ON CONFLICT (id) DO NOTHING;`, [hash])
    });
    await db.query(`INSERT INTO clothlog VALUES(tstzrange(current_timestamp(3),NULL,'[)'),$1,(SELECT val from tags WHERE tag->>'name' = 'fullWarpBeamLength'))`, [0])
    await network.get_active_interface(async (err, obj) => {
      const ipConf = { opIP: obj, tcp1: { ip: '192.168.1.123', port: '502', sId: 1, swapBytes: true, swapWords: true } }
      await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['ipConf', ipConf])
    })
    await SerialPort.list().then(async function (ports) {
      if (ports[0] !== undefined && com1.path == '') { com1.path = ports[0].path; } else if (com1.path == '') { com1.path = "COM1"; }
      if (ports[1] !== undefined && com2.path == '') { com2.path = ports[1].path; } else if (com2.path == '') { com2.path = "COM2"; }
      const comConf = { opCOM1: { path: com1.path, conf: { baudRate: 230400, parity: "none", dataBits: 8, stopBits: 1 }, scan: 0, timeout: 500 }, opCOM2: { path: com2.path, conf: { baudRate: 115200, parity: "none", dataBits: 8, stopBits: 1 }, scan: 0, timeout: 0 } }
      await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['comConf', comConf])
    });
  }
  await dbConf();
}

dbInit();
//==============================================================
const connectClient = async function (client, port) {
  // set requests parameters
  await client.setTimeout(port.timeout ? port.timeout : 50);
  // try to connect
  try {
    if (port.ip) { await client.connectTCP(port.ip, { port: Number(port.port) }) }
    else { await client.connectRTUBuffered(port.path, port.conf) }
    port.mbsState = MBS_STATE_GOOD_CONNECT;
    mbsStatus = "[" + port.path + "]" + "Connected, wait for reading...";
    console.log(mbsStatus);
  } catch (e) {
    port.slaves.map(async (slave: any) => {
      await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 AND link=true;', ['dev', slave.name]);
    })
    port.mbsState = MBS_STATE_FAIL_CONNECT;
    mbsStatus = "[" + port.path + "]" + e.message;
    console.log(mbsStatus);
  }
}

//==============================================================
function getByteLength(type) {
  switch (String(type).toLowerCase()) {
    case "int16":
    case "word":
      return 2;
    case "int32":
    case "dword":
    case "float":
      return 4;
    default:
      throw new Error("Unsupported type");
  }
}

let writeTrig = false;
let writeTag = { name: '', val: 0 };
const writeModbusData = async function (tagName, val) {
  await process(tagName, val)
  writeTrig = false;
  async function process(tagName, val) {
    const tagRow = await db.query('select tag->$1 as dev, tag->$2 as addr, tag->$3 as type, tag->$4 as reg FROM tags WHERE tag->>$5=$6', ['dev', 'addr', 'type', 'reg', 'name', tagName]);
    let tag = tagRow.rows[0];
    tag.name = tagName;
    let client
    let port
    let slave
    if (contype == 'com') {
      const slaveRow = await db.query('SELECT data->$1 as rtu FROM hwconfig WHERE name=$2 AND data?$1', [tagRow.rows[0].dev, 'rtuConf']);
      slave = slaveRow.rows[0].rtu;
      switch (slave.com) {
        case "opCOM1":
          client = client1;
          port = com1;
          break;
        case "opCOM2":
          client = client2;
          port = com2;
          break;
        default:
        // nothing to do
      }
    }
    else {
      const slaveRow = await db.query('SELECT data->$1 as tcp FROM hwconfig WHERE name=$2 AND data?$1', [tagRow.rows[0].dev, 'ipConf']);
      slave = slaveRow.rows[0].tcp;
      client = client3;
      port = ip1;
    }
    await client.setID(slave.sId);
    switch (tag.type) {
      case 'bool':
        try {
          await client.writeCoils(tag.addr, val);
          port.mbsState = MBS_STATE_GOOD_WRITE;
          mbsStatus = "success";
          //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
        } catch (e) {
          port.mbsState = MBS_STATE_FAIL_WRITE;
          mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
          console.log(mbsStatus);
        }
        break;
      default:
        try {
          const size = getByteLength(tag.type);
          const buffer = Buffer.allocUnsafe(size);
          if (tag.type === "int16") {
            slave.swapWords ? buffer.writeInt16LE(val) : buffer.writeInt16BE(val);
          } else if (tag.type === "word") {
            slave.swapWords ? buffer.writeUInt16LE(val) : buffer.writeUInt16BE(val);
          } else if (tag.type === "int32") {
            slave.swapWords ? buffer.writeInt32LE(val) : buffer.writeInt32BE(val);
          } else if (tag.type === "dword") {
            slave.swapWords ? buffer.writeUInt32LE(val) : buffer.writeUInt32BE(val);
          } else if (tag.type === "float") {
            slave.swapWords ? buffer.writeFloatLE(val) : buffer.writeFloatBE(val);
          }
          if (slave.swapBytes) { buffer.swap16(); }
          await client.writeRegisters(tag.addr, buffer);
          port.mbsState = MBS_STATE_GOOD_WRITE;
          mbsStatus = "success";
          //console.log("[" + port.path + "]" + "[#" + slave.sId + "][WRITE]" + tag.name + " = " + val);
        } catch (e) {
          port.mbsState = MBS_STATE_FAIL_WRITE;
          mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
          console.log(mbsStatus);
        }
        break;
    }
  }
};
const readModbusData = async function (client, port, slave) {
  await client.setID(slave.sId);
  let count = slave.tags.length;
  await process(slave.tags[count - 1])
  async function process(tag) {
    switch (tag.reg) {
      case 'rw':
        switch (tag.type) {
          case 'bool':
            try {
              const data = await client.readCoils(tag.addr, 1);
              port.mbsState = MBS_STATE_GOOD_READ;
              mbsStatus = "success";
              let val = data.buffer[0];
              await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false);', [val, 'dev', slave.name, 'name', tag.name]);
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true', ['dev', slave.name, 'name', tag.name]);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            }
            break;
          default:
            try {
              const data = await client.readHoldingRegisters(tag.addr, getByteLength(tag.type) / 2)
              port.mbsState = MBS_STATE_GOOD_READ;
              mbsStatus = "success";
              if (slave.swapBytes) { data.buffer.swap16(); }
              let val;
              switch (tag.type) {
                case 'dword':
                  val = slave.swapWords ? data.buffer.readUInt32LE(0) : data.buffer.readUInt32BE(0);
                  break;
                case 'word':
                  val = slave.swapWords ? data.buffer.readUInt16LE(0) : data.buffer.readUInt16BE(0);
                  break;
                case 'float':
                  val = slave.swapWords ? data.buffer.readFloatLE(0) : data.buffer.readFloatBE(0);
                  break;
                default:
                  break;
              }
              await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false);', [val, 'dev', slave.name, 'name', tag.name]);
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true;', ['dev', slave.name, 'name', tag.name]);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            }
            break;
        }
        break;
      case 'r':
        switch (tag.type) {
          case 'bool':
            try {
              const data = await client.readDiscreteInputs(tag.addr, 1);
              port.mbsState = MBS_STATE_GOOD_READ;
              mbsStatus = "success";
              let val = data.buffer[0];
              await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false);', [val, 'dev', slave.name, 'name', tag.name]);
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true;', ['dev', slave.name, 'name', tag.name]);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            }
            break;
          default:
            try {
              const data = await client.readInputRegisters(tag.addr, getByteLength(tag.type) / 2)
              port.mbsState = MBS_STATE_GOOD_READ;
              mbsStatus = "success";
              if (slave.swapBytes) { data.buffer.swap16(); }
              let val;
              switch (tag.type) {
                case 'dword':
                  val = slave.swapWords ? data.buffer.readUInt32LE(0) : data.buffer.readUInt32BE(0);
                  break;
                case 'word':
                  val = slave.swapWords ? data.buffer.readUInt16LE(0) : data.buffer.readUInt16BE(0);
                  break;
                case 'float':
                  val = slave.swapWords ? data.buffer.readFloatLE(0) : data.buffer.readFloatBE(0);
                  break;
                default:
                  break;
              }
              await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false);', [val, 'dev', slave.name, 'name', tag.name]);
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              if (tag.name == 'modeCode') {
                await db.query('UPDATE tags SET updated=current_timestamp, link=false, val=0 where tag->>$1=$2 and tag->>$3=$4 AND link=true;', ['dev', slave.name, 'name', tag.name]);
              }
              else {
                await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true;', ['dev', slave.name, 'name', tag.name]);
              }
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            }
            break;
        }
        break;
      default:
        break;
    }
  }
};
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
//==============================================================
const runModbus = async function (client, port) {
  if (updFlagConn) {
    const conn = await db.query('SELECT * FROM hwconfig WHERE name = $1', ['connConf']);
    contype = conn.rows[0].data.conn
    await client.close(async () => { console.log("[" + port.path + "]closed"); });
    await dbConf();
    resetFlagConn();
  }
  if ((updFlagCOM1 && port.self == 'com1') || (updFlagCOM2 && port.self == 'com2') || (updFlagTCP1 && port.self == 'ip1')) {
    await client.close(async () => { console.log("[" + port.path + "]closed"); });
    await dbConf();
    await connectClient(client, port);
    (port.self == 'com1') && resetFlagCOM1(); (port.self == 'com2') && resetFlagCOM2(); (port.self == 'ip1') && resetFlagTCP1();
  }
  let nextAction;
  let slave = port.slaves[port.act];
  if (port.slaves.length > 0 && slave?.tags?.length > 0 && ((port.ip && (contype == 'ip')) || (!port.ip && (contype == 'com')))) {
    switch (port.mbsState) {
      case MBS_STATE_INIT:
        nextAction = await connectClient(client, port);
        break;

      case MBS_STATE_NEXT:
        nextAction = await readModbusData(client, port, slave);
        break;

      case MBS_STATE_GOOD_CONNECT:
        nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave);
        break;

      case MBS_STATE_FAIL_CONNECT:
        nextAction = await connectClient(client, port);
        break;
      case MBS_STATE_GOOD_WRITE:
      case MBS_STATE_GOOD_READ:
        nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave);
        break;

      case MBS_STATE_FAIL_READ:
      case MBS_STATE_FAIL_WRITE:
        if (client.isOpen) { port.mbsState = MBS_STATE_NEXT; nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave); }
        else { nextAction = await connectClient(client, port); }
        break;

      default:
      // nothing to do, keep scanning until actionable case
    }
    // execute "next action" function if defined
    if (nextAction !== undefined) {
      //console.log("[" + port.path + "]" + nextAction);
      if ((!((port.self == 'com1') && updFlagCOM1) || !((port.self == 'com2') && updFlagCOM2) || !((port.self == 'ip1') && updFlagTCP1)) && !updFlagConn) {
        await nextAction();
        port.mbsState = MBS_STATE_IDLE;
      }
    }
  }
  else if (port.slaves.length == 0) { if (client.isOpen) { await client.close(async () => { console.log("[" + port.path + "]closed"); }); } }
  port.act++;
  if (port.act === port.slaves.length) {
    port.act = 0;
  }
  await delay(port.scan);
  //console.log("runmodbus" + port.path + port.mbsState + port.slaves.length)
  if ((port.ip && contype == 'ip') || (!port.ip && contype == 'com')) { runModbus(client, port); }
  else { port.mbsState = MBS_STATE_STEADY; (port.self == 'com1') && resetFlagCOM1(); (port.self == 'com2') && resetFlagCOM2(); (port.self == 'ip1') && resetFlagTCP1(); }
};
