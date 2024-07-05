import * as dotenv from 'dotenv'
dotenv.config()
import cors from 'cors'
import express from 'express'
import compression from 'compression'
import mountRoutes from './routes'
import { sse, updFlagCOM1, updFlagCOM2, resetFlagCOM1, resetFlagCOM2, updFlagTCP1, resetFlagTCP1, updFlagConn, resetFlagConn, usbPath, usbAttach, usbDetach } from './routes'
import db from '../db'
import * as bcrypt from 'bcrypt';
import createTableText from './createdb'
import rulocale from './rulocale'
import enlocale from './enlocale'
import trlocale from './trlocale'
import eslocale from './eslocale'
import network from 'network'
import { SerialPort } from 'serialport'
import parseInterval from 'postgres-interval'
import ModbusRTU from 'modbus-serial'
import sudo from 'sudo-prompt'
import { isString } from 'lodash'

const options = {
  name: 'Electron',
};
const client1 = new ModbusRTU();
const client2 = new ModbusRTU();
const client3 = new ModbusRTU();
const api = express()
api.use(express.json())
api.use(express.urlencoded({ extended: true }))
api.use(cors())
api.use(compression())
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

api.get('/config/getinterfaces', async (req, res) => {

  await network.get_interfaces_list(async (err, obj) => {
    if (err) {
      res.status(500).send({
        error: err
      })
    }
    else {
      let ifs = obj
      for (let ifc of ifs) {
        if (ifc.type == 'Wired') {

          sudo.exec("nmcli -g ipv4.method connection show wired", options, async (error, data, getter) => {
            let dhcp = false
            if (!error) {
              dhcp = (data?.toString().split('\n')[0] == 'auto') ? true : false;
            }
            else {
              dhcp = ((ifc.ip_address == undefined) || (ifc.netmask == undefined) || (ifc.gateway_ip == undefined)) ? true : false
            }
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, dhcp}', dhcp]);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, mac_address}', '"' + ifc.mac_address + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, ip_address}', '"' + (ifc.ip_address != undefined ? ifc.ip_address : '') + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, netmask}', '"' + (ifc.netmask != undefined ? ifc.netmask : '255.255.255.0') + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, gateway_ip}', '"' + (ifc.gateway_ip != undefined ? ifc.gateway_ip : '') + '"']);
          });
        }
        if (ifc.type == 'Wireless') {

          sudo.exec("nmcli -g ipv4.method connection show wireless", options, async (error, data, getter) => {
            let dhcp = false
            if (!error) {
              dhcp = (data?.toString().split('\n')[0] == 'auto') ? true : false;
            }
            else {
              dhcp = ((ifc.ip_address == undefined) || (ifc.netmask == undefined) || (ifc.gateway_ip == undefined)) ? true : false
            }
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, dhcp}', dhcp]);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, mac_address}', '"' + ifc.mac_address + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, ip_address}', '"' + (ifc.ip_address != undefined ? ifc.ip_address : '') + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, netmask}', '"' + (ifc.netmask != undefined ? ifc.netmask : '255.255.255.0') + '"']);
            await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, gateway_ip}', '"' + (ifc.gateway_ip != undefined ? ifc.gateway_ip : '') + '"']);
          });
        }
      }
      sudo.exec("nmcli -g GENERAL.STATE connection show wired", options, async (error, data, getter) => {
        let status = 'invisible'
        if (!error) {
          status = data?.toString().split('\n')[0] || 'invisible';
        }
        else {
          status = 'invisible';
        }
        await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, status}', '"' + status + '"']);
      });
      sudo.exec("nmcli -g GENERAL.STATE connection show wireless", options, async (error, data, getter) => {
        let status = 'invisible'
        if (!error) {
          status = data?.toString().split('\n')[0] || 'invisible';
        }
        else {
          status = 'invisible';
        }
        await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, status}', '"' + status + '"']);
      });
      sudo.exec("nmcli general hostname", options, async (error, data, getter) => {
        let hostname = ''
        if (!error) {
          hostname = data?.toString().split('\n')[0] || '';
        }
        await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, name}', '"' + hostname + '"']);
      });
      res.status(200).send({
        opIP: ifs,
      })
    }
  })
})

api.listen(process.env['EXPRESS_SERVER_PORT'] || 3000, () => {
  //console.log(`API Server listening on port `, process.env['EXPRESS_SERVER_PORT'] || 3000)
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
//  Object.assign({}, ...slave.mbarr.map(item => ({ [item[keyField]]: item })))
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
      { tag: { name: "stopAngle", group: "monitoring", dev: "tcp1", addr: "8", type: "word", reg: "r", min: 0, max: 359, dec: 0 }, link: false },
      { tag: { name: "orderLength", group: "monitoring", dev: "tcp1", addr: "10", type: "float", reg: "r", min: 0, max: 1000, dec: 2 }, link: false },
      { tag: { name: "speedMainDrive", group: "monitoring", dev: "tcp1", addr: "6", type: "float", reg: "r", min: 0, max: 600, dec: 1 }, link: false },
      { tag: { name: "modeCode", group: "event", dev: "tcp1", addr: "0", type: "word", reg: "r", min: 0, max: 6, dec: 0 }, link: false },
      { tag: { name: "picksLastRun", group: "monitoring", dev: "tcp1", addr: "4", type: "float", reg: "r", min: -2147483648, max: 2147483647, dec: 4 }, link: false },
      { tag: { name: "realPicksLastRun", group: "monitoring", dev: "tcp1", addr: "2", type: "int32", reg: "r", min: -2147483648, max: 2147483647, dec: 0 }, link: false },
      { tag: { name: "modeControl", group: "event", dev: "tcp1", addr: "14", type: "word", reg: "rw", min: 0, max: 65535, dec: 0 }, link: false },
      { tag: { name: "planClothDensity", group: "event", dev: "tcp1", type: "float", addr: "12", reg: "rw", min: 0.5, max: 1000, dec: 2 }, link: false },
      { tag: { name: "planOrderLength", group: "event", dev: "tcp1", type: "float", addr: "16", reg: "rw", min: 0, max: 1000, dec: 2 }, link: false },
      { tag: { name: "pickAngle", group: "event", dev: "tcp1", type: "word", addr: "18", reg: "rw", min: 0, max: 359, dec: 0 }, link: false },
      { tag: { name: "planSpeedMainDrive", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 600, dec: 1 }, val: 200.0 },
      { tag: { name: "warpShrinkage", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 100, dec: 1 }, val: 1.0 },
      { tag: { name: "fullWarpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
      { tag: { name: "warpBeamLength", group: "monitoring", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
    ]
    await db.query('DELETE FROM tags WHERE tag->>$1~$2', ['dev', 'rtu']);
    await db.query('INSERT INTO tags(tag,val,link) SELECT * FROM jsonb_to_recordset($1) as x(tag jsonb, val numeric, link boolean) ON CONFLICT (tag) DO NOTHING;', [JSON.stringify(tcpTags)])
    const ip1t = await db.query('SELECT tag->$3 as name, tag->$4 as addr, tag->$5 as type, tag->$6 as reg FROM tags WHERE tag->>$1=$2 ORDER BY tag->>$4 DESC ', ['dev', 'tcp1', 'name', 'addr', 'type', 'reg']);
    ip1.slaves.push(Object.assign({ name: 'tcp1' }, tcpRows.rows[0].data['tcp1'], { tags: ip1t.rows, mbarr: sortAndGroup(ip1t.rows) }));
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
      { tag: { name: "stopAngle", group: "monitoring", dev: "rtu1", addr: "8", type: "word", reg: "r", min: 0, max: 359, dec: 0 }, link: false },
      { tag: { name: "orderLength", group: "monitoring", dev: "rtu1", addr: "10", type: "float", reg: "r", min: 0, max: 1000, dec: 2 }, link: false },
      { tag: { name: "speedMainDrive", group: "monitoring", dev: "rtu1", addr: "6", type: "float", reg: "r", min: 0, max: 600, dec: 1 }, link: false },
      { tag: { name: "modeCode", group: "event", dev: "rtu1", addr: "0", type: "word", reg: "r", min: 0, max: 6, dec: 0 }, link: false },
      { tag: { name: "picksLastRun", group: "monitoring", dev: "rtu1", addr: "4", type: "float", reg: "r", min: -2147483648, max: 2147483647, dec: 4 }, link: false },
      { tag: { name: "realPicksLastRun", group: "monitoring", dev: "rtu1", addr: "2", type: "int32", reg: "r", min: -2147483648, max: 2147483647, dec: 0 }, link: false },
      { tag: { name: "modeControl", group: "event", dev: "rtu1", addr: "14", type: "word", reg: "rw", min: 0, max: 65535, dec: 0 }, link: false },
      { tag: { name: "planSpeedMainDrive", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 600, dec: 1 }, val: 200.0 },
      { tag: { name: "planClothDensity", group: "event", dev: "rtu1", type: "float", addr: "12", reg: "rw", min: 0.5, max: 1000, dec: 2 }, link: false },
      { tag: { name: "planOrderLength", group: "event", dev: "rtu1", type: "float", addr: "16", reg: "rw", min: 0, max: 1000, dec: 2 }, link: false },
      { tag: { name: "pickAngle", group: "event", dev: "rtu1", type: "word", addr: "18", reg: "rw", min: 0, max: 359, dec: 0 }, link: false },
      { tag: { name: "warpShrinkage", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 100, dec: 1 }, val: 1.0 },
      { tag: { name: "fullWarpBeamLength", group: "setting", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
      { tag: { name: "warpBeamLength", group: "monitoring", dev: "op", type: "float", reg: "rw", min: 0, max: 5000, dec: 1 }, val: 3000.0 },
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
          com1.slaves.push(Object.assign({ name: prop }, rtuRows.rows[0].data[prop], { tags: com1t.rows, mbarr: sortAndGroup(com1t.rows) }));
          break;
        case "opCOM2":
          //const com2t = await db.query('SELECT tag->$5 as name, tag->$6 as addr, tag->$7 as type, tag->$8 as reg FROM tags WHERE tag->>$1=$2 AND tag->>$3=$4', ['dev', prop, 'group', 'monitoring', 'name', 'addr', 'type', 'reg']);
          const com2t = await db.query('SELECT tag->$3 as name, tag->$4 as addr, tag->$5 as type, tag->$6 as reg FROM tags WHERE tag->>$1=$2 ORDER BY tag->>$4 DESC ', ['dev', prop, 'name', 'addr', 'type', 'reg']);
          com2.slaves.push(Object.assign({ name: prop }, rtuRows.rows[0].data[prop], { tags: com2t.rows, mbarr: sortAndGroup(com2t.rows) }));
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
      const ipConf = { opIP: { name: 'bloomhmi1', wired: { status: 'invisible', dhcp: false, netmask: '255.255.255.0', gateway_ip: '127.0.0.1', ip_address: '127.0.0.1', mac_address: '00:00:00:00:00:00' }, wireless: { status: 'invisible', dhcp: false, netmask: '255.255.255.0', gateway_ip: '127.0.0.1', ip_address: '127.0.0.1', mac_address: '00:00:00:00:00:00' }, wifi: { ssid: 'BloomConnect', pwd: 'textile2023' } }, tcp1: { ip: '192.168.1.123', port: '502', sId: 1, swapBytes: true, swapWords: true } }
      //console.log(ipConf)
      await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['ipConf', ipConf])
    })
    await SerialPort.list().then(async function (ports) {
      if (ports[0] !== undefined && com1.path == '') { com1.path = ports[0].path; } else if (com1.path == '') { com1.path = "COM1"; }
      if (ports[1] !== undefined && com2.path == '') { com2.path = ports[1].path; } else if (com2.path == '') { com2.path = "COM2"; }
      const comConf = { opCOM1: { path: com1.path, conf: { baudRate: 230400, parity: "none", dataBits: 8, stopBits: 1 }, scan: 0, timeout: 500 }, opCOM2: { path: com2.path, conf: { baudRate: 115200, parity: "none", dataBits: 8, stopBits: 1 }, scan: 0, timeout: 0 } }
      await db.query('INSERT INTO hwconfig VALUES($1,$2) ON CONFLICT (name) DO NOTHING;', ['comConf', comConf])
    });
    /*
    switch (process.platform) {
      case 'linux':
        sudo.exec(" nmcli con add con-name \"wireless\"  type wifi ifname wlp4s0 ssid \"BloomConnect\" wifi-sec.key-mgmt wpa-psk wifi-sec.psk \"textile2023\" ipv4.method auto ipv4.dns 8.8.8.8 && nmcli con up wireless", options, async (error, data, getter) => {
        });
        sudo.exec(" nmcli con add con-name \"wired\" type ethernet ifname enp2s0 ipv4.method auto ipv4.dns 8.8.8.8 && nmcli con up wired", options, async (error, data, getter) => {
        });
        break;
      case 'win32':
        break;
    }
  */
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
    //console.log(mbsStatus);
  } catch (e) {
    port.slaves.map(async (slave: any) => {
      await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 AND link=true;', ['dev', slave.name]);
    })
    port.mbsState = MBS_STATE_FAIL_CONNECT;
    mbsStatus = "[" + port.path + "]" + e.message;
    //console.log(mbsStatus);
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
          //console.log(mbsStatus);
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
          //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
          //console.log(mbsStatus);
        }
        break;
    }
  }
};
const readModbusData = async function (client, port, slave, group) {
  await client.setID(slave.sId);
  let count = slave.tags.length;
  if (group == true) {
    if (slave.mbarr.coils) {
      try {
        const data = await client.readCoils(slave.mbarr.coils.addr, slave.mbarr.coils.len);
        port.mbsState = MBS_STATE_GOOD_READ;
        mbsStatus = "success";
        await slave.mbarr.coils.tags.reduce(async (memo, tag) => {
          await memo;
          let val = await data.buffer[tag.addr - slave.mbarr.coils?.addr];
          if (val !== null) {
            const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false) RETURNING *;', [val, 'dev', slave.name, 'name', tag.name]);
            if (rows[0] && rows[0]['tag']['group'] == 'event') {
              sse.send(rows, 'tags', tag.name);
            }
            //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
          }
        }, undefined);
      } catch (e) {
        port.mbsState = MBS_STATE_FAIL_READ;
        //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]COILS" + " " + e.message;
        //console.log(mbsStatus);
        if (e.message.includes('Timed out')) {
          await slave.mbarr.coils.tags.reduce(async (memo, tag) => {
            await memo;
            const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *', ['dev', slave.name, 'name', tag.name]);
            if (rows[0]) {
              sse.send(rows, 'tags', tag.name);
            }
          }, undefined);
        }
      }
    }
    if (slave.mbarr.discr) {
      try {
        const data = await client.readDiscreteInputs(slave.mbarr.discr.addr, slave.mbarr.discr.len);
        port.mbsState = MBS_STATE_GOOD_READ;
        mbsStatus = "success";
        await slave.mbarr.discr.tags.reduce(async (memo, tag) => {
          await memo;
          let val = await data.buffer[tag.addr - slave.mbarr.discr?.addr];
          if (val !== null) {
            const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false) RETURNING *;', [val, 'dev', slave.name, 'name', tag.name]);
            if (rows[0] && rows[0]['tag']['group'] == 'event') {
              sse.send(rows, 'tags', tag.name);
            }
            //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
          }
        }, undefined);
      } catch (e) {
        port.mbsState = MBS_STATE_FAIL_READ;
        //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]COILS" + " " + e.message;
        //console.log(mbsStatus);
        if (e.message.includes('Timed out')) {
          await slave.mbarr.discr.tags.reduce(async (memo, tag) => {
            await memo;
            const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *', ['dev', slave.name, 'name', tag.name]);
            if (rows[0]) {
              sse.send(rows, 'tags', tag.name);
            }
          }, undefined);
        }
      }
    }
    if (slave.mbarr.hregs) {
      try {
        const data = await client.readHoldingRegisters(slave.mbarr.hregs.addr, slave.mbarr.hregs.len);
        port.mbsState = MBS_STATE_GOOD_READ;
        mbsStatus = "success";
        await slave.mbarr.hregs.tags.reduce(async (memo, tag) => {
          await memo;
          const buf = await data.buffer.slice((tag.addr - slave.mbarr.hregs?.addr) * 2, (tag.addr - slave.mbarr.hregs?.addr) * 2 + getByteLength(tag.type))
          if (slave.swapBytes) { await buf.swap16(); }
          let val;
          switch (tag.type) {
            case 'dword':
              val = slave.swapWords ? await buf.readUInt32LE(0) : await buf.readUInt32BE(0);
              break;
            case 'int32':
              val = slave.swapWords ? await buf.readInt32LE(0) : await buf.readInt32BE(0);
              break;
            case 'word':
              val = slave.swapWords ? await buf.readUInt16LE(0) : await buf.readUInt16BE(0);
              break;
            case 'float':
              val = slave.swapWords ? await buf.readFloatLE(0) : await buf.readFloatBE(0);
              break;
            default:
              break;
          }
          if (val !== null) {
            const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND ( (round(val::numeric,(tag->>$6)::integer)) IS DISTINCT FROM (round($1::numeric,(tag->>$6)::integer)) OR link=false) RETURNING tag, (round(val::numeric,(tag->>$6)::integer)) as val, updated, link;', [val, 'dev', slave.name, 'name', tag.name, 'dec']);
            if (rows[0] && rows[0]['tag']['group'] == 'event') {
              sse.send(rows, 'tags', tag.name);
            }
            //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
          }
        }, undefined);
      } catch (e) {
        port.mbsState = MBS_STATE_FAIL_READ;
        if (e.message.includes('Timed out')) {
          await slave.mbarr.hregs.tags.reduce(async (memo, tag) => {
            await memo;
            //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
            //console.log(mbsStatus);
            //console.log(e);
            const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING tag, (round(val::numeric,(tag->>$5)::integer)) as val, updated, link;', ['dev', slave.name, 'name', tag.name, 'dec']);
            if (rows[0]) {
              sse.send(rows, 'tags', tag.name);
            }
          }, undefined);
        }
      }
    }
    if (slave.mbarr.iregs) {
      try {
        const data = await client.readInputRegisters(slave.mbarr.iregs.addr, slave.mbarr.iregs.len);
        port.mbsState = MBS_STATE_GOOD_READ;
        mbsStatus = "success";
        await slave.mbarr.iregs.tags.reduce(async (memo, tag) => {
          await memo;
          const buf = await data.buffer.slice((tag.addr - slave.mbarr.iregs?.addr) * 2, (tag.addr - slave.mbarr.iregs?.addr) * 2 + getByteLength(tag.type))
          if (slave.swapBytes) { await buf.swap16(); }
          let val;
          switch (tag.type) {
            case 'dword':
              val = slave.swapWords ? await buf.readUInt32LE(0) : await buf.readUInt32BE(0);
              break;
            case 'int32':
              val = slave.swapWords ? await buf.readInt32LE(0) : await buf.readInt32BE(0);
              break;
            case 'word':
              val = slave.swapWords ? await buf.readUInt16LE(0) : await buf.readUInt16BE(0);
              break;
            case 'float':
              val = slave.swapWords ? await buf.readFloatLE(0) : await buf.readFloatBE(0);
              break;
            default:
              break;
          }
          if (val !== null) {
            const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND ( (round(val::numeric,(tag->>$6)::integer)) IS DISTINCT FROM (round($1::numeric,(tag->>$6)::integer)) OR link=false) RETURNING tag, (round(val::numeric,(tag->>$6)::integer)) as val, updated, link;', [val, 'dev', slave.name, 'name', tag.name, 'dec']);
            if (rows[0] && rows[0]['tag']['group'] == 'event') {
              if (tag.name == 'modeCode') {
                modeCodeProcess(rows);
              }
              else { sse.send(rows, 'tags', tag.name); }
            }
            //console.log('[' + new Date().toJSON() + ']' + "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
          }
        }, undefined);
      } catch (e) {
        port.mbsState = MBS_STATE_FAIL_READ;
        if (e.message.includes('Timed out')) {
          await slave.mbarr.iregs.tags.reduce(async (memo, tag) => {
            await memo;
            //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
            //console.log(mbsStatus);
            //console.log(e);
            if (tag.name == 'modeCode') {
              const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *;', ['dev', slave.name, 'name', tag.name]);
              if (rows[0]) {
                sse.send(rows, 'tags', tag.name);
              }
            }
            else {
              const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING tag, (round(val::numeric,(tag->>$5)::integer)) as val, updated, link;', ['dev', slave.name, 'name', tag.name, 'dec']);
              if (rows[0]) {
                sse.send(rows, 'tags', tag.name);
              }
            }
          }, undefined);
        }
      }
    }
  }
  else {
    await process(slave.tags[count - 1])
  }

  async function modeCodeProcess(rows) {
    let info: { rows: any[] };
    do {
      info = await db.query('SELECT * FROM getcurrentinfo();');
      info.rows[0]['userinfo'] && await info.rows[0]['userinfo']['stops'].map((row: any) => {
        row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
      });
      info.rows[0]['shiftinfo'] && await info.rows[0]['shiftinfo']['stops'].map((row: any) => {
        row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
      });
      info.rows[0]['dayinfo'] && await info.rows[0]['dayinfo']['stops'].map((row: any) => {
        row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
      });
      info.rows[0]['monthinfo'] && await info.rows[0]['monthinfo']['stops'].map((row: any) => {
        row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
      });
      info.rows[0]['shift'] && (info.rows[0]['shift']['shiftdur'] = parseInterval(info.rows[0]['shift']['shiftdur']))
      info.rows[0]['userinfo'] && (info.rows[0]['userinfo']['runtime'] = parseInterval(info.rows[0]['userinfo']['runtime']))
      info.rows[0]['userinfo'] && (info.rows[0]['userinfo']['workdur'] = parseInterval(info.rows[0]['userinfo']['workdur']))
      info.rows[0]['shiftinfo'] && (info.rows[0]['shiftinfo']['runtime'] = parseInterval(info.rows[0]['shiftinfo']['runtime']))
      info.rows[0]['dayinfo'] && (info.rows[0]['dayinfo']['runtime'] = parseInterval(info.rows[0]['dayinfo']['runtime']))
      info.rows[0]['monthinfo'] && (info.rows[0]['monthinfo']['runtime'] = parseInterval(info.rows[0]['monthinfo']['runtime']))
      info.rows[0]['lifetime'] && (info.rows[0]['lifetime']['motor'] = parseInterval(info.rows[0]['lifetime']['motor']))
      info.rows[0]['modeCode'] = { val: rows[0]['val'], updated: rows[0]['updated'] }
      //console.log('wait')
    } while (info.rows[0]['tags'].filter((tag: any) => { return ['realPicksLastRun', 'picksLastRun'].includes(tag['tag']['name']) && ((new Date(tag['updated'])) > (new Date(rows[0]['updated']))) }).length != 2)
    sse.send(info.rows[0], 'fullinfo', 'all');
    sse.send(rows, 'tags', 'modeCode');
    //console.log('[' + new Date().toJSON() + ']' + "modeCode processed")
  }

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
              const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false) RETURNING *;', [val, 'dev', slave.name, 'name', tag.name]);
              if (rows[0] && rows[0]['tag']['group'] == 'event') {
                sse.send(rows, 'tags', tag.name);
              }
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              if (e.message.includes('Timed out')) {
                const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *', ['dev', slave.name, 'name', tag.name]);
                if (rows[0]) {
                  sse.send(rows, 'tags', tag.name);
                }
              }
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
                case 'int32':
                  val = slave.swapWords ? data.buffer.readInt32LE(0) : data.buffer.readInt32BE(0);
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
              const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND ( (round(val::numeric,(tag->>$6)::integer)) IS DISTINCT FROM (round($1::numeric,(tag->>$6)::integer)) OR link=false) RETURNING tag, (round(val::numeric,(tag->>$6)::integer)) as val, updated, link;', [val, 'dev', slave.name, 'name', tag.name, 'dec']);
              if (rows[0] && rows[0]['tag']['group'] == 'event') {
                sse.send(rows, 'tags', tag.name);
              }
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              if (e.message.includes('Timed out')) {
                const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING tag, (round(val::numeric,(tag->>$5)::integer)) as val, updated, link;', ['dev', slave.name, 'name', tag.name, 'dec']);
                if (rows[0]) {
                  sse.send(rows, 'tags', tag.name);
                }
              }
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
              const { rows } = db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND (val IS DISTINCT FROM $1 OR link=false) RETURNING *;', [val, 'dev', slave.name, 'name', tag.name]);
              if (rows[0] && rows[0]['tag']['group'] == 'event') {
                sse.send(rows, 'tags', tag.name);
              }
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              if (e.message.includes('Timed out')) {
                const { rows } = db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *;', ['dev', slave.name, 'name', tag.name]);
                if (rows[0]) {
                  sse.send(rows, 'tags', tag.name);
                }
              }
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
                case 'int32':
                  val = slave.swapWords ? data.buffer.readInt32LE(0) : data.buffer.readInt32BE(0);
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
              const { rows } = await db.query('UPDATE tags SET val=$1, updated=current_timestamp, link=true where tag->>$2=$3 and tag->>$4=$5 AND ( (round(val::numeric,(tag->>$6)::integer)) IS DISTINCT FROM (round($1::numeric,(tag->>$6)::integer)) OR link=false) RETURNING tag, (round(val::numeric,(tag->>$6)::integer)) as val, updated, link;', [val, 'dev', slave.name, 'name', tag.name, 'dec']);
              if (rows[0] && rows[0]['tag']['group'] == 'event') {
                if (tag.name == 'modeCode') {
                  const info = await db.query('SELECT * FROM getcurrentinfo();');
                  info.rows[0]['userinfo'] && await info.rows[0]['userinfo']['stops'].map((row: any) => {
                    row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
                  });
                  info.rows[0]['shiftinfo'] && await info.rows[0]['shiftinfo']['stops'].map((row: any) => {
                    row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
                  });
                  info.rows[0]['dayinfo'] && await info.rows[0]['dayinfo']['stops'].map((row: any) => {
                    row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
                  });
                  info.rows[0]['monthinfo'] && await info.rows[0]['monthinfo']['stops'].map((row: any) => {
                    row[Object.keys(row)[0]].dur = parseInterval(row[Object.keys(row)[0]].dur)
                  });
                  info.rows[0]['shift'] && (info.rows[0]['shift']['shiftdur'] = parseInterval(info.rows[0]['shift']['shiftdur']))
                  info.rows[0]['userinfo'] && (info.rows[0]['userinfo']['runtime'] = parseInterval(info.rows[0]['userinfo']['runtime']))
                  info.rows[0]['userinfo'] && (info.rows[0]['userinfo']['workdur'] = parseInterval(info.rows[0]['userinfo']['workdur']))
                  info.rows[0]['shiftinfo'] && (info.rows[0]['shiftinfo']['runtime'] = parseInterval(info.rows[0]['shiftinfo']['runtime']))
                  info.rows[0]['dayinfo'] && (info.rows[0]['dayinfo']['runtime'] = parseInterval(info.rows[0]['dayinfo']['runtime']))
                  info.rows[0]['monthinfo'] && (info.rows[0]['monthinfo']['runtime'] = parseInterval(info.rows[0]['monthinfo']['runtime']))
                  info.rows[0]['lifetime'] && (info.rows[0]['lifetime']['motor'] = parseInterval(info.rows[0]['lifetime']['motor']))
                  info.rows[0]['modeCode'] = { val: rows[0]['val'], updated: rows[0]['updated'] }
                  sse.send(info.rows[0], 'fullinfo', 'all');
                }
                sse.send(rows, 'tags', tag.name);
              }
              //console.log("[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " = " + val);
              if (count > 1) { count--; await process(slave.tags[count - 1]); }
            } catch (e) {
              port.mbsState = MBS_STATE_FAIL_READ;
              //mbsStatus = "[" + port.path + "]" + "[#" + slave.sId + "]" + tag.name + " " + e.message;
              //console.log(mbsStatus);
              //console.log(e);
              if (e.message.includes('Timed out')) {
                if (tag.name == 'modeCode') {
                  const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false, val=0 where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING *;', ['dev', slave.name, 'name', tag.name]);
                  if (rows[0]) {
                    sse.send(rows, 'tags', tag.name);
                  }
                }
                else {
                  const { rows } = await db.query('UPDATE tags SET updated=current_timestamp, link=false where tag->>$1=$2 and tag->>$3=$4 AND link=true RETURNING tag, (round(val::numeric,(tag->>$5)::integer)) as val, updated, link;', ['dev', slave.name, 'name', tag.name, 'dec']);
                  if (rows[0]) {
                    sse.send(rows, 'tags', tag.name);
                  }
                }
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
    await client.close(async () => { /*console.log("[" + port.path + "]closed");*/ });
    await dbConf();
    resetFlagConn();
  }
  if ((updFlagCOM1 && port.self == 'com1') || (updFlagCOM2 && port.self == 'com2') || (updFlagTCP1 && port.self == 'ip1')) {
    await client.close(async () => { /*console.log("[" + port.path + "]closed");*/ });
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
        nextAction = await readModbusData(client, port, slave, true);
        break;

      case MBS_STATE_GOOD_CONNECT:
        nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave, true);
        break;

      case MBS_STATE_FAIL_CONNECT:
        nextAction = await connectClient(client, port);
        break;
      case MBS_STATE_GOOD_WRITE:
      case MBS_STATE_GOOD_READ:
        nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave, true);
        break;

      case MBS_STATE_FAIL_READ:
      case MBS_STATE_FAIL_WRITE:
        if (client.isOpen) { port.mbsState = MBS_STATE_NEXT; nextAction = writeTrig ? await writeModbusData(writeTag.name, writeTag.val) : await readModbusData(client, port, slave, true); }
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
  else if (port.slaves.length == 0) { if (client.isOpen) { await client.close(async () => { /*console.log("[" + port.path + "]closed");*/ }); } }
  port.act++;
  if (port.act === port.slaves.length) {
    port.act = 0;
  }
  await delay(port.scan);
  //console.log("runmodbus" + port.path + port.mbsState + port.slaves.length)
  if ((port.ip && contype == 'ip') || (!port.ip && contype == 'com')) { runModbus(client, port); }
  else { port.mbsState = MBS_STATE_STEADY; (port.self == 'com1') && resetFlagCOM1(); (port.self == 'com2') && resetFlagCOM2(); (port.self == 'ip1') && resetFlagTCP1(); }
};

function sortAndGroup(arr) {
  const group1: any[] = [];
  const group2: any[] = [];
  const group3: any[] = [];
  const group4: any[] = [];

  arr.sort((a, b) => a.addr - b.addr);

  for (let i = 0; i < arr.length; i++) {
    if (arr[i].reg === 'r' && arr[i].type === 'bool') {
      group1.push(arr[i]);
    } else if (arr[i].reg === 'rw' && arr[i].type === 'bool') {
      group2.push(arr[i]);
    } else if (arr[i].reg === 'r' && arr[i].type !== 'bool') {
      group3.push(arr[i]);
    } else if (arr[i].reg === 'rw' && arr[i].type !== 'bool') {
      group4.push(arr[i]);
    }
  }
  return { ...(group1?.length > 0 && { discr: { tags: group1, addr: group1[0]?.addr, len: group1?.length } }), ...(group2?.length > 0 && { coils: { tags: group2, addr: group2[0]?.addr, len: group2?.length } }), ...(group3?.length > 0 && { iregs: { tags: group3, addr: group3[0]?.addr, len: group3[group3.length - 1]?.addr - group3[0]?.addr + getByteLength(group3[group3.length - 1]?.type) / 2 } }), ...(group4?.length > 0 && { hregs: { tags: group4, addr: group4[0]?.addr, len: group4[group4.length - 1]?.addr - group4[0]?.addr + getByteLength(group4[group4.length - 1]?.type) / 2 } }) };
}
