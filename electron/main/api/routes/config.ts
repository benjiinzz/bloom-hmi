import PromiseRouter from 'express-promise-router'
import db from '../../db'
import sudo from 'sudo-prompt'
const options = {
  name: 'Electron',
};
export let updFlagCOM1 = false;
export let updFlagCOM2 = false;
export let updFlagTCP1 = false;
export let updFlagConn = false;
export const resetFlagCOM1 = () => {
  updFlagCOM1 = false;
}
export const resetFlagCOM2 = () => {
  updFlagCOM2 = false;
}
export const resetFlagTCP1 = () => {
  updFlagTCP1 = false;
}
export const resetFlagConn = () => {
  updFlagConn = false;
}
// create a new express-promise-router
// this has the same API as the normal express router except
// it allows you to use async functions as route handlers
const router = PromiseRouter();

const maskToPrefixLength = (mask: string) => {
  return mask.split('.')
    .reduce((c, o) => c - Math.log2(256 - +o), 32);
}

// export our router to be mounted by the parent application
router.get('/', async (req, res) => {
  const { rows } = await db.query('SELECT * FROM hwconfig')

  let mobj, obj
  let conf
  mobj = {}
  for (let row of rows) {
    obj = {}
    for (let prop in row) {
      if (prop == 'name') {
        conf = row[prop]
        mobj = Object.assign(mobj, { [conf]: {} })
      }
      else if (prop == 'data') {
        obj = Object.assign(obj, row[prop])
      }
    }
    mobj[conf] = obj
  }
  res.status(200).send(mobj)
})

router.post('/update', async (req, res) => {
  try {
    if (req.body.opIP) {

      const { opIP } = req.body;
      opIP.name && await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, name}', '"' + opIP.name + '"']);

      if (opIP.wired) {
        if (opIP.wired.dhcp == false) {
          switch (process.platform) {
            case 'linux':
              sudo.exec('nmcli con del wired && nmcli con add con-name "wired" type $(nmcli device status | awk -v e=\'ethernet\' -v b=\'bridge\' \'BEGIN {cnt=0} $2==e {cnt++} END {print (cnt>=2? b:e)}\') ifname $(nmcli device status | awk -v e=\'ethernet\' \'$2==e\' | sort | awk -v e=\'ethernet\' -v a=\'app-br0\' \'BEGIN {cnt=0} $2==e {cnt++} END {print (cnt>=2? a:$1)}\') ipv4.method manual ip4 ' + opIP.wired.ip_address + "/" + maskToPrefixLength(opIP.wired.netmask) + " gw4 " + opIP.wired.gateway_ip + " && nmcli con down wired && nmcli con up wired", options, async (error, data, getter) => {
                if (!error) {
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, dhcp}', opIP.wired.dhcp]);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, ip_address}', '"' + opIP.wired.ip_address + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, netmask}', '"' + opIP.wired.netmask + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, gateway_ip}', '"' + opIP.wired.gateway_ip + '"']);
                  res.status(200).json({
                    message: "notifications.confupdate",
                  });
                }
                else {
                  res.status(500).json({
                    error: "Could not change opIP",
                    message: "notifications.servererror",
                  });
                }
              });
              break;
            case 'win32':
              //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, dhcp}', opIP.wired.dhcp]);
              //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, ip_address}', '"' + opIP.wired.ip_address + '"']);
              //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, netmask}', '"' + opIP.wired.netmask + '"']);
              //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, gateway_ip}', '"' + opIP.wired.gateway_ip + '"']);
              break;
          }
        }
        else {
          switch (process.platform) {
            case 'linux':
              sudo.exec('nmcli con del wired && nmcli con add con-name "wired" type $(nmcli device status | awk -v e=\'ethernet\' -v b=\'bridge\' \'BEGIN {cnt=0} $2==e {cnt++} END {print (cnt>=2? b:e)}\') ifname $(nmcli device status | awk -v e=\'ethernet\' \'$2==e\' | sort | awk -v e=\'ethernet\' -v a=\'app-br0\' \'BEGIN {cnt=0} $2==e {cnt++} END {print (cnt>=2? a:$1)}\') ipv4.method auto && nmcli con down wired && nmcli con up wired', options, async (error, data, getter) => {
                if (!error) {
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, dhcp}', opIP.wired.dhcp]);
                  res.status(200).json({
                    message: "notifications.confupdate",
                  });
                }
                else {
                  res.status(500).json({
                    error: "Could not change opIP",
                    message: "notifications.servererror",
                  });
                }
              });
              break;
            case 'win32':
              //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wired, dhcp}', opIP.wired.dhcp]);
              break;
          }
        }
      }

      if (opIP.wifi) {
        switch (process.platform) {
          case 'linux':
            if (opIP.wireless.dhcp == false) {
              sudo.exec('nmcli con del wireless && nmcli con add con-name "wireless" type wifi ifname $(nmcli device status | awk -v w=\'wifi\' \'$2==w {print $1}\' | sed -n 1p) ssid "' + opIP.wifi.ssid + "\" wifi-sec.key-mgmt wpa-psk wifi-sec.psk \"" + opIP.wifi.pwd + "\" ipv4.method manual ip4 " + opIP.wireless.ip_address + "/" + maskToPrefixLength(opIP.wireless.netmask) + " gw4 " + opIP.wireless.gateway_ip + " && nmcli con down wireless && nmcli con up wireless", options, async (error, data, getter) => {
                if (!error) {
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, ssid}', '"' + opIP.wifi.ssid + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, pwd}', '"' + opIP.wifi.pwd + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, dhcp}', opIP.wireless.dhcp]);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, ip_address}', '"' + opIP.wireless.ip_address + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, netmask}', '"' + opIP.wireless.netmask + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, gateway_ip}', '"' + opIP.wireless.gateway_ip + '"']);
                  res.status(200).json({
                    message: "notifications.confupdate",
                  });
                }
                else {
                  res.status(500).json({
                    error: "Could not change opIP",
                    message: "notifications.servererror",
                  });
                }
              });
            }
            else {
              sudo.exec('nmcli con del wireless && nmcli con add con-name "wireless" type wifi ifname $(nmcli device status | awk -v w=\'wifi\' \'$2==w {print $1}\' | sed -n 1p) ssid "' + opIP.wifi.ssid + "\" wifi-sec.key-mgmt wpa-psk wifi-sec.psk \"" + opIP.wifi.pwd + "\" ipv4.method auto && nmcli con down wireless && nmcli con up wireless", options, async (error, data, getter) => {
                if (!error) {
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, ssid}', '"' + opIP.wifi.ssid + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, pwd}', '"' + opIP.wifi.pwd + '"']);
                  await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wireless, dhcp}', opIP.wireless.dhcp]);
                  res.status(200).json({
                    message: "notifications.confupdate",
                  });
                }
                else {
                  res.status(500).json({
                    error: "Could not change opIP",
                    message: "notifications.servererror",
                  });
                }
              });
            }
            break;
          case 'win32':
            //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, ssid}', '"' + opIP.wifi.ssid + '"']);
            //await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, wifi, pwd}', '"' + opIP.wifi.pwd + '"']);
            break;
        }
      }

      switch (process.platform) {
        case 'linux':
          if (opIP.name) {
            sudo.exec("nmcli general hostname " + opIP.name + " && rm ~/.Xauthority && touch ~/.Xauthority && xauth generate :0 . trusted && xauth generate unix:0 . trusted", options, async (error, data, getter) => {
              if (!error) {
                await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{opIP, name}', '"' + opIP.name + '"']);
                res.status(200).json({
                  message: "notifications.confupdate",
                });
              }
            });
          }
          /*
          sudo.exec("ip addr flush dev eth0 && ifconfig eth0 " + opIP.ip_address + " netmask " + opIP.netmask + " && ip route add default via " + opIP.gateway_ip + " dev eth0", options, (error, data, getter) => {
            if (!error) {
              res.status(200).json({
                message: "notifications.confupdate",
              });
            }
          });
          */
          break;
        case 'win32':
          //console.log(maskToPrefixLength(opIP.netmask))
          /*
          sudo.exec("powershell -command \"Remove-NetIPAddress -InterfaceAlias Ethernet -Confirm:$false; Remove-NetRoute -InterfaceAlias Ethernet -Confirm:$false; New-NetIPAddress -InterfaceAlias Ethernet -AddressFamily IPv4 " + opIP.ip_address + " -PrefixLength " + maskToPrefixLength(opIP.netmask) + " -DefaultGateway " + opIP.gateway_ip + " -Type Unicast  -Confirm:$false\"", options, (error, data, getter) => {
            if (!error) {
              res.status(200).json({
                message: "notifications.confupdate",
              });
            }
            else {
              res.status(500).json({
                error: "Could not change opIP",
                message: "notifications.servererror",
              });
            }
          });
          */
          break;
      }
    }
    else if (req.body.opCOM1) {

      const { opCOM1 } = req.body;
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, path}', '"' + opCOM1.path + '"']);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, scan}', opCOM1.scan]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, timeout}', opCOM1.timeout]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, conf, baudRate}', opCOM1.conf.baudRate]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, conf, dataBits}', opCOM1.conf.dataBits]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, conf, stopBits}', opCOM1.conf.stopBits]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM1, conf, parity}', '"' + opCOM1.conf.parity + '"']);
      updFlagCOM1 = true;
      res.status(200).json({
        message: "notifications.confupdate",
      });
    }
    else if (req.body.opCOM2) {
      const { opCOM2 } = req.body;
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, path}', '"' + opCOM2.path + '"']);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, scan}', opCOM2.scan]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, timeout}', opCOM2.timeout]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, conf, baudRate}', opCOM2.conf.baudRate]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, conf, dataBits}', opCOM2.conf.dataBits]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, conf, stopBits}', opCOM2.conf.stopBits]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['comConf', '{opCOM2, conf, parity}', '"' + opCOM2.conf.parity + '"']);
      updFlagCOM2 = true;
      res.status(200).json({
        message: "notifications.confupdate",
      });
    }
    else if (req.body.rtu1) {
      const { rtu1 } = req.body;
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['rtuConf', '{rtu1, com}', '"' + rtu1.com + '"']);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['rtuConf', '{rtu1, sId}', rtu1.sId]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['rtuConf', '{rtu1, swapBytes}', rtu1.swapBytes]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['rtuConf', '{rtu1, swapWords}', rtu1.swapWords]);
      updFlagCOM1 = true;
      updFlagCOM2 = true;
      res.status(200).json({
        message: "notifications.confupdate",
      });
    }
    else if (req.body.tcp1) {
      const { tcp1 } = req.body;
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{tcp1, ip}', '"' + tcp1.ip + '"']);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{tcp1, port}', '"' + tcp1.port + '"']);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{tcp1, sId}', tcp1.sId]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{tcp1, swapBytes}', tcp1.swapBytes]);
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['ipConf', '{tcp1, swapWords}', tcp1.swapWords]);
      updFlagTCP1 = true;
      res.status(200).json({
        message: "notifications.confupdate",
      });
    }
    else if (req.body.conn) {
      const { conn } = req.body;
      await db.query('UPDATE hwconfig set data = jsonb_set(data, $2, $3) where name=$1', ['connConf', '{conn}', '"' + conn + '"']);
      updFlagConn = true;
      res.status(200).json({
        message: "notifications.confupdate",
      });
    }
  }
  catch (err) {
    /*console.log(err);*/
    res.status(500).json({
      message: "notifications.dberror",
      error: "Database error while updating config!", //Database connection error
    });
  };
});

export default router
