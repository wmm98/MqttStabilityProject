from gmqtt.mqtt.constants import MQTTv311
import asyncio
from gmqtt import Client as MQTTClient
import aiofiles
from config import MQTTConfig, MyLog
import json
import os
import aiohttp
import hashlib
import socket
import argparse
import platform

log = MyLog()


class DeviceClient:
    connected_devices = set()  # Set to keep track of connected devices
    md5_failed_devices = set()
    md5_success_devices = set()

    def __init__(self, device_sn, broker, port, username, password):
        self.device_sn = device_sn
        self.broker = broker
        self.port = port
        self.username = username
        self.password = password
        self.client = MQTTClient(client_id=f"MT_{device_sn}")
        self.client.sock = None  # 初始化 sock 属性
        self.client.on_socket_open = self.on_socket_open
        # 方案1
        # === 新增：启用端口重用 ===

    def on_socket_open(self, client, userdata, sock):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # 禁用Nagle算法
        if hasattr(socket, 'SO_REUSEPORT'):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        # 调整缓冲区大小（根据实际情况调整）
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8192)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8192)

    async def connect(self, max_retries=5):
        retries = 0
        while retries < max_retries:
            try:
                self.client.set_auth_credentials(self.username, self.password)
                await self.client.connect(host=self.broker, port=self.port, version=MQTTv311)
                log.info(f"{self.device_sn} connected to {self.broker}:{self.port}")
                return
            except (asyncio.TimeoutError, OSError) as e:
                retries += 1
                log.error(
                    f"{self.device_sn} failed to connect to {self.broker}:{self.port}, retry {retries}/{max_retries}: {str(e)}")
                await asyncio.sleep(2 ** retries)  # Exponential backoff
        log.error(f"{self.device_sn} failed to connect after {max_retries} retries")

    async def disconnect(self):
        await self.client.disconnect()

    def on_connect(self, client, flags, rc, properties):
        if rc == 0:
            DeviceClient.connected_devices.add(self.device_sn)
            port = client._connection._transport.get_extra_info('sockname')[1]
            log.info(f"{self.device_sn}:{port} connected to {self.broker} successfully.")
            # 订阅相关的主题
            client.subscribe(MQTTConfig.org_sub_topic_default, qos=MQTTConfig.qos)
            client.subscribe(MQTTConfig.org_sub_topic_department, qos=MQTTConfig.qos)
            client.subscribe(MQTTConfig.other_sub_topic_group, qos=MQTTConfig.qos)
            client.subscribe(MQTTConfig.sub_topic + self.device_sn, qos=MQTTConfig.qos)
            log.info(f"{self.device_sn} subscribe topic ")
            log.info(f"Total connected devices: {len(DeviceClient.connected_devices)}")
            print("Total connected devices: %d" % len(DeviceClient.connected_devices))
        else:
            log.error(f"{self.device_sn} failed to connect to {self.broker}. Return code: {rc}")
            print("%s failed to connect to %s. Return code: %d" % (self.device_sn, self.broker, rc))

    def on_disconnect(self, client, userdata, rc, properties=None):
        try:
            log.info(f"{self.device_sn} disconnected")
            DeviceClient.connected_devices.discard(self.device_sn)
            log.info(f"Total connected devices: {len(DeviceClient.connected_devices)}")
            print("Total connected devices: %d" % len(DeviceClient.connected_devices))
        except Exception as e:
            print("%s error in on_disconnect:" % self.device_sn)
            print(e)
            log.error(f"{self.device_sn} error in on_disconnect: {str(e)}")

    async def report_progress(self, client, data):
        response = json.dumps(data)
        client.publish(MQTTConfig.pub_topic, response, qos=MQTTConfig.qos)

    async def report_message(self, client, data):
        task_id = data.get("taskId", "")
        response = {
            "cmd": "sendDisplayResp",
            "sn": self.device_sn,
            "taskId": task_id,
            "param": {"status": "2", "failDes": ""}
        }
        await self.report_progress(client, response)
        log.info(f"{self.device_sn} report topic message")

    async def on_message(self, client, topic, payload, qos, properties):
        log.info(f"{self.username} received message on topic {topic}: {payload.decode()}")
        data = json.loads(payload.decode())
        if data.get("cmd") == "sendDisplay":
            await self.report_message(client, data)
        elif data.get("cmd") == "apkUpgrade":
            await self.download_and_report_apk_file(client, data)
        # elif data.get("cmd") == "otaUpgrade":
        #     self.download_ota_file(client, data)
        elif data.get("cmd") == "customResourcePoster":
            await self.download_and_report_hb_file(client, data)
        else:
            log.error(f"设备 {self.device_sn} 收到未知指令")
            return

    async def download_and_report_apk_file(self, client_, data):
        url = data.get("param", {}).get("apkDownloadUrl", "")
        task_id = data.get("taskId", "")
        app_name = data.get("param", {}).get("applicationName", "")
        app_version = data.get("param", {}).get("apkVersion", "")
        device_sn = self.device_sn
        report_data = {}
        json_data = {
            "cmd": "apkUpgradeResp",
            "sn": device_sn,
            "taskId": task_id,
            "param": {
                "status": "",
                "applicationName": app_name,
                "apkVersion": app_version,
                "failDes": ""
            }
        }

        report_data["report_data"] = json_data
        report_data["url"] = url
        report_data["client"] = client_
        report_data["device_sn"] = device_sn
        report_data["file_path"] = os.path.join(MQTTConfig.apk_download_path, device_sn, app_name)
        if os.path.exists(report_data["file_path"]):
            os.remove(report_data["file_path"])
        await self.download_file(report_data)

    async def download_file(self, data):
        print(data)
        file_path = data["file_path"]
        device_sn = data["device_sn"]
        report_data = data["report_data"]
        url = data["url"]
        dev_client = data["client"]
        cmd = report_data["cmd"]

        downloaded = 0
        last_reported = 0
        retries = 0
        timeout = aiohttp.ClientTimeout(total=300)  # 5分钟超时

        loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while dev_client.is_connected:
                try:
                    async with session.get(url) as response1:
                        response1.raise_for_status()
                        total_length = int(response1.headers.get('content-length', 0))
                        if total_length == 0:
                            log.error(f"{device_sn} apk file size is 0")
                            return

                        # 支持断点续传（需服务器支持Range头）
                        headers = {}
                        if os.path.exists(file_path):
                            downloaded = os.path.getsize(file_path)
                            headers['Range'] = f'bytes={downloaded}-'

                        async with session.get(url, headers=headers) as response:
                            if response.status == 206:
                                log.info(f"{device_sn} resuming download from {downloaded} bytes")
                            elif response.status == 200:
                                log.info(f"{device_sn} starting download from the beginning")
                            elif response.status == 429:
                                raise aiohttp.ClientError(f"{device_sn} rate limited")

                            async with aiofiles.open(file_path, 'ab') as f:  # 追加模式写入
                                async for chunk in response.content.iter_chunked(8192):
                                    if not chunk:
                                        continue
                                    await f.write(chunk)
                                    downloaded += len(chunk)
                                    current_progress = int((downloaded * 100) // total_length)

                                    # 减少日志频率，避免阻塞
                                    if current_progress > last_reported and current_progress % 10 == 0:
                                        log.info(f"{device_sn} progress: {current_progress}%")
                                    last_reported = current_progress

                                    # 上报100%时候
                                    if current_progress == 100:
                                        # log.info(f"{device_sn} progress: {current_progress}%")
                                        await f.flush()
                                        await loop.run_in_executor(None, os.fsync, f.fileno())
                                        test_md5 = self.get_md5(file_path)
                                        if cmd == "apkUpgradeResp":
                                            if test_md5 == MQTTConfig.apk_md5:
                                                report_data["param"]["status"] = "2"
                                                report_data["param"]["failDes"] = f"Downloaded {current_progress}% , MD5 check success"
                                                log.info("%s MD5 check success" % device_sn)
                                            else:
                                                report_data["param"]["status"] = "3"
                                                report_data["param"]["failDes"] = f"Downloaded {current_progress}% , MD5 check failed"
                                                log.error("%s MD5 check fail" % device_sn)
                                        await self.report_progress(dev_client, report_data)
                                        return

                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                    log.error(f"{device_sn} error: {str(e)}, retry {retries + 1}")
                    retries += 1
                    await asyncio.sleep(2 ** retries)  # 指数退避
                except Exception as e:
                    log.error(f"{device_sn} unexpected error: {str(e)}")
                    return

    async def download_and_report_hb_file(self, client, data):
        url = data.get("param", {}).get("apkDownloadUrl", "")
        task_id = data.get("taskId", "")
        app_name = data.get("param", {}).get("applicationName", "")
        app_version = data.get("param", {}).get("apkVersion", "")
        device_sn = self.device_sn

        file_path = os.path.join(MQTTConfig.hb_download_path, device_sn, app_name)

        downloaded = 0
        last_reported = 0
        retries = 0
        timeout = aiohttp.ClientTimeout(total=300)  # 5分钟超时

        loop = asyncio.get_event_loop()
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while client.is_connected:
                try:
                    async with session.get(url) as response1:
                        response1.raise_for_status()
                        total_length = int(response1.headers.get('content-length', 0))
                        if total_length == 0:
                            log.error(f"{device_sn} apk file size is 0")
                            return

                        # 支持断点续传（需服务器支持Range头）
                        headers = {}
                        if os.path.exists(file_path):
                            downloaded = os.path.getsize(file_path)
                            headers['Range'] = f'bytes={downloaded}-'

                        async with session.get(url, headers=headers) as response:
                            if response.status == 206:
                                log.info(f"{device_sn} resuming download from {downloaded} bytes")
                            elif response.status == 200:
                                log.info(f"{device_sn} starting download from the beginning")
                            elif response.status == 429:
                                raise aiohttp.ClientError(f"{device_sn} rate limited")

                            async with aiofiles.open(file_path, 'ab') as f:  # 追加模式写入
                                async for chunk in response.content.iter_chunked(1024):
                                    if not chunk:
                                        continue
                                    await f.write(chunk)
                                    downloaded += len(chunk)
                                    current_progress = int((downloaded * 100) // total_length)

                                    # 减少日志频率，避免阻塞
                                    if current_progress - last_reported >= 10:
                                        log.info(f"{device_sn} progress: {current_progress}%")
                                    last_reported = current_progress

                                    # 上报100%时候
                                    if current_progress == 100:
                                        # log.info(f"{device_sn} progress: {current_progress}%")
                                        await f.flush()
                                        await loop.run_in_executor(None, os.fsync, f.fileno())
                                        # status = "1" if current_progress < 100 else "2"
                                        test_md5 = self.get_md5(file_path)
                                        if test_md5 == MQTTConfig.hb_md5:
                                            status = "2"
                                            fail_des = f"Downloaded {current_progress}% , MD5 check success"
                                            log.info("%s MD5 check success" % device_sn)
                                        else:
                                            status = "3"
                                            fail_des = f"Downloaded {current_progress}% , MD5 check failed"
                                            log.error("%s MD5 check fail" % device_sn)
                                        # json_data = {
                                        #     "cmd": "apkUpgradeResp",
                                        #     "sn": device_sn,
                                        #     "taskId": task_id,
                                        #     "param": {
                                        #         "status": status,
                                        #         "applicationName": app_name,
                                        #         "apkVersion": app_version,
                                        #         "failDes": fail_des
                                        #     }}

                                        json_data = {
                                            "cmd": "customResourcePosterResp",
                                            "sn": device_sn,
                                            "taskId": task_id,
                                            "param": {"status": status, "failDes": fail_des}
                                        }

                                        await self.report_progress(client, json_data)
                                        return  # 成功退出

                except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                    log.error(f"{device_sn} error: {str(e)}, retry {retries + 1}")
                    retries += 1
                    await asyncio.sleep(2 ** retries)  # 指数退避
                except Exception as e:
                    log.error(f"{device_sn} unexpected error: {str(e)}")
                    break

    def get_md5(self, file_path):
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def setup_callbacks(self):
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message


def mkdir_and_rm_file(file_path, device_sn):
    act_file_path = os.path.join(file_path, device_sn)
    if not os.path.exists(act_file_path):
        os.makedirs(act_file_path)
    else:
        for i in os.listdir(act_file_path):
            path_file = os.path.join(act_file_path, i)
            if os.path.isfile(path_file):
                os.remove(path_file)


async def connect_batch(start_sn, batch_size, broker, port, password):
    clients = []
    for i in range(batch_size):
        device_sn = f"CS{start_sn + i}"
        mkdir_and_rm_file(MQTTConfig.apk_download_path, device_sn)
        mkdir_and_rm_file(MQTTConfig.ota_download_path, device_sn)
        client = DeviceClient(device_sn, broker, port, device_sn, password)
        client.setup_callbacks()
        clients.append(client)
        await client.connect()
    return clients


async def main():
    if platform.system() != "Windows":
        # "python3 async_mqtt.py "192.168.0.33:1883" -c 10 -s 100000 -b 10 &> ouput.txt"
        parser = argparse.ArgumentParser(description="Process some arguments.")
        parser.add_argument('address', type=str, help='The broker address')
        parser.add_argument('-c', '--count', type=int, help='Number of devices to connect')
        parser.add_argument('-s', '--start_sn', type=str, help='Starting serial number')
        parser.add_argument('-b', '--batch_size', type=int, help='Batch size')

        args = parser.parse_args()

        broker_address = args.address
        num_devices = args.count
        start_sn = args.start_sn
        batch_size = args.batch_size
        broker, port = broker_address.split(":")
    else:
        broker = MQTTConfig.broker
        port = MQTTConfig.port
        num_devices = 1
        start_sn = 100000
        batch_size = 1

    print("Broker address:", broker)
    print("Number of devices:", num_devices)
    print("Starting serial number:", start_sn)
    print("Batch size:", batch_size)

    all_clients = []
    for batch_start in range(0, num_devices, batch_size):
        clients = await connect_batch(int(start_sn) + batch_start, int(batch_size), broker, port, MQTTConfig.password)
        all_clients.extend(clients)
        await asyncio.sleep(1)  # Control the connection rate

    # Keep the connections alive
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await asyncio.gather(*(client.disconnect() for client in all_clients))


if __name__ == "__main__":
    asyncio.run(main())
