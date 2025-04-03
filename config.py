# config.py
import logging
import time


class MQTTConfig:
    # 连接配置
    client_id = "MT_01011"
    first_username = 100000
    password = "telpo.123"
    broker = "192.168.0.33"
    port = 1883

    # 配置组织消息
    org_sub_topic_default = "/e/55/ma/d/default"
    org_sub_topic_department = "/e/55/ma/d/58"
    other_sub_topic_group = "/e/55/ma/g/0"

    # 蚂蚁平台
    # org_sub_topic_default = "/e/361/ma/d/default"
    # org_sub_topic_department = "/e/361/ma/d/366"
    # other_sub_topic_group = "/e/361/ma/g/0"

    # 单独设备
    sub_topic = "/e/default/ma/d/"
    pub_topic = "/e/default/ma/d/plat"

    # 配置组织消息
    department_id = 58
    eid = 55

    # 性能参数
    keep_alive = 60
    reconnect_delay = (1, 120)
    qos = 1
    queue_timeout = 5
    heartbeat_internet = 60

    # 调试设置
    debug = True
    log_level = logging.INFO

    log_file_name = "log.log"

    # 下载路径设置
    # ota包下载路径
    ota_download_path = "Download/OTA/"
    apk_download_path = "Download/APK/"
    hb_download_path = "Download/HB/"

    CONNECT_TIMEOUT = 5
    MAX_CONCURRENT = 500

    ota_md5 = "2983985bc6b28efc3dd4460317ee2643"
    apk_md5 = "b5c279fa6738f04af866feaddff2add8"
    hb_md5 = "fad81cfec2945bfeca5258c22a3b15ce"


LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING,
    'error': logging.ERROR,
    'critical': logging.CRITICAL
}
level = 'info'
logger = logging.getLogger()


def create_file(filename):
    f = open(filename, "w", encoding='utf-8')
    f.close()


def set_handler(levels):
    logger.addHandler(MyLog.handler)


def remove_handler(levels):
    logger.removeHandler(MyLog.handler)


def get_current_time():
    return time.strftime(MyLog.date, time.localtime(time.time()))


class MyLog:
    log_file = MQTTConfig.log_file_name
    logger.setLevel(LEVELS.get(level, logging.NOTSET))

    create_file(log_file)
    date = '%Y-%m-%d %H:%M:%S'

    handler = logging.FileHandler(log_file, encoding='utf-8')

    @staticmethod
    def info(log_meg):
        set_handler('info')
        logger.info("[INFO " + get_current_time() + "]" + log_meg)
        print("[INFO " + get_current_time() + "]" + log_meg)
        remove_handler('info')

    @staticmethod
    def error(log_meg):
        set_handler('error')
        logger.error("[ERROR " + get_current_time() + "]" + log_meg)
        print("[ERROR " + get_current_time() + "]" + log_meg)
        remove_handler('error')

    @staticmethod
    def warning(log_meg):
        set_handler('warning')
        logger.error("[Warning " + get_current_time() + "]" + log_meg)
        print("[Warning " + get_current_time() + "]" + log_meg)
        remove_handler('Warning')


