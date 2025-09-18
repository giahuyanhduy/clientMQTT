#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MQTT Client cho Fuel Station Management System
Thay thế HTTP requests bằng MQTT communication
"""

import paho.mqtt.client as mqtt
import json
import time
import os
import subprocess
import re
import random
import logging
from datetime import datetime, timedelta
from threading import Thread
import requests

# ==================== CẤU HÌNH LOGGING CHI TIẾT ====================
def setup_logging():
    """Thiết lập hệ thống logging chi tiết"""
    # Tạo thư mục log trong thư mục hiện tại hoặc /opt/fuel-client-mqtt/logs
    if os.path.exists("/opt/fuel-client-mqtt"):
        log_dir = "/opt/fuel-client-mqtt/logs"
    else:
        log_dir = "./logs"
    
    os.makedirs(log_dir, exist_ok=True)
    
    # Cấu hình logging chính
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'{log_dir}/client_mqtt.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

# Khởi tạo logging
logger = setup_logging()

# ==================== CẤU HÌNH MQTT ====================
MQTT_BROKER_HOST = "103.77.166.69"  # Kết nối về server, không phải localhost
MQTT_BROKER_PORT = 1883
MQTT_KEEPALIVE = 60
MQTT_QOS = 1

# Topics MQTT
TOPICS = {
    'station_data': 'fuel_station/data',
    'station_status': 'fuel_station/status',
    'station_command': 'fuel_station/command',
    'station_response': 'fuel_station/response',
    'station_warning': 'fuel_station/warning',
    'station_heartbeat': 'fuel_station/heartbeat'
}

# ==================== MQTT CLIENT CLASS ====================
class MQTTFuelStationClient:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        self.connected = False
        self.port = None
        self.version = None
        self.mac = None
        self.mabom_history = {}
        self.connection_status = {}
        self.is_all_disconnect_restart = [False]
        self.last_restart_all = None
        self.last_non_sequential_restart = None
        self.should_stop = False  # Đánh dấu để dừng client
        self.should_reconnect = False  # Đánh dấu để kết nối lại
        
    def on_connect(self, client, userdata, flags, rc):
        """Callback khi kết nối MQTT"""
        if rc == 0:
            self.connected = True
            logger.info("✅ Kết nối MQTT thành công")
            
            # Subscribe các topics cần thiết
            self.client.subscribe(f"{TOPICS['station_command']}/{self.port}", qos=MQTT_QOS)
            self.client.subscribe(f"{TOPICS['station_command']}/all", qos=MQTT_QOS)
            logger.info(f"📡 Đã subscribe command topics cho port {self.port}")
            
        else:
            logger.error(f"❌ Lỗi kết nối MQTT: {rc}")
            
    def on_disconnect(self, client, userdata, rc):
        """Callback khi mất kết nối MQTT"""
        self.connected = False
        logger.warning(f"⚠️ Mất kết nối MQTT: {rc}")
        
        # Không dừng client, chỉ đánh dấu mất kết nối để reconnect
        if rc != 0:
            logger.warning("⚠️ Mất kết nối không mong muốn, sẽ thử kết nối lại...")
            self.should_reconnect = True
        
    def on_log(self, client, userdata, level, buf):
        """Callback cho log MQTT"""
        logger.debug(f"MQTT Log: {buf}")
        
    def on_message(self, client, userdata, msg):
        """Callback khi nhận message MQTT"""
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode('utf-8'))
            
            logger.info(f"📨 Nhận message từ {topic}")
            
            if topic.startswith(TOPICS['station_command']):
                self.handle_command(payload)
                
        except json.JSONDecodeError as e:
            logger.error(f"❌ Lỗi parse JSON từ MQTT: {e}")
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý message MQTT: {e}")
            
    def handle_command(self, command_data):
        """Xử lý lệnh từ server"""
        try:
            command = command_data.get('command')
            port = command_data.get('port')
            data = command_data.get('data', {})
            
            logger.info(f"📋 Nhận lệnh: {command} cho port {port}")
            
            if command == 'restart':
                self.handle_restart_command()
            elif command == 'ssh':
                self.handle_ssh_command(data.get('command', ''))
            elif command == 'getdata':
                self.handle_getdata_command(data.get('getdata', 'Off'))
            elif command == 'laymabom':
                self.handle_laymabom_command(data.get('pump_id', ''))
                
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý lệnh: {e}")
            
    def handle_restart_command(self):
        """Xử lý lệnh restart"""
        try:
            logger.info("🔄 Nhận lệnh restart, đang khởi động lại hệ thống...")
            subprocess.run(['reboot', 'now'], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Lỗi thực thi lệnh restart: {e}")
        except Exception as e:
            logger.error(f"❌ Lỗi không mong muốn khi restart: {e}")
            
    def handle_ssh_command(self, ssh_command):
        """Xử lý lệnh SSH"""
        try:
            logger.info(f"💻 Nhận lệnh SSH: {ssh_command}")
            subprocess.Popen(ssh_command, shell=True)
            logger.info(f"✅ Đã bắt đầu thực thi lệnh: {ssh_command}")
        except Exception as e:
            logger.error(f"❌ Lỗi thực thi lệnh SSH: {e}")
            
    def handle_getdata_command(self, getdata_status):
        """Xử lý lệnh getdata"""
        try:
            logger.info(f"📊 Nhận lệnh getdata: {getdata_status}")
            
            # Cập nhật trạng thái gửi dữ liệu
            if getdata_status.lower() == 'on':
                self.getdata_enabled = True
                logger.info("✅ Bật chế độ gửi dữ liệu")
            else:
                self.getdata_enabled = False
                logger.info("⏸️ Tắt chế độ gửi dữ liệu, chỉ gửi heartbeat")
                
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý lệnh getdata: {e}")
            
    def handle_laymabom_command(self, pump_id):
        """Xử lý lệnh laymabom"""
        try:
            logger.info(f"🔢 Nhận lệnh laymabom cho pump: {pump_id}")
            # Gọi API daylaidulieu
            self.call_daylaidulieu_api(pump_id)
        except Exception as e:
            logger.error(f"❌ Lỗi xử lý lệnh laymabom: {e}")
            
    def connect(self):
        """Kết nối đến MQTT broker"""
        try:
            logger.info(f"🔌 Đang kết nối đến MQTT broker {MQTT_BROKER_HOST}:{MQTT_BROKER_PORT}")
            self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEPALIVE)
            self.client.loop_start()
            
            # Chờ một chút để kết nối được thiết lập
            time.sleep(2)
            
            if self.connected:
                logger.info("✅ Kết nối MQTT thành công")
                self.should_reconnect = False
                return True
            else:
                logger.warning("⚠️ Kết nối chưa thành công")
                return False
                
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối MQTT: {e}")
            return False
            
    def disconnect(self):
        """Ngắt kết nối MQTT"""
        try:
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("🔌 Đã ngắt kết nối MQTT")
        except Exception as e:
            logger.error(f"❌ Lỗi ngắt kết nối MQTT: {e}")
            
    def publish_data(self, data):
        """Gửi dữ liệu trạm qua MQTT"""
        try:
            message = {
                'port': self.port,
                'version': self.version,
                'mac': self.mac,
                'timestamp': datetime.now().isoformat(),
                'data': data
            }
            
            self.client.publish(TOPICS['station_data'], json.dumps(message), qos=MQTT_QOS)
            logger.debug(f"📤 Đã gửi dữ liệu trạm qua MQTT")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi gửi dữ liệu MQTT: {e}")
            return False
            
    def publish_status(self, status):
        """Gửi trạng thái trạm qua MQTT"""
        try:
            message = {
                'port': self.port,
                'status': status,
                'timestamp': datetime.now().isoformat()
            }
            
            self.client.publish(TOPICS['station_status'], json.dumps(message), qos=MQTT_QOS)
            logger.debug(f"📤 Đã gửi trạng thái trạm qua MQTT")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi gửi trạng thái MQTT: {e}")
            return False
            
    def publish_warning(self, warning_type, pump_id, mabom=None):
        """Gửi cảnh báo qua MQTT"""
        try:
            message = {
                'port': self.port,
                'warning_type': warning_type,
                'pump_id': pump_id,
                'mabom': mabom,
                'timestamp': datetime.now().isoformat()
            }
            
            self.client.publish(TOPICS['station_warning'], json.dumps(message), qos=MQTT_QOS)
            logger.warning(f"⚠️ Đã gửi cảnh báo {warning_type} qua MQTT")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi gửi cảnh báo MQTT: {e}")
            return False
            
    def publish_heartbeat(self, include_info=False):
        """Gửi heartbeat qua MQTT"""
        try:
            message = {
                'port': self.port
            }
            
            # Chỉ gửi thông tin đầy đủ khi cần thiết (lần đầu hoặc khi được yêu cầu)
            if include_info:
                message.update({
                    'version': self.version,
                    'mac': self.mac
                })
            
            self.client.publish(TOPICS['station_heartbeat'], json.dumps(message), qos=MQTT_QOS)
            logger.debug(f"💓 Đã gửi heartbeat qua MQTT")
            return True
            
        except Exception as e:
            logger.error(f"❌ Lỗi gửi heartbeat MQTT: {e}")
            return False

# ==================== HELPER FUNCTIONS ====================
def get_cpu_arch():
    """Lấy kiến trúc CPU"""
    try:
        arch = subprocess.check_output(['uname', '-m']).decode().strip()
        if 'arm' in arch.lower() or 'aarch64' in arch.lower():
            return 'ARM'
        elif 'x86' in arch.lower() or 'i686' in arch.lower():
            return 'X86'
        else:
            return 'Unknown'
    except subprocess.CalledProcessError as e:
        logger.error(f"Lỗi khi lấy kiến trúc CPU: {e}")
        return 'Unknown'
    except Exception as e:
        logger.error(f"Lỗi không mong muốn khi lấy kiến trúc CPU: {e}")
        return 'Unknown'

def get_version_from_js():
    """Lấy version từ file JavaScript"""
    possible_paths = [
        '/home/Phase_3/GasController.js',
        '/home/giang/Phase_3/GasController.js'
    ]

    has_ips = False
    has_fuelmet = False
    try:
        with open('/opt/autorun', 'r') as file:
            content = file.read()
            if './ips' in content:
                has_ips = True
            if 'fuelmet' in content:
                has_fuelmet = True
    except Exception as e:
        logger.error(f"Lỗi khi đọc file /opt/autorun: {e}")

    cpu_arch = get_cpu_arch()
    for path in possible_paths:
        if os.path.exists(path):
            with open(path, 'r') as file:
                content = file.read()
                match = re.search(r'const\s+ver\s*=\s*"([^"]+)"', content)
                if match:
                    version = match.group(1)
                    version = f"{cpu_arch}-{version}"
                    if has_ips and has_fuelmet:
                        return version + "-IPS-Fuelmet"
                    elif has_ips:
                        return version + "-IPS"
                    elif has_fuelmet:
                        return version + "-Fuelmet"
                    return version
    
    version = f"{cpu_arch}-1.0"
    if has_ips and has_fuelmet:
        return version + "-IPS-Fuelmet"
    elif has_ips:
        return version + "-IPS"
    elif has_fuelmet:
        return version + "-Fuelmet"
    return version

def get_port_from_file():
    """Lấy port từ file autorun"""
    try:
        with open('/opt/autorun', 'r') as file:
            content = file.read()
            match = re.search(r'(\s\d{4}|\d{5}):localhost:22', content)
            if match:
                port = match.group(1).strip()
                return port
            else:
                logger.error("Không tìm thấy port trong file.")
                return None
    except Exception as e:
        logger.error(f"Lỗi khi đọc port từ file: {e}")
        return None

def get_mac():
    """Lấy địa chỉ MAC"""
    try:
        interface_cmd = "ip route get 1.1.1.1 | grep -oP 'dev \\K\\w+'"
        interface = subprocess.check_output(interface_cmd, shell=True).decode().strip()
        result = subprocess.check_output(f"ip link show {interface}", shell=True).decode()
        mac_match = re.search(r"ether ([\da-fA-F:]+)", result)
        if mac_match:
            return mac_match.group(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Lỗi khi chạy lệnh ip link: {e}")
    except Exception as e:
        logger.error(f"Lỗi lấy MAC Address: {e}")
    return "00:00:00:00:00:00"

def get_data_from_url(url):
    """Lấy dữ liệu từ URL (fallback cho HTTP)"""
    try:
        logger.debug(f"🔍 Đang gọi URL: {url}")
        response = requests.get(url, timeout=10)
        logger.debug(f"📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"✅ Lấy được dữ liệu: {type(data)} với {len(str(data))} ký tự")
            return data
        else:
            logger.error(f"❌ Mã trạng thái không phải 200: {response.status_code}")
            logger.error(f"❌ Response content: {response.text[:200]}...")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Lỗi khi lấy dữ liệu từ URL: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Lỗi không mong đợi khi xử lý dữ liệu: {e}")
        return None

def call_daylaidulieu_api(pump_id):
    """Gọi API daylaidulieu"""
    api_url = f"http://localhost:6969/daylaidulieu/{pump_id}"
    try:
        response = requests.get(api_url, timeout=10)
        logger.info(f"Đã gọi API daylaidulieu cho pump ID {pump_id}. Mã trạng thái: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi gọi API daylaidulieu: {e}")

def check_disk_and_clear_logs(threshold=85):
    """Kiểm tra dung lượng ổ cứng và xóa log nếu cần"""
    try:
        output = subprocess.check_output(['df', '-h', '/'], stderr=subprocess.STDOUT).decode()
        lines = output.splitlines()
        for line in lines:
            if line.strip().endswith(' /'):
                parts = line.split()
                if len(parts) >= 5:
                    usage_str = parts[4].replace('%', '')
                    disk_usage_percent = float(usage_str)
                    logger.info(f"Mức sử dụng ổ cứng hiện tại: {disk_usage_percent:.2f}%")
                    
                    if disk_usage_percent > threshold:
                        logger.warning(f"Ổ cứng sử dụng vượt quá {threshold}%. Tiến hành xóa các file log...")
                        try:
                            # Xóa file log (tính năng giải phóng dung lượng)
                            subprocess.run("find / -type f -name '*.log' -execdir rm -- '{}' +", shell=True, check=True)
                            logger.info("Đã xóa các file log trong toàn bộ hệ thống")
                        except subprocess.CalledProcessError as e:
                            logger.error(f"Lỗi khi xóa file log: {e}")
                    else:
                        logger.info(f"Ổ cứng sử dụng dưới ngưỡng {threshold}%, không cần xóa file log.")
                break
    except subprocess.CalledProcessError as e:
        logger.error(f"Lỗi khi chạy lệnh df: {e}")
    except Exception as e:
        logger.error(f"Lỗi khi kiểm tra ổ cứng: {e}")

# ==================== MAIN CLIENT CLASS ====================
class FuelStationClient:
    def __init__(self):
        self.mqtt_client = MQTTFuelStationClient()
        self.port = None
        self.version = None
        self.mac = None
        self.mabom_history = {}
        self.connection_status = {}
        self.is_all_disconnect_restart = [False]
        self.last_restart_all = None
        self.last_non_sequential_restart = None
        self.getdata_enabled = False  # Mặc định tắt gửi dữ liệu
        self.info_sent = False  # Đánh dấu đã gửi thông tin chưa
        self.should_stop = False  # Đánh dấu để dừng client
        self.should_reconnect = False  # Đánh dấu để kết nối lại
        
    def initialize(self):
        """Khởi tạo client"""
        try:
            # Lấy thông tin cơ bản
            self.port = get_port_from_file()
            if not self.port:
                logger.error("Không tìm thấy port. Thoát.")
                return False
                
            self.mac = get_mac()
            if not self.mac:
                logger.error("Không tìm thấy MAC. Thoát.")
                return False
                
            self.version = get_version_from_js()
            
            # Thiết lập MQTT client
            self.mqtt_client.port = self.port
            self.mqtt_client.version = self.version
            self.mqtt_client.mac = self.mac
            
            logger.info(f"Sử dụng port: {self.port}")
            logger.info(f"Sử dụng MAC: {self.mac}")
            logger.info(f"Sử dụng version: {self.version}")
            
            return True
            
        except Exception as e:
            logger.error(f"Lỗi khởi tạo client: {e}")
            return False
            
    def connect(self):
        """Kết nối MQTT"""
        success = self.mqtt_client.connect()
        if not success:
            logger.warning("⚠️ Không thể kết nối MQTT, sẽ thử lại sau")
            self.should_reconnect = True
        return success
        
    def disconnect(self):
        """Ngắt kết nối MQTT"""
        self.mqtt_client.disconnect()
        
    def send_data_continuously(self):
        """Gửi heartbeat liên tục và dữ liệu khi cần thiết"""
        while not self.should_stop:
            try:
                # Kiểm tra kết nối MQTT trước khi gửi
                if not self.mqtt_client.connected or self.should_reconnect:
                    logger.warning("⚠️ Mất kết nối MQTT, đang thử kết nối lại...")
                    
                    # Thử kết nối lại
                    if self.mqtt_client.connect():
                        logger.info("✅ Đã kết nối lại MQTT thành công")
                        self.should_reconnect = False
                        self.info_sent = False  # Reset để gửi thông tin đầy đủ lại
                    else:
                        logger.warning("⚠️ Chưa thể kết nối lại, chờ 5 giây...")
                        time.sleep(5)
                        continue
                
                # Gửi heartbeat với thông tin đầy đủ lần đầu, sau đó chỉ gửi heartbeat đơn giản
                if not self.info_sent:
                    self.mqtt_client.publish_heartbeat(include_info=True)
                    self.info_sent = True
                    logger.info("📋 Đã gửi thông tin đầy đủ lần đầu")
                else:
                    self.mqtt_client.publish_heartbeat(include_info=False)
                    logger.debug("💓 Đã gửi heartbeat đơn giản")
                
                # Chỉ gửi dữ liệu khi getdata_enabled = True
                if self.getdata_enabled:
                    logger.info("📊 Chế độ gửi dữ liệu đã bật, đang lấy dữ liệu...")
                    data_from_url = get_data_from_url("http://localhost:6969/GetfullupdateArr")
                    if data_from_url:
                        # Gửi dữ liệu qua MQTT
                        logger.info(f"📊 Dữ liệu lấy được: {len(str(data_from_url))} ký tự")
                        self.mqtt_client.publish_data(data_from_url)
                        logger.info("📊 Đã gửi dữ liệu tới MQTT broker")
                    else:
                        logger.warning("⚠️ Không lấy được dữ liệu từ URL, tạo dữ liệu mẫu...")
                        # Tạo dữ liệu mẫu để test
                        sample_data = {
                            "station_id": self.port,
                            "timestamp": datetime.now().isoformat(),
                            "status": "online",
                            "pumps": [
                                {"id": 1, "status": "active", "volume": 100.5},
                                {"id": 2, "status": "active", "volume": 200.3}
                            ]
                        }
                        logger.info("📊 Gửi dữ liệu mẫu để test...")
                        self.mqtt_client.publish_data(sample_data)
                        logger.info("📊 Đã gửi dữ liệu mẫu tới MQTT broker")
                else:
                    logger.debug("⏸️ Chế độ gửi dữ liệu tắt, chỉ gửi heartbeat")
                
            except Exception as e:
                logger.error(f"❌ Lỗi trong vòng lặp gửi dữ liệu: {e}")
                # Đánh dấu cần kết nối lại thay vì dừng client
                self.should_reconnect = True
                time.sleep(5)  # Chờ 5 giây trước khi thử lại
                continue
            
            # Sleep cố định cho heartbeat (nằm ngoài try-except)
            if not self.should_stop:
                time.sleep(10)  # Gửi heartbeat mỗi 10 giây
        
        logger.info("🛑 Client đã dừng")
            
    def check_mabom_continuously(self):
        """Kiểm tra mã bơm liên tục"""
        while not self.should_stop:
            try:
                # Chỉ kiểm tra mã bơm khi đã kết nối MQTT
                if self.mqtt_client.connected and not self.should_reconnect:
                    data_from_url = get_data_from_url("http://localhost:6969/GetfullupdateArr")
                    if data_from_url:
                        self.check_mabom(data_from_url)
                    else:
                        logger.warning("Không lấy được dữ liệu để kiểm tra mã bơm")
                else:
                    logger.debug("⏸️ Chưa kết nối MQTT, bỏ qua kiểm tra mã bơm")
                    
            except Exception as e:
                logger.error(f"Lỗi trong vòng lặp kiểm tra mã bơm: {e}")
                
            time.sleep(2)
            
    def check_mabom(self, data):
        """Kiểm tra mã bơm (giữ nguyên logic cũ)"""
        current_time = datetime.now()
        all_disconnected = True

        try:
            for item in data:
                idcot = item.get('id')
                pump = item.get('pump')
                statusnow = item.get('status')
                mabom_moinhat = item.get('MaBomMoiNhat', {}).get('pump')

                if idcot is None or pump is None:
                    continue

                pump_id = str(idcot)
                mabomtiep = pump
                is_disconnected = item.get('isDisconnected', False)

                if not is_disconnected:
                    all_disconnected = False

                if pump_id not in self.connection_status:
                    self.connection_status[pump_id] = {
                        'is_disconnected': is_disconnected,
                        'disconnect_time': current_time if is_disconnected else None,
                        'alert_sent': False,
                        'last_alerted_mabom': None,
                        'mismatch_count': 0,
                        'restart_done': False
                    }
                else:
                    if is_disconnected:
                        if not self.connection_status[pump_id]['is_disconnected']:
                            self.connection_status[pump_id]['is_disconnected'] = True
                            self.connection_status[pump_id]['disconnect_time'] = current_time
                            self.connection_status[pump_id]['alert_sent'] = False
                            self.connection_status[pump_id]['restart_done'] = False
                        else:
                            if current_time - self.connection_status[pump_id]['disconnect_time'] > timedelta(seconds=65):
                                if not self.connection_status[pump_id]['alert_sent']:
                                    logger.warning(f"Pump ID {pump_id} mất kết nối quá 65 giây.")
                                    self.mqtt_client.publish_warning("disconnection", pump_id, mabomtiep)
                                    self.connection_status[pump_id]['alert_sent'] = True
                    else:
                        if self.connection_status[pump_id]['is_disconnected']:
                            if current_time - self.connection_status[pump_id]['disconnect_time'] <= timedelta(seconds=65):
                                logger.info(f"Pump ID {pump_id} đã kết nối lại trong vòng 65 giây.")
                            self.connection_status[pump_id] = {
                                'is_disconnected': False,
                                'disconnect_time': None,
                                'alert_sent': False,
                                'last_alerted_mabom': self.connection_status[pump_id].get('last_alerted_mabom'),
                                'mismatch_count': self.connection_status[pump_id].get('mismatch_count', 0),
                                'restart_done': self.connection_status[pump_id].get('restart_done', False)
                            }

                if pump_id not in self.mabom_history:
                    self.mabom_history[pump_id] = []

                if self.mabom_history[pump_id] and isinstance(self.mabom_history[pump_id][-1], tuple) and self.mabom_history[pump_id][-1][0] == mabomtiep:
                    continue
                else:
                    self.mabom_history[pump_id].append((mabomtiep, current_time.strftime('%Y-%m-%d %H:%M:%S')))
                    if len(self.mabom_history[pump_id]) > 10:
                        self.mabom_history[pump_id].pop(0)

                mabom_entries = [entry for entry in self.mabom_history[pump_id] if isinstance(entry, tuple)]

                if statusnow == 'sẵn sàng':
                    if mabom_moinhat and mabom_moinhat != pump:
                        self.connection_status[pump_id]['mismatch_count'] += 1
                        logger.warning(f"Mã bơm không khớp lần {self.connection_status[pump_id]['mismatch_count']} cho pump ID {pump_id}: {mabom_moinhat} != {pump}")

                        if self.connection_status[pump_id]['mismatch_count'] == 3:
                            if self.last_non_sequential_restart is None or (current_time - self.last_non_sequential_restart) > timedelta(minutes=10):
                                logger.warning(f"Pump ID {pump_id} có mã bơm không khớp 3 lần. Thực hiện restartall.")
                                subprocess.run(['forever', 'restartall'])
                                self.last_non_sequential_restart = current_time
                                time.sleep(3)
                                call_daylaidulieu_api(pump_id)
                                self.mqtt_client.publish_warning("nonsequential", pump_id, mabomtiep)
                                self.connection_status[pump_id]['mismatch_count'] = 0
                            else:
                                logger.info("Phát hiện mã bơm không liên tiếp, nhưng đã restartall gần đây. Đợi 10 phút.")
                    else:
                        self.connection_status[pump_id]['mismatch_count'] = 0

                    if len(mabom_entries) > 1:
                        previous_mabom = mabom_entries[-2][0]
                        if isinstance(mabomtiep, int) and isinstance(previous_mabom, int):
                            if mabomtiep != previous_mabom + 1:
                                if self.connection_status[pump_id]['last_alerted_mabom'] != mabomtiep:
                                    logger.warning(f"Lỗi mã bơm không liên tiếp: Vòi bơm {pump_id} của port {self.port}.")
                                    self.mqtt_client.publish_warning("nonsequential", pump_id, mabomtiep)
                                    call_daylaidulieu_api(pump_id)
                                    self.mabom_history[pump_id].append({
                                        'type': 'nonsequential',
                                        'time': current_time.strftime('%Y-%m-%d %H:%M:%S')
                                    })
                                    self.connection_status[pump_id]['last_alerted_mabom'] = mabomtiep
                            else:
                                self.mabom_history[pump_id] = [entry for entry in self.mabom_history[pump_id] if not (isinstance(entry, dict) and entry.get('type') == 'nonsequential')]

            if all_disconnected and not any(conn['restart_done'] for conn in self.connection_status.values()) and not self.is_all_disconnect_restart[0]:
                if self.last_restart_all is None or (current_time - self.last_restart_all) > timedelta(minutes=10):
                    logger.warning("Tất cả các vòi đều mất kết nối. Thực hiện restartall.")
                    subprocess.run(['forever', 'restartall'])
                    self.last_restart_all = current_time
                    for conn in self.connection_status.values():
                        conn['restart_done'] = True
                    self.mqtt_client.publish_warning("all_disconnection", "all", "Tất cả các vòi đều mất kết nối.")
                    self.is_all_disconnect_restart[0] = True
                else:
                    logger.info("Tất cả các vòi mất kết nối, nhưng đã restartall gần đây. Đợi 10 phút.")
            
            if not all_disconnected:
                self.is_all_disconnect_restart[0] = False

        except Exception as e:
            logger.error(f"Lỗi trong check_mabom: {e}")

# ==================== MAIN FUNCTION ====================
def main():
    """Hàm chính khởi động client"""
    try:
        logger.info("🚀 Khởi động MQTT Fuel Station Client")
        
        # Kiểm tra dung lượng ổ cứng
        check_disk_and_clear_logs()
        
        # Khởi tạo client
        client = FuelStationClient()
        if not client.initialize():
            logger.error("❌ Không thể khởi tạo client")
            return
            
        # Kết nối MQTT
        if not client.connect():
            logger.error("❌ Không thể kết nối MQTT")
            return
            
        # Khởi động các thread
        data_thread = Thread(target=client.send_data_continuously, daemon=True)
        mabom_thread = Thread(target=client.check_mabom_continuously, daemon=True)
        
        data_thread.start()
        mabom_thread.start()
        
        logger.info("✅ Client đã khởi động thành công")
        
        # Giữ chương trình chạy
        try:
            while not client.should_stop:
                time.sleep(1)
                
                # Không dừng client khi mất kết nối, để nó tự reconnect
                # Chỉ log trạng thái kết nối
                if not client.mqtt_client.connected:
                    logger.debug("⚠️ Mất kết nối MQTT, client sẽ tự động kết nối lại...")
                    
        except KeyboardInterrupt:
            logger.info("⏹️ Nhận tín hiệu dừng, đang tắt client...")
            client.should_stop = True
            
        # Dừng các thread
        logger.info("🛑 Đang dừng các thread...")
        client.should_stop = True
        
        # Chờ các thread kết thúc
        if 'data_thread' in locals() and data_thread.is_alive():
            data_thread.join(timeout=5)
        if 'mabom_thread' in locals() and mabom_thread.is_alive():
            mabom_thread.join(timeout=5)
            
    except Exception as e:
        logger.error(f"❌ Lỗi khởi động client: {e}")
    finally:
        # Dọn dẹp
        if 'client' in locals():
            client.disconnect()
        logger.info("✅ Client đã được tắt")

if __name__ == "__main__":
    main()
