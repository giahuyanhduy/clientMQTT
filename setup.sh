#!/bin/bash

# ==========================================
# SCRIPT CÀI ĐẶT CLIENT MQTT FUEL STATION
# ==========================================
# Script này sẽ cài đặt client MQTT mới lên hệ thống
# Thay thế client cũ và cấu hình auto-update từ GitHub

set -e  # Dừng nếu có lỗi

# Màu sắc cho output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Cấu hình
CLIENT_DIR="$HOME/fuel-client-mqtt"
SERVICE_NAME="fuel-client-mqtt"
GITHUB_REPO="https://github.com/giahuyanhduy/clientMQTT.git"
TEMP_DIR="/tmp/fuel-client-setup"
SERVER_IP="103.77.166.69"

echo -e "${BLUE}==========================================${NC}"
echo -e "${BLUE}  CÀI ĐẶT CLIENT MQTT FUEL STATION${NC}"
echo -e "${BLUE}==========================================${NC}"

# Hàm log
log() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
    echo -e "${RED}[ERROR] $1${NC}"
    exit 1
}

warning() {
    echo -e "${YELLOW}[WARNING] $1${NC}"
}

# Kiểm tra quyền user (không cần root)
log "Chạy script với quyền user hiện tại: $(whoami)"

log "Bắt đầu cài đặt client MQTT..."

# 1. KIỂM TRA VÀ CÀI ĐẶT DEPENDENCIES
log "Kiểm tra các gói cần thiết..."

# Kiểm tra Python3
if ! command -v python3 &> /dev/null; then
    log "Cài đặt Python3..."
    if command -v apt &> /dev/null; then
        sudo apt update && sudo apt install -y python3
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y python3
    else
        error "Không thể cài đặt Python3. Vui lòng cài đặt thủ công."
    fi
else
    log "✅ Python3 đã được cài đặt: $(python3 --version)"
fi

# Kiểm tra pip3
if ! command -v pip3 &> /dev/null; then
    log "Cài đặt pip3..."
    if command -v apt &> /dev/null; then
        sudo apt install -y python3-pip
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3-pip
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y python3-pip
    else
        error "Không thể cài đặt pip3. Vui lòng cài đặt thủ công."
    fi
else
    log "✅ pip3 đã được cài đặt: $(pip3 --version)"
fi

# Kiểm tra git
if ! command -v git &> /dev/null; then
    log "Cài đặt git..."
    if command -v apt &> /dev/null; then
        sudo apt install -y git
    elif command -v yum &> /dev/null; then
        sudo yum install -y git
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y git
    else
        error "Không thể cài đặt git. Vui lòng cài đặt thủ công."
    fi
else
    log "✅ git đã được cài đặt: $(git --version)"
fi

# 2. DỪNG VÀ XÓA CLIENT CŨ (NẾU CÓ)
log "Kiểm tra và dừng client cũ..."

# Kiểm tra và dừng service cũ (chỉ khi có quyền)
if systemctl is-active --quiet client 2>/dev/null; then
    log "Dừng service client cũ..."
    sudo systemctl stop client
    sudo systemctl disable client
fi

if systemctl is-active --quiet fuel-client-mqtt 2>/dev/null; then
    log "Dừng service fuel-client-mqtt cũ..."
    sudo systemctl stop fuel-client-mqtt
    sudo systemctl disable fuel-client-mqtt
fi

# Xóa service cũ hoàn toàn (chỉ khi có quyền)
log "Xóa service cũ..."
if [ -f "/etc/systemd/system/client.service" ]; then
    log "Xóa client.service cũ..."
    sudo systemctl stop client 2>/dev/null || true
    sudo systemctl disable client 2>/dev/null || true
    sudo rm -f /etc/systemd/system/client.service
fi

if [ -f "/etc/systemd/system/fuel-client-mqtt.service" ]; then
    log "Xóa fuel-client-mqtt.service cũ..."
    sudo systemctl stop fuel-client-mqtt 2>/dev/null || true
    sudo systemctl disable fuel-client-mqtt 2>/dev/null || true
    sudo rm -f /etc/systemd/system/fuel-client-mqtt.service
fi

# Xóa thư mục client cũ nếu có
if [ -d "/home/client-repo" ]; then
    log "Xóa thư mục client cũ (/home/client-repo)..."
    rm -rf /home/client-repo
fi

if [ -d "/opt/fuel-client" ]; then
    log "Xóa thư mục client cũ (/opt/fuel-client)..."
    sudo rm -rf /opt/fuel-client
fi

if [ -d "$CLIENT_DIR" ]; then
    log "Xóa thư mục client cũ ($CLIENT_DIR)..."
    rm -rf "$CLIENT_DIR"
fi

sudo systemctl daemon-reload

# 4. TẠO THƯ MỤC VÀ CẤU TRÚC
log "Tạo cấu trúc thư mục..."
mkdir -p $CLIENT_DIR/{scripts,logs,config}
mkdir -p $TEMP_DIR

# 5. TẢI CODE TỪ GITHUB
log "Tải code từ GitHub..."
if [ -d "$TEMP_DIR" ]; then
    rm -rf $TEMP_DIR
fi

git clone $GITHUB_REPO $TEMP_DIR

# 6. COPY FILES
log "Copy files vào thư mục cài đặt..."
cp $TEMP_DIR/client_mqtt.py $CLIENT_DIR/
cp $TEMP_DIR/requirements.txt $CLIENT_DIR/ 2>/dev/null || echo "requirements.txt không tồn tại"

# 7. KIỂM TRA VÀ CÀI ĐẶT PYTHON DEPENDENCIES
log "Kiểm tra và cài đặt Python dependencies..."

# Danh sách thư viện cần thiết cho client_mqtt.py
REQUIRED_PACKAGES=(
    "paho-mqtt"
    "requests"
    "flask"
)

# Kiểm tra từng thư viện
for package in "${REQUIRED_PACKAGES[@]}"; do
    if python3 -c "import $package" 2>/dev/null; then
        log "✅ $package đã được cài đặt"
    else
        log "Cài đặt $package..."
        pip3 install --user $package
        if [ $? -eq 0 ]; then
            log "✅ Đã cài đặt $package thành công"
        else
            error "❌ Không thể cài đặt $package"
        fi
    fi
done

# Kiểm tra thư viện built-in (không cần cài đặt)
BUILTIN_MODULES=("json" "time" "os" "subprocess" "re" "random" "logging" "datetime" "threading")
for module in "${BUILTIN_MODULES[@]}"; do
    if python3 -c "import $module" 2>/dev/null; then
        log "✅ $module (built-in) - OK"
    else
        warning "⚠️ $module không khả dụng"
    fi
done

# 8. TẠO FILE CẤU HÌNH
log "Tạo file cấu hình..."
cat > $CLIENT_DIR/config/client.conf << EOF
# Cấu hình Client MQTT Fuel Station
# File này được tạo tự động bởi setup.sh

# Thông tin trạm
STATION_NAME=Trạm Xăng
STATION_PORT=12345
STATION_MAC=00:11:22:33:44:55

# MQTT Broker
MQTT_BROKER_HOST=$SERVER_IP
MQTT_BROKER_PORT=1883
MQTT_KEEPALIVE=60
MQTT_QOS=1

# Topics
TOPIC_DATA=fuel_station/data
TOPIC_STATUS=fuel_station/status
TOPIC_COMMAND=fuel_station/command
TOPIC_HEARTBEAT=fuel_station/heartbeat
TOPIC_WARNING=fuel_station/warning

# Cấu hình khác
HEARTBEAT_INTERVAL=30
LOG_LEVEL=INFO
LOG_FILE=$CLIENT_DIR/logs/client.log
EOF

# 9. TẠO SCRIPT AUTO-UPDATE (CHẠY KHI SERVICE KHỞI ĐỘNG)
log "Tạo script auto-update..."
cat > $CLIENT_DIR/scripts/auto_update.sh << 'EOF'
#!/bin/bash

# Script auto-update cho client MQTT
# Chạy mỗi lần service khởi động (reboot/restart)

CLIENT_DIR="/opt/fuel-client-mqtt"
GITHUB_REPO="https://github.com/giahuyanhduy/clientMQTT.git"
TEMP_DIR="/tmp/fuel-client-update"
LOG_FILE="$CLIENT_DIR/logs/update.log"

# Tạo log file nếu chưa có
mkdir -p "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log "=== BẮT ĐẦU KIỂM TRA CẬP NHẬT (Service khởi động) ==="

# Kiểm tra kết nối internet
if ! ping -c 1 github.com >/dev/null 2>&1; then
    log "Không có kết nối internet, bỏ qua cập nhật"
    exit 0
fi

# Tải code mới
if [ -d "$TEMP_DIR" ]; then
    rm -rf "$TEMP_DIR"
fi

log "Tải code từ GitHub..."
git clone "$GITHUB_REPO" "$TEMP_DIR" 2>/dev/null

if [ $? -eq 0 ]; then
    # So sánh version
    if [ -f "$CLIENT_DIR/client_mqtt.py" ] && [ -f "$TEMP_DIR/client_mqtt.py" ]; then
        if ! cmp -s "$CLIENT_DIR/client_mqtt.py" "$TEMP_DIR/client_mqtt.py"; then
            log "🔄 Phát hiện phiên bản mới, bắt đầu cập nhật..."
            
            # Backup file cũ
            BACKUP_FILE="$CLIENT_DIR/client_mqtt.py.backup.$(date +%Y%m%d_%H%M%S)"
            cp "$CLIENT_DIR/client_mqtt.py" "$BACKUP_FILE"
            log "📦 Backup file cũ: $BACKUP_FILE"
            
            # Copy file mới
            cp "$TEMP_DIR/client_mqtt.py" "$CLIENT_DIR/"
            log "✅ Copy file mới thành công"
            
            # Restart service
            log "🔄 Restart service..."
            systemctl restart fuel-client-mqtt
            
            if [ $? -eq 0 ]; then
                log "🎉 Cập nhật thành công! Service đã restart"
            else
                log "❌ Lỗi khi restart service, khôi phục file cũ..."
                cp "$BACKUP_FILE" "$CLIENT_DIR/client_mqtt.py"
                systemctl restart fuel-client-mqtt
                log "🔄 Đã khôi phục file cũ và restart service"
            fi
        else
            log "✅ Đã là phiên bản mới nhất, không cần cập nhật"
        fi
    else
        log "⚠️ Không tìm thấy file client_mqtt.py để so sánh"
    fi
    
    # Cleanup
    rm -rf "$TEMP_DIR"
    log "🧹 Đã dọn dẹp file tạm"
else
    log "❌ Lỗi khi tải code từ GitHub"
fi

log "=== KẾT THÚC KIỂM TRA CẬP NHẬT ==="
EOF

chmod +x $CLIENT_DIR/scripts/auto_update.sh

# 10. TẠO SYSTEMD SERVICE (VỚI AUTO-UPDATE)
log "Tạo systemd service..."
cat > /tmp/fuel-client-mqtt.service << EOF
[Unit]
Description=Fuel Station MQTT Client
After=network.target

[Service]
Type=simple
User=$(whoami)
WorkingDirectory=$CLIENT_DIR
ExecStartPre=$CLIENT_DIR/scripts/auto_update.sh
ExecStart=/usr/bin/python3 $CLIENT_DIR/client_mqtt.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Copy service file với quyền sudo
sudo cp /tmp/fuel-client-mqtt.service /etc/systemd/system/fuel-client-mqtt.service
sudo chmod 644 /etc/systemd/system/fuel-client-mqtt.service
rm /tmp/fuel-client-mqtt.service

# 11. TẠO SCRIPT QUẢN LÝ
log "Tạo script quản lý..."

# Script start
cat > $CLIENT_DIR/start.sh << 'EOF'
#!/bin/bash
echo "Khởi động Fuel Station MQTT Client..."
systemctl start fuel-client-mqtt
systemctl status fuel-client-mqtt
EOF

# Script stop
cat > $CLIENT_DIR/stop.sh << 'EOF'
#!/bin/bash
echo "Dừng Fuel Station MQTT Client..."
systemctl stop fuel-client-mqtt
EOF

# Script restart
cat > $CLIENT_DIR/restart.sh << 'EOF'
#!/bin/bash
echo "Khởi động lại Fuel Station MQTT Client..."
systemctl restart fuel-client-mqtt
systemctl status fuel-client-mqtt
EOF

# Script status
cat > $CLIENT_DIR/status.sh << 'EOF'
#!/bin/bash
echo "Trạng thái Fuel Station MQTT Client:"
systemctl status fuel-client-mqtt
echo ""
echo "Logs gần đây:"
journalctl -u fuel-client-mqtt -n 20 --no-pager
EOF

# Script update manual
cat > $CLIENT_DIR/update.sh << 'EOF'
#!/bin/bash
echo "Cập nhật thủ công Fuel Station MQTT Client..."
echo "Chạy auto-update script..."
/opt/fuel-client-mqtt/scripts/auto_update.sh
echo ""
echo "Restart service để áp dụng cập nhật..."
systemctl restart fuel-client-mqtt
echo "✅ Hoàn tất cập nhật thủ công!"
EOF

# Script xem logs
cat > $CLIENT_DIR/logs.sh << 'EOF'
#!/bin/bash
echo "Logs Fuel Station MQTT Client:"
echo "================================"
tail -f /opt/fuel-client-mqtt/logs/client.log
EOF

# Cấp quyền thực thi
chmod +x $CLIENT_DIR/*.sh

# 12. XÓA CRON JOB CŨ (KHÔNG CẦN CRON NỮA)
log "Xóa cron job cũ (không cần cron nữa)..."
# Xóa cron job cũ nếu có
crontab -l 2>/dev/null | grep -v "fuel-client-mqtt" | crontab - 2>/dev/null || true
crontab -l 2>/dev/null | grep -v "fuel-client" | crontab - 2>/dev/null || true
crontab -l 2>/dev/null | grep -v "client-repo" | crontab - 2>/dev/null || true
crontab -l 2>/dev/null | grep -v "startup.sh" | crontab - 2>/dev/null || true

log "✅ Auto-update sẽ chạy mỗi lần service khởi động (reboot/restart)"

# 13. RELOAD SYSTEMD VÀ KHỞI ĐỘNG
log "Reload systemd và khởi động service..."
sudo systemctl daemon-reload
sudo systemctl enable fuel-client-mqtt
sudo systemctl start fuel-client-mqtt

# 14. KIỂM TRA TRẠNG THÁI
sleep 5
if systemctl is-active --quiet fuel-client-mqtt; then
    log "✅ Client MQTT đã khởi động thành công!"
    log "📁 Thư mục cài đặt: $CLIENT_DIR"
    log "📋 Service: fuel-client-mqtt"
    log "🔄 Auto-update: Mỗi lần service khởi động (reboot/restart)"
    log "📊 Xem logs: $CLIENT_DIR/logs.sh"
    log "🔄 Update thủ công: $CLIENT_DIR/update.sh"
    log "📋 Xem update logs: tail -f $CLIENT_DIR/logs/update.log"
else
    error "❌ Không thể khởi động client MQTT"
fi

# 15. CLEANUP
log "Dọn dẹp file tạm..."
rm -rf $TEMP_DIR

echo -e "${GREEN}==========================================${NC}"
echo -e "${GREEN}  CÀI ĐẶT HOÀN TẤT!${NC}"
echo -e "${GREEN}==========================================${NC}"
echo -e "${BLUE}Các lệnh hữu ích:${NC}"
echo -e "  ${YELLOW}systemctl status fuel-client-mqtt${NC}  - Xem trạng thái"
echo -e "  ${YELLOW}$CLIENT_DIR/logs.sh${NC}              - Xem logs"
echo -e "  ${YELLOW}$CLIENT_DIR/update.sh${NC}            - Cập nhật thủ công"
echo -e "  ${YELLOW}$CLIENT_DIR/restart.sh${NC}           - Khởi động lại"
echo -e "  ${YELLOW}tail -f $CLIENT_DIR/logs/update.log${NC} - Xem update logs"
echo -e "  ${YELLOW}sudo systemctl restart fuel-client-mqtt${NC} - Restart để update"
echo -e ""
echo -e "${BLUE}Lưu ý:${NC}"
echo -e "  - Script chạy với quyền user: $(whoami)"
echo -e "  - Thư mục cài đặt: $CLIENT_DIR"
echo -e "  - Service chạy với user: $(whoami)"
echo -e "  - Chỉ cần sudo cho systemctl commands"
