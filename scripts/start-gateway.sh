#!/bin/bash

# Soulbase Gateway 启动脚本
# 自动加载 .env 文件并启动网关

set -e

# 进入项目根目录
cd "$(dirname "$0")/.."

echo "======================================"
echo "  Soulbase Gateway 启动脚本"
echo "======================================"
echo ""

# 检查 .env 文件
if [ ! -f .env ]; then
    echo "⚠️  未找到 .env 文件"
    echo ""
    echo "请执行以下命令创建配置文件："
    echo "  cp .env.example .env"
    echo ""
    echo "然后根据需要修改 .env 中的配置"
    exit 1
fi

echo "✓ 发现 .env 文件，正在加载环境变量..."

# 加载 .env 文件（过滤注释和空行）
export $(cat .env | grep -v '^#' | grep -v '^$' | xargs)

echo "✓ 环境变量加载完成"
echo ""

# 显示配置信息
echo "当前配置："
echo "  配置文件: ${GATEWAY_CONFIG_FILE:-未设置}"
echo "  Token: ${GATEWAY_TOKEN_LOCAL:-未设置}"
echo "  HMAC: ${GATEWAY_HMAC_LOCAL:0:10}... (已隐藏)"
echo ""

# 检查必需的环境变量
if [ -z "$GATEWAY_CONFIG_FILE" ]; then
    echo "❌ 错误: GATEWAY_CONFIG_FILE 未设置"
    exit 1
fi

if [ -z "$GATEWAY_TOKEN_LOCAL" ]; then
    echo "❌ 错误: GATEWAY_TOKEN_LOCAL 未设置"
    exit 1
fi

if [ -z "$GATEWAY_HMAC_LOCAL" ]; then
    echo "❌ 错误: GATEWAY_HMAC_LOCAL 未设置"
    exit 1
fi

# 检查配置文件是否存在
if [ ! -f "$GATEWAY_CONFIG_FILE" ]; then
    echo "❌ 错误: 配置文件不存在: $GATEWAY_CONFIG_FILE"
    exit 1
fi

echo "✓ 所有必需的环境变量已设置"
echo ""

# 启动网关
echo "======================================"
echo "  正在启动 Soulbase Gateway..."
echo "======================================"
echo ""

cargo run -p soulbase-gateway
