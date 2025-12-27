#!/bin/bash

# 解压指定目录下的所有 tar.gz 文件并删除源文件
# 用法: ./extract_and_remove.sh <目标目录>

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查参数
if [ $# -eq 0 ]; then
    echo -e "${RED}错误: 请指定目标目录${NC}"
    echo "用法: $0 <目标目录>"
    exit 1
fi

TARGET_DIR="$1"

# 检查目录是否存在
if [ ! -d "$TARGET_DIR" ]; then
    echo -e "${RED}错误: 目录 '$TARGET_DIR' 不存在${NC}"
    exit 1
fi

# 转换为绝对路径
TARGET_DIR=$(cd "$TARGET_DIR" && pwd)

echo -e "${GREEN}开始处理目录: $TARGET_DIR${NC}"
echo "----------------------------------------"

# 计数器
success_count=0
fail_count=0

# 查找所有 tar.gz 文件
shopt -s nullglob
tar_files=("$TARGET_DIR"/*.tar.gz)

if [ ${#tar_files[@]} -eq 0 ]; then
    echo -e "${YELLOW}未找到任何 .tar.gz 文件${NC}"
    exit 0
fi

echo -e "找到 ${#tar_files[@]} 个 tar.gz 文件"
echo "----------------------------------------"

# 遍历并处理每个文件
for file in "${tar_files[@]}"; do
    filename=$(basename "$file")
    echo -e "${YELLOW}正在处理: $filename${NC}"
    
    # 解压文件到同一目录
    if tar -xzf "$file" -C "$TARGET_DIR"; then
        echo -e "  ${GREEN}✓ 解压成功${NC}"
        
        # 删除源文件
        if rm "$file"; then
            echo -e "  ${GREEN}✓ 已删除源文件${NC}"
            ((success_count++))
        else
            echo -e "  ${RED}✗ 删除源文件失败${NC}"
            ((fail_count++))
        fi
    else
        echo -e "  ${RED}✗ 解压失败${NC}"
        ((fail_count++))
    fi
    echo ""
done

echo "----------------------------------------"
echo -e "${GREEN}处理完成!${NC}"
echo -e "成功: ${GREEN}$success_count${NC} 个"
echo -e "失败: ${RED}$fail_count${NC} 个"

